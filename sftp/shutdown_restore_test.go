package sftp_test

import (
	"context"
	"os"
	"path/filepath"
	"strings"
	"testing"
	"time"

	"github.com/superfly/ltx"

	"github.com/benbjohnson/litestream"
	"github.com/benbjohnson/litestream/internal/testingutil"
)

// TestStore_Close_SFTPReplicaRestores asserts the end of the chain over the
// Transport this change is about: a transaction committed immediately before
// Shutdown is present in a restore taken from the SFTP replica afterwards,
// Read back by a new client.
//
// The equivalent test over the file backend proves the ordering inside
// Close. It cannot prove anything about SFTP, which is where the connection
// Ownership, the abort and the error attribution all live — and a fast stop is
// Worth nothing if the last commit did not arrive.
//
// The restore uses a fresh client deliberately: after a terminal Abort the
// Original client refuses to reconnect, so reusing it would test the abort
// Rather than the backup.
func TestStore_Close_SFTPReplicaRestores(t *testing.T) {
	srv := startTestServer(t, serveFully)

	dir := t.TempDir()
	db := testingutil.NewDB(t, filepath.Join(dir, "db"))
	db.Replica = litestream.NewReplica(db)
	db.Replica.Client = newClientAt(t, srv, "replica")
	db.Replica.MonitorEnabled = false

	if err := db.Open(); err != nil {
		t.Fatal(err)
	}
	sqldb := testingutil.MustOpenSQLDB(t, db.Path())
	defer testingutil.MustCloseSQLDB(t, sqldb)

	s := litestream.NewStore([]*litestream.DB{db}, litestream.CompactionLevels{{Level: 0}})
	s.CompactionMonitorEnabled = false
	if err := s.Open(t.Context()); err != nil {
		t.Fatal(err)
	}

	if _, err := sqldb.ExecContext(t.Context(), `CREATE TABLE marker (id TEXT)`); err != nil {
		t.Fatal(err)
	}
	if err := db.Sync(t.Context()); err != nil {
		t.Fatal(err)
	}
	if err := db.Replica.Sync(t.Context()); err != nil {
		t.Fatal(err)
	}

	const marker = "committed-immediately-before-shutdown"
	if _, err := sqldb.ExecContext(t.Context(), `INSERT INTO marker (id) VALUES (?)`, marker); err != nil {
		t.Fatal(err)
	}

	if err := s.Close(context.Background()); err != nil {
		t.Fatalf("close: %v", err)
	}

	// A fresh client and a fresh replica, reading what the shutdown left.
	readback := litestream.NewDB(filepath.Join(t.TempDir(), "readback"))
	readback.Replica = litestream.NewReplica(readback)
	readback.Replica.Client = newClientAt(t, srv, "replica")
	readback.Replica.MonitorEnabled = false

	outputPath := filepath.Join(t.TempDir(), "restored.db")
	if err := readback.Replica.Restore(context.Background(), litestream.RestoreOptions{
		OutputPath: outputPath,
	}); err != nil {
		t.Fatalf("restore from the SFTP replica: %v", err)
	}

	restored := testingutil.MustOpenSQLDB(t, outputPath)
	defer testingutil.MustCloseSQLDB(t, restored)

	var got string
	if err := restored.QueryRowContext(context.Background(), `SELECT id FROM marker`).Scan(&got); err != nil {
		t.Fatalf("the marker row is not in the restored database: %v", err)
	}
	if got != marker {
		t.Fatalf("restored %q, want %q", got, marker)
	}

	var integrity string
	if err := restored.QueryRowContext(context.Background(), `PRAGMA integrity_check`).Scan(&integrity); err != nil {
		t.Fatal(err)
	}
	if integrity != "ok" {
		t.Fatalf("integrity_check: %s", integrity)
	}
}

// TestStore_Close_InterruptedUpload_EarlierBackupRestores asserts the promise
// That makes a fast shutdown acceptable at all: interrupting an upload costs
// The newest commit, and nothing else.
//
// A Fast stop is Only safe if The previous backup survives it. Uploads are
// Written to a temporary name and renamed, so an interrupted one must leave no
// Object under its final name and must not damage what was already there. This
// Checks both, and restores through a third client because the interrupted one
// Is terminal.
//
// Two servers, one directory: the first serves normally and the second stalls
// At the first WRITE. Pkg/sftp resolves absolute paths on the real filesystem,
// So both act on the same replica path without a new fixture mode.
func TestStore_Close_InterruptedUpload_EarlierBackupRestores(t *testing.T) {
	good := startTestServer(t, serveFully)
	stalling := startTestServer(t, stallAtWrite)
	replicaPath := filepath.Join(good.root, "replica")

	dir := t.TempDir()
	db := testingutil.NewDB(t, filepath.Join(dir, "db"))
	db.MonitorInterval = 0
	db.Replica = litestream.NewReplica(db)
	db.Replica.MonitorEnabled = false

	healthy := newClient(t, good)
	healthy.Path = replicaPath
	db.Replica.Client = healthy

	if err := db.Open(); err != nil {
		t.Fatal(err)
	}
	sqldb := testingutil.MustOpenSQLDB(t, db.Path())
	defer testingutil.MustCloseSQLDB(t, sqldb)

	if _, err := sqldb.ExecContext(t.Context(), `CREATE TABLE marker (id TEXT)`); err != nil {
		t.Fatal(err)
	}
	const kept = "committed-and-backed-up"
	if _, err := sqldb.ExecContext(t.Context(), `INSERT INTO marker (id) VALUES (?)`, kept); err != nil {
		t.Fatal(err)
	}
	if err := db.Sync(t.Context()); err != nil {
		t.Fatal(err)
	}
	if err := db.Replica.Sync(t.Context()); err != nil {
		t.Fatal(err)
	}

	level0 := filepath.Join(replicaPath, "ltx", "0")
	before := committed(t, level0)
	if len(before) == 0 {
		t.Fatal("the baseline backup wrote nothing")
	}

	// From here every upload stalls: the transport that carries the next
	// Commit is the one that goes quiet.
	doomed := newClient(t, stalling)
	doomed.Path = replicaPath
	db.Replica.Client = doomed
	t.Cleanup(doomed.Abort)

	if _, err := sqldb.ExecContext(t.Context(), `INSERT INTO marker (id) VALUES (?)`, "lost-to-the-stall"); err != nil {
		t.Fatal(err)
	}
	if err := db.Sync(t.Context()); err != nil {
		t.Fatal(err)
	}

	// The store owns these fields. Store.Open copies its own
	// ShutdownSyncTimeout and ShutdownSyncInterval onto every database it
	// Attaches (store.go), so a value set on the DB beforehand is silently
	// Replaced. Setting it there left this test on the zero-timeout path with
	// A background parent: no deadline, nothing to cancel, and a backstop
	// Correctly waiting forever for an end that could never come. The hang was
	// The test's configuration, not the fix.
	s := litestream.NewStore([]*litestream.DB{db}, litestream.CompactionLevels{{Level: 0}})
	s.CompactionMonitorEnabled = false
	// And the setter, not the field: NewStore copies its defaults onto every
	// Database it is constructed with, so assigning the field afterwards
	// Changes the store and not the databases. SetShutdownSyncTimeout exists
	// For exactly this and propagates.
	s.SetShutdownSyncTimeout(2 * time.Second)
	if err := s.Open(t.Context()); err != nil {
		t.Fatal(err)
	}

	closed := make(chan error, 1)
	go func() { closed <- s.Close(context.Background()) }()

	// Prove the stall is where the test says it is. Without this, a Close that
	// Returned for any other reason — a handshake failure, a misconfigured
	// Path — would satisfy everything below and the test would be describing a
	// Scenario that never happened.
	select {
	case <-stalling.writeReached:
	case err := <-closed:
		t.Fatalf("Close returned before the server saw a WRITE: %v", err)
	case <-time.After(30 * time.Second):
		t.Fatal("no WRITE reached the stalling server; nothing was interrupted")
	}

	select {
	case err := <-closed:
		// An interrupted final sync is a failed shutdown and must say so. A nil
		// Here would mean Close reported success for work that did not reach
		// The replica, which is the one thing a backup tool must never do.
		if err == nil {
			t.Fatal("Close reported success for a final sync that was interrupted")
		}
	case <-time.After(30 * time.Second):
		t.Fatal("Close did not return with a stalled upload in flight")
	}

	// The committed set must be unchanged: nothing removed, and nothing new
	// Published under a final name by the upload that was cut off.
	after := committed(t, level0)
	for name := range before {
		if _, ok := after[name]; !ok {
			t.Fatalf("an interrupted upload removed an earlier backup file: %s", name)
		}
	}
	for name := range after {
		if _, ok := before[name]; !ok {
			t.Fatalf("an interrupted upload published a final-name object: %s", name)
		}
	}

	// Restore through a fresh client: the interrupted one is terminal.
	readback := litestream.NewDB(filepath.Join(t.TempDir(), "readback"))
	readback.Replica = litestream.NewReplica(readback)
	fresh := newClient(t, good)
	fresh.Path = replicaPath
	readback.Replica.Client = fresh
	readback.Replica.MonitorEnabled = false

	outputPath := filepath.Join(t.TempDir(), "restored.db")
	if err := readback.Replica.Restore(context.Background(), litestream.RestoreOptions{
		OutputPath: outputPath,
	}); err != nil {
		t.Fatalf("the earlier backup does not restore after an interrupted upload: %v", err)
	}

	restored := testingutil.MustOpenSQLDB(t, outputPath)
	defer testingutil.MustCloseSQLDB(t, restored)

	var got string
	if err := restored.QueryRowContext(context.Background(), `SELECT id FROM marker WHERE id = ?`, kept).Scan(&got); err != nil {
		t.Fatalf("the committed-and-backed-up row is missing from the restore: %v", err)
	}
	var integrity string
	if err := restored.QueryRowContext(context.Background(), `PRAGMA integrity_check`).Scan(&integrity); err != nil {
		t.Fatal(err)
	}
	if integrity != "ok" {
		t.Fatalf("integrity_check after an interrupted upload: %s", integrity)
	}

	// And replication actually resumes. Reconnecting and listing files proves
	// A connection, not replication — the earlier version of this block called
	// Init and LTXFiles and claimed resumption on that basis. Reopen the same
	// Database with a fresh client, commit something new, sync it, and prove
	// The new commit restores. That is what "resumes" has to mean for a backup
	// Tool.
	resumed := litestream.NewDB(filepath.Join(dir, "db"))
	resumed.MonitorInterval = 0
	resumed.Replica = litestream.NewReplica(resumed)
	resumedClient := newClient(t, good)
	resumedClient.Path = replicaPath
	resumed.Replica.Client = resumedClient
	resumed.Replica.MonitorEnabled = false
	if err := resumed.Open(); err != nil {
		t.Fatalf("the database does not reopen after an interrupted shutdown: %v", err)
	}
	defer func() { _ = resumed.Close(context.Background()) }()

	const afterInterruption = "written-after-the-interruption"
	if _, err := sqldb.ExecContext(t.Context(), `INSERT INTO marker (id) VALUES (?)`, afterInterruption); err != nil {
		t.Fatal(err)
	}
	if err := resumed.Sync(t.Context()); err != nil {
		t.Fatalf("replication did not resume: %v", err)
	}
	if err := resumed.Replica.Sync(t.Context()); err != nil {
		t.Fatalf("the resumed replica did not accept the new commit: %v", err)
	}

	secondPath := filepath.Join(t.TempDir(), "restored-2.db")
	readback2 := litestream.NewDB(filepath.Join(t.TempDir(), "readback2"))
	readback2.Replica = litestream.NewReplica(readback2)
	fresh2 := newClient(t, good)
	fresh2.Path = replicaPath
	readback2.Replica.Client = fresh2
	readback2.Replica.MonitorEnabled = false
	if err := readback2.Replica.Restore(context.Background(), litestream.RestoreOptions{
		OutputPath: secondPath,
	}); err != nil {
		t.Fatalf("the resumed replica does not restore: %v", err)
	}

	restored2 := testingutil.MustOpenSQLDB(t, secondPath)
	defer testingutil.MustCloseSQLDB(t, restored2)
	var again string
	if err := restored2.QueryRowContext(context.Background(), `SELECT id FROM marker WHERE id = ?`, afterInterruption).Scan(&again); err != nil {
		t.Fatalf("a commit made after the interruption is not in the replica: %v", err)
	}
	if err := restored2.QueryRowContext(context.Background(), `SELECT id FROM marker WHERE id = ?`, kept).Scan(&again); err != nil {
		t.Fatalf("the pre-interruption commit was lost when replication resumed: %v", err)
	}
}

// Committed lists the final-name LTX files on the replica, ignoring temporaries.
// An upload is temp-name-and-rename, so a file under a final name is a file the
// Replica has committed to; the set before and after an interruption is what
// "no partial object was published" actually means.
func committed(tb testing.TB, dir string) map[string]struct{} {
	tb.Helper()
	entries, err := os.ReadDir(dir)
	if err != nil {
		tb.Fatalf("replica level directory unreadable: %v", err)
	}
	out := map[string]struct{}{}
	for _, e := range entries {
		if strings.HasSuffix(e.Name(), ".tmp") {
			continue
		}
		if _, _, err := ltx.ParseFilename(e.Name()); err != nil {
			tb.Fatalf("replica holds a file under a name nothing can read: %s", e.Name())
		}
		out[e.Name()] = struct{}{}
	}
	return out
}
