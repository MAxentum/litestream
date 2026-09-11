package litestream_test

import (
	"context"
	"io"
	"log/slog"
	"path/filepath"
	"strings"
	"sync"
	"testing"
	"time"

	"github.com/superfly/ltx"

	"github.com/benbjohnson/litestream"
	"github.com/benbjohnson/litestream/file"
	"github.com/benbjohnson/litestream/internal/testingutil"
)

// blockingAt wraps a real replica client and blocks uploads of one level,
// the way an SFTP write blocks when the far end stops answering: parked inside
// the transport, where the caller's context cannot reach it. Everything else —
// level 0 uploads, listings, reads — works normally, so the database can sync
// and compaction has something to compact.
type blockingAt struct {
	litestream.ReplicaClient // the real client; everything not overridden here

	level   int
	blocked chan struct{} // closed once an upload at that level is parked
	once    sync.Once

	mu       sync.Mutex
	aborted  chan struct{}
	disarmed bool // when set, writes pass through instead of parking
}

func newBlockingAt(client litestream.ReplicaClient, level int) *blockingAt {
	return &blockingAt{
		ReplicaClient: client,
		level:         level,
		blocked:       make(chan struct{}),
		aborted:       make(chan struct{}),
	}
}

// disarm lets writes through until arm is called, so a test can establish a
// baseline on the replica and block only the sync it is actually about.
func (c *blockingAt) disarm() {
	c.mu.Lock()
	c.disarmed = true
	c.mu.Unlock()
}

func (c *blockingAt) arm() {
	c.mu.Lock()
	c.disarmed = false
	c.mu.Unlock()
}

func (c *blockingAt) isDisarmed() bool {
	c.mu.Lock()
	defer c.mu.Unlock()
	return c.disarmed
}

func (c *blockingAt) WriteLTXFile(ctx context.Context, level int, minTXID, maxTXID ltx.TXID, r io.Reader) (*ltx.FileInfo, error) {
	if level != c.level || c.isDisarmed() {
		return c.ReplicaClient.WriteLTXFile(ctx, level, minTXID, maxTXID, r)
	}

	// Consume the input as a real client does, so the producer is not left
	// blocked writing into a pipe for a reason unrelated to the transport.
	if _, err := io.Copy(io.Discard, r); err != nil {
		return nil, err
	}

	c.once.Do(func() { close(c.blocked) })

	// Deliberately ignores ctx: a context-aware stub would pass without the fix.
	<-c.aborted
	return nil, context.Canceled
}

func (c *blockingAt) Abort() {
	c.mu.Lock()
	defer c.mu.Unlock()
	select {
	case <-c.aborted:
	default:
		close(c.aborted)
	}
}

// aborted reports whether Abort has been called, so a test can assert that
// Close returned BECAUSE the transport was released rather than by luck.
func (c *blockingAt) wasAborted() bool {
	select {
	case <-c.aborted:
		return true
	default:
		return false
	}
}

// slowClient succeeds, but takes its time — a healthy replica on a slow link.
type slowClient struct {
	litestream.ReplicaClient
	delay time.Duration

	mu              sync.Mutex
	writes          int
	inFlight        int
	abortedMidWrite bool //  the property under test
	aborted         bool
}

func (c *slowClient) Type() string                        { return "slow" }
func (c *slowClient) Init(ctx context.Context) error      { return nil }
func (c *slowClient) SetLogger(logger *slog.Logger)       {}
func (c *slowClient) DeleteAll(ctx context.Context) error { return nil }
func (c *slowClient) DeleteLTXFiles(ctx context.Context, a []*ltx.FileInfo) error {
	return nil
}

func (c *slowClient) LTXFiles(ctx context.Context, level int, seek ltx.TXID, useMetadata bool) (ltx.FileIterator, error) {
	return ltx.NewFileInfoSliceIterator(nil), nil
}

func (c *slowClient) OpenLTXFile(ctx context.Context, level int, minTXID, maxTXID ltx.TXID, offset, size int64) (io.ReadCloser, error) {
	return nil, ltx.ErrNoChecksum
}

func (c *slowClient) WriteLTXFile(ctx context.Context, level int, minTXID, maxTXID ltx.TXID, r io.Reader) (*ltx.FileInfo, error) {
	// A successful upload has consumed its input; report a read failure rather
	// than claiming success for bytes that never arrived.
	if _, err := io.Copy(io.Discard, r); err != nil {
		return nil, err
	}

	c.mu.Lock()
	c.inFlight++
	c.mu.Unlock()

	time.Sleep(c.delay)

	c.mu.Lock()
	defer c.mu.Unlock()
	c.inFlight--
	if c.aborted {
		return nil, context.Canceled
	}
	c.writes++
	return &ltx.FileInfo{Level: level, MinTXID: minTXID, MaxTXID: maxTXID}, nil
}

func (c *slowClient) Abort() {
	c.mu.Lock()
	defer c.mu.Unlock()
	c.aborted = true
	// Aborting a client with nothing in flight is the normal end of shutdown,
	// and is not what this test is about. Aborting one Mid-Sync is the fault.
	if c.inFlight > 0 {
		c.abortedMidWrite = true
	}
}

func (c *slowClient) counts() (writes int, abortedMidWrite bool) {
	c.mu.Lock()
	defer c.mu.Unlock()
	return c.writes, c.abortedMidWrite
}

// TestStore_Close_BudgetIsPerDatabase asserts that one database's slow shutdown
// does not spend another's budget.
//
// Databases are closed Sequentially. A single watchdog over the whole
// sequence would start the clock on the first database and abort every client
// when it expired — so the second database loses most of its budget and the
// third may never start at all. Each database is watched against the budget it
// was given.
func TestStore_Close_BudgetIsPerDatabase(t *testing.T) {
	// Ten databases, each given a 1.2 s final-sync budget and each taking
	// 350 ms — every one of them well inside its own budget.
	//
	// The numbers are set by the design this test rejects. That design arms one
	// clock over the whole sequence, at the longest budget plus a fixed
	// two-second grace, so the sequence has to outlast 1.2 s + 2 s or the
	// watchdog never fires and the test passes under the very thing it exists
	// to catch. Ten times 350 ms is 3.5 s, which clears it. Shortening these
	// timings without preserving that margin removes the test's power to
	// discriminate.
	const dbCount = 10

	dbs := make([]*litestream.DB, 0, dbCount)
	clients := make([]*slowClient, 0, dbCount)
	for i := 0; i < dbCount; i++ {
		db, sqldb := testingutil.MustOpenDBs(t)
		defer testingutil.MustCloseDBs(t, db, sqldb)

		client := &slowClient{delay: 350 * time.Millisecond}
		db.Replica.Client = client
		dbs = append(dbs, db)
		clients = append(clients, client)

		if _, err := sqldb.ExecContext(t.Context(), `CREATE TABLE t (id INT)`); err != nil {
			t.Fatal(err)
		}
		if err := db.Sync(t.Context()); err != nil {
			t.Fatal(err)
		}
	}

	s := litestream.NewStore(dbs, litestream.CompactionLevels{{Level: 0}})
	s.CompactionMonitorEnabled = false
	if err := s.Open(t.Context()); err != nil {
		t.Fatal(err)
	}
	for _, db := range dbs {
		db.ShutdownSyncTimeout = 1200 * time.Millisecond
		db.ShutdownSyncInterval = 20 * time.Millisecond
	}

	if err := s.Close(context.Background()); err != nil {
		t.Fatalf("close reported an error with every database healthy: %v", err)
	}

	for i, client := range clients {
		writes, midWrite := client.counts()
		if midWrite {
			t.Errorf("database %d was aborted DURING a sync that was inside its own budget", i)
		}
		if writes == 0 {
			t.Errorf("database %d never completed its final sync", i)
		}
	}
}

// TestStore_Close_AbortsBlockedReplicaTransport asserts that a store whose
// replica client is stuck inside a network call still shuts down.
//
// Without the abort, Store.Close cancels its context and then waits on the
// compaction goroutine for ever, because the blocked call cannot see the
// cancellation; the supervisor's kill timeout is what ends the process.
//
// The client is installed BEFORE the database is opened: DB.Open binds the
// compactor to whatever client the replica has at that moment, so replacing it
// afterwards leaves compaction talking to the original.
func TestStore_Close_AbortsBlockedReplicaTransport(t *testing.T) {
	const compactionLevel = 1

	dir := t.TempDir()
	db := testingutil.NewDB(t, filepath.Join(dir, "db"))
	db.MonitorInterval = 0
	db.Replica = litestream.NewReplica(db)
	db.Replica.MonitorEnabled = false
	client := newBlockingAt(file.NewReplicaClient(filepath.Join(dir, "replica")), compactionLevel)
	// A failing test must still unblock: the fixture parks a goroutine that
	// only Abort releases, so a test that fails before its own abort would
	// leave the store wedged and later tests reporting the wrong thing.
	t.Cleanup(client.Abort)
	db.Replica.Client = client
	if err := db.Open(); err != nil {
		t.Fatal(err)
	}
	sqldb := testingutil.MustOpenSQLDB(t, db.Path())
	defer testingutil.MustCloseSQLDB(t, sqldb)

	levels := litestream.CompactionLevels{
		{Level: 0},
		{Level: compactionLevel, Interval: 50 * time.Millisecond},
	}
	s := litestream.NewStore([]*litestream.DB{db}, levels)
	if err := s.Open(t.Context()); err != nil {
		t.Fatal(err)
	}

	// Give the compaction monitor something to upload.
	if _, err := sqldb.ExecContext(t.Context(), `CREATE TABLE t (id INT)`); err != nil {
		t.Fatal(err)
	}
	if err := db.Sync(t.Context()); err != nil {
		t.Fatal(err)
	}
	if err := db.Replica.Sync(t.Context()); err != nil {
		t.Fatal(err)
	}

	// Wait until the compaction upload is parked in the client. That goroutine
	// is registered in the store's wait group, so Store.Close cannot return
	// while it is blocked.
	select {
	case <-client.blocked:
	case <-time.After(30 * time.Second):
		t.Fatal("no level-1 compaction upload was started; nothing would be blocked")
	}

	done := make(chan error, 1)
	go func() { done <- s.Close(context.Background()) }()

	select {
	case err := <-done:
		// Assert the outcome, not just the return. Only the background
		// compaction was blocked and the final sync had a healthy path to the
		// same replica, so an error here means Close interrupted more than it
		// was asked to.
		if err != nil {
			t.Fatalf("shutdown interrupted a healthy final sync: %v", err)
		}
	case <-time.After(15 * time.Second):
		t.Fatal("Store.Close did not return: a blocked replica transport still holds shutdown open")
	}
	if !client.wasAborted() {
		t.Fatal("the blocked transport was never aborted; Close returned for some other reason")
	}
}

// TestStore_Close_LastCommitReachesTheReplica asserts that a transaction
// committed immediately before shutdown is present in a restore taken from the
// replica afterwards.
//
// Shutting down quickly is only worth having if the final sync still happens:
// a fast exit that drops the last commit is worse than a slow one. This uses a
// healthy file-backed replica, so nothing here is aborted — the point is what
// Close guarantees when the transport is fine.
func TestStore_Close_LastCommitReachesTheReplica(t *testing.T) {
	db, sqldb := testingutil.MustOpenDBs(t)
	defer testingutil.MustCloseDBs(t, db, sqldb)

	s := litestream.NewStore([]*litestream.DB{db}, litestream.CompactionLevels{{Level: 0}})
	s.CompactionMonitorEnabled = false
	if err := s.Open(t.Context()); err != nil {
		t.Fatal(err)
	}

	if _, err := sqldb.ExecContext(t.Context(), `CREATE TABLE marker (id TEXT)`); err != nil {
		t.Fatal(err)
	}

	// Establish a baseline on the replica, so the test is about the LAST
	// commit rather than about a cold start.
	if err := db.Sync(t.Context()); err != nil {
		t.Fatal(err)
	}
	if err := db.Replica.Sync(t.Context()); err != nil {
		t.Fatal(err)
	}

	// The last thing to happen before shutdown.
	const marker = "committed-immediately-before-shutdown"
	if _, err := sqldb.ExecContext(t.Context(), `INSERT INTO marker (id) VALUES (?)`, marker); err != nil {
		t.Fatal(err)
	}

	if err := s.Close(context.Background()); err != nil {
		t.Fatalf("close: %v", err)
	}

	outputPath := filepath.Join(t.TempDir(), "restored.db")
	if err := db.Replica.Restore(context.Background(), litestream.RestoreOptions{
		OutputPath: outputPath,
	}); err != nil {
		t.Fatalf("restore: %v", err)
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

// TestDB_Close_MonitorHoldsSyncSemaphore covers the shutdown race where the
// blocked upload is not the final sync's own.
//
// The replica monitor takes the sync semaphore and parks in the transport. The
// final sync then waits for that semaphore, so its deadline expires while it is
// still waiting — and the abort it is owed must not be discarded by the stop
// that follows. Without it, Replica.Stop waits for the monitor for ever.
func TestDB_Close_MonitorHoldsSyncSemaphore(t *testing.T) {
	dir := t.TempDir()
	db := testingutil.NewDB(t, filepath.Join(dir, "db"))
	db.MonitorInterval = 0
	db.ShutdownSyncTimeout = 500 * time.Millisecond
	db.ShutdownSyncInterval = 50 * time.Millisecond
	db.Replica = litestream.NewReplica(db)

	// Block level 0: that is what the replica monitor uploads.
	client := newBlockingAt(file.NewReplicaClient(filepath.Join(dir, "replica")), 0)
	t.Cleanup(client.Abort)
	db.Replica.Client = client
	db.Replica.SyncInterval = 50 * time.Millisecond
	db.Replica.MonitorEnabled = true

	if err := db.Open(); err != nil {
		t.Fatal(err)
	}

	sqldb := testingutil.MustOpenSQLDB(t, db.Path())
	defer testingutil.MustCloseSQLDB(t, sqldb)
	if _, err := sqldb.ExecContext(t.Context(), `CREATE TABLE t (id INT)`); err != nil {
		t.Fatal(err)
	}
	if err := db.Sync(t.Context()); err != nil {
		t.Fatal(err)
	}

	// Wait until the monitor's upload is parked, holding the semaphore.
	select {
	case <-client.blocked:
	case <-time.After(30 * time.Second):
		t.Fatal("the replica monitor never started an upload")
	}

	done := make(chan error, 1)
	go func() { done <- db.Close(context.Background()) }()

	select {
	case <-done:
		// Any error is acceptable: the final sync legitimately failed. What
		// matters is that Close returned rather than waiting on the monitor.
	case <-time.After(15 * time.Second):
		t.Fatal("DB.Close did not return: the monitor's blocked upload still holds it")
	}
}

// countingAborter records aborts, and takes long enough over an upload that a
// short final-sync deadline expires while the attempt is still running.
type countingAborter struct {
	litestream.ReplicaClient
	delay   time.Duration
	aborted chan struct{}
	once    sync.Once
}

func (c *countingAborter) WriteLTXFile(ctx context.Context, level int, minTXID, maxTXID ltx.TXID, r io.Reader) (*ltx.FileInfo, error) {
	time.Sleep(c.delay)
	return c.ReplicaClient.WriteLTXFile(ctx, level, minTXID, maxTXID, r)
}

func (c *countingAborter) Abort() { c.once.Do(func() { close(c.aborted) }) }

// TestDB_Close_StopDoesNotDiscardDueAbort pins down the race between a due
// abort and the stop that follows it.
//
// Both the deadline and the stop signal can be ready at the same moment — the
// deadline expires, and the wait it released returns — at which point a plain
// select is free to take either. The watcher is held at its start here so the
// stop always wins the race, which is the case that must still abort.
func TestDB_Close_StopDoesNotDiscardDueAbort(t *testing.T) {
	release := make(chan struct{})

	dir := t.TempDir()
	db := testingutil.NewDB(t, filepath.Join(dir, "db"))
	db.SetBackstopStarted(func() { <-release })
	db.MonitorInterval = 0
	db.ShutdownSyncTimeout = 100 * time.Millisecond
	db.ShutdownSyncInterval = 20 * time.Millisecond
	db.Replica = litestream.NewReplica(db)

	client := &countingAborter{
		ReplicaClient: file.NewReplicaClient(filepath.Join(dir, "replica")),
		delay:         400 * time.Millisecond, // longer than the 100ms budget
		aborted:       make(chan struct{}),
	}
	db.Replica.Client = client
	db.Replica.MonitorEnabled = false

	if err := db.Open(); err != nil {
		t.Fatal(err)
	}
	sqldb := testingutil.MustOpenSQLDB(t, db.Path())
	defer testingutil.MustCloseSQLDB(t, sqldb)
	if _, err := sqldb.ExecContext(t.Context(), `CREATE TABLE t (id INT)`); err != nil {
		t.Fatal(err)
	}
	if err := db.Sync(t.Context()); err != nil {
		t.Fatal(err)
	}

	closed := make(chan error, 1)
	go func() { closed <- db.Close(context.Background()) }()

	// The watcher cannot act; the stop path must perform the abort itself.
	select {
	case <-client.aborted:
	case <-time.After(5 * time.Second):
		close(release)
		<-closed
		t.Fatal("a due abort was discarded by the stop that followed it")
	}

	close(release)
	select {
	case <-closed:
	case <-time.After(10 * time.Second):
		t.Fatal("DB.Close did not return")
	}
}

// TestDB_Close_ClientWithoutAbort_IsLeftAlone asserts that a replica client
// which cannot be aborted is not announced as one that was.
//
// The file and s3 clients do not implement the optional interface, so a
// watcher and a "closing replica transport" warning for them would name a
// teardown that never happened. A log line claiming the transport was closed
// is worse than no line, because it sends the reader looking for a connection
// nothing touched.
func TestDB_Close_ClientWithoutAbort_IsLeftAlone(t *testing.T) {
	var mu sync.Mutex
	var records []string
	handler := slog.NewTextHandler(io.Discard, nil)

	dir := t.TempDir()
	db := testingutil.NewDB(t, filepath.Join(dir, "db"))
	db.MonitorInterval = 0
	db.ShutdownSyncTimeout = 100 * time.Millisecond
	db.ShutdownSyncInterval = 20 * time.Millisecond
	db.Logger = slog.New(&capturing{h: handler, mu: &mu, out: &records})
	db.Replica = litestream.NewReplica(db)

	// slowReplica takes longer than the budget and has NO Abort method, so the
	// backstop has nothing it could do.
	db.Replica.Client = &slowNoAborter{
		ReplicaClient: file.NewReplicaClient(filepath.Join(dir, "replica")),
		delay:         400 * time.Millisecond,
	}
	db.Replica.MonitorEnabled = false

	if _, ok := db.Replica.Client.(litestream.ReplicaClientAborter); ok {
		t.Fatal("the fixture must not implement the aborter interface")
	}

	if err := db.Open(); err != nil {
		t.Fatal(err)
	}
	sqldb := testingutil.MustOpenSQLDB(t, db.Path())
	defer testingutil.MustCloseSQLDB(t, sqldb)
	if _, err := sqldb.ExecContext(t.Context(), `CREATE TABLE t (id INT)`); err != nil {
		t.Fatal(err)
	}
	if err := db.Sync(t.Context()); err != nil {
		t.Fatal(err)
	}

	_ = db.Close(context.Background())

	mu.Lock()
	defer mu.Unlock()
	for _, msg := range records {
		if strings.Contains(msg, "closing replica transport") {
			t.Fatalf("a client with no Abort was announced as torn down: %q", msg)
		}
	}
}

type slowNoAborter struct {
	litestream.ReplicaClient
	delay time.Duration
}

func (c *slowNoAborter) WriteLTXFile(ctx context.Context, level int, minTXID, maxTXID ltx.TXID, r io.Reader) (*ltx.FileInfo, error) {
	time.Sleep(c.delay)
	return c.ReplicaClient.WriteLTXFile(ctx, level, minTXID, maxTXID, r)
}

// capturing records message text; the shape of the handler is not the point.
type capturing struct {
	h   slog.Handler
	mu  *sync.Mutex
	out *[]string
}

func (c *capturing) Enabled(ctx context.Context, l slog.Level) bool { return true }
func (c *capturing) Handle(ctx context.Context, r slog.Record) error {
	c.mu.Lock()
	*c.out = append(*c.out, r.Message)
	c.mu.Unlock()
	return nil
}
func (c *capturing) WithAttrs(a []slog.Attr) slog.Handler { return c }
func (c *capturing) WithGroup(n string) slog.Handler      { return c }

// TestDB_Close_ParentCancellation covers the shutdown route the abort tests do
// not: nobody calls Abort, and the context handed to Close is cancelled while
// a replica upload is parked.
//
// Calling Abort directly tests the mechanism, not the route. In production the
// trigger is a context ending, and the backstop is what turns that into a
// teardown. Both budgets are covered, because zero timeout takes a different
// branch: one attempt, no deadline of its own, and therefore nothing but
// cancellation to end it.
func TestDB_Close_ParentCancellation(t *testing.T) {
	for _, tc := range []struct {
		name    string
		timeout time.Duration
	}{
		{"non-zero timeout", 30 * time.Second}, // long enough that only the cancel can end it
		{"zero timeout", 0},                    // single attempt, no deadline at all
	} {
		t.Run(tc.name, func(t *testing.T) {
			dir := t.TempDir()
			db := testingutil.NewDB(t, filepath.Join(dir, "db"))
			db.MonitorInterval = 0
			db.ShutdownSyncTimeout = tc.timeout
			db.ShutdownSyncInterval = 20 * time.Millisecond
			db.Replica = litestream.NewReplica(db)

			client := newBlockingAt(file.NewReplicaClient(filepath.Join(dir, "replica")), 0)
			t.Cleanup(client.Abort)
			client.disarm() // let the baseline through
			db.Replica.Client = client
			db.Replica.MonitorEnabled = false

			if err := db.Open(); err != nil {
				t.Fatal(err)
			}
			sqldb := testingutil.MustOpenSQLDB(t, db.Path())
			defer testingutil.MustCloseSQLDB(t, sqldb)
			if _, err := sqldb.ExecContext(t.Context(), `CREATE TABLE t (id INT)`); err != nil {
				t.Fatal(err)
			}
			if err := db.Sync(t.Context()); err != nil {
				t.Fatal(err)
			}
			if err := db.Replica.Sync(t.Context()); err != nil {
				t.Fatal(err)
			}

			// From here the replica parks: the final sync is what this test is
			// about, and it must have something to upload.
			client.arm()
			if _, err := sqldb.ExecContext(t.Context(), `INSERT INTO t (id) VALUES (1)`); err != nil {
				t.Fatal(err)
			}
			if err := db.Sync(t.Context()); err != nil {
				t.Fatal(err)
			}

			ctx, cancel := context.WithCancel(context.Background())
			defer cancel()

			closed := make(chan error, 1)
			go func() { closed <- db.Close(ctx) }()

			// Wait until the final sync is genuinely parked in the client.
			select {
			case <-client.blocked:
			case err := <-closed:
				t.Fatalf("Close returned before the upload blocked: %v", err)
			case <-time.After(15 * time.Second):
				t.Fatal("no upload was started; nothing would be blocked")
			}

			cancel()

			select {
			case err := <-closed:
				// The failure must still be reported: a cancelled final sync is
				// not a successful shutdown, and a Close that swallows it tells
				// an operator the replica is current when it is not.
				if err == nil {
					t.Fatal("Close reported success for a final sync that never completed")
				}
			case <-time.After(15 * time.Second):
				t.Fatal("cancelling the parent did not release the blocked transport")
			}

			if !client.wasAborted() {
				t.Fatal("the transport was never aborted; Close returned for some other reason")
			}
		})
	}
}

// TestDB_Close_SecondSignal covers the other way a shutdown ends early: the
// operator sends a second SIGTERM, which closes db.Done while the final sync
// is parked in the replica client.
//
// The second signal is a promise that the process will go. It closes Done,
// which the retry loop notices between attempts; a goroutine parked inside the
// client notices nothing, so without a teardown the answer to "stop now" is the
// same hang and the operator's next step is SIGKILL. Both budgets are covered
// because the zero-timeout branch makes a single attempt with no deadline, so
// nothing but this signal can end it.
func TestDB_Close_SecondSignal(t *testing.T) {
	for _, tc := range []struct {
		name    string
		timeout time.Duration
	}{
		{"non-zero timeout", 30 * time.Second},
		{"zero timeout", 0},
	} {
		t.Run(tc.name, func(t *testing.T) {
			dir := t.TempDir()
			db := testingutil.NewDB(t, filepath.Join(dir, "db"))
			db.MonitorInterval = 0
			db.ShutdownSyncTimeout = tc.timeout
			db.ShutdownSyncInterval = 20 * time.Millisecond

			done := make(chan struct{})
			db.Done = done
			db.Replica = litestream.NewReplica(db)

			client := newBlockingAt(file.NewReplicaClient(filepath.Join(dir, "replica")), 0)
			t.Cleanup(client.Abort)
			client.disarm()
			db.Replica.Client = client
			db.Replica.MonitorEnabled = false

			if err := db.Open(); err != nil {
				t.Fatal(err)
			}
			sqldb := testingutil.MustOpenSQLDB(t, db.Path())
			defer testingutil.MustCloseSQLDB(t, sqldb)
			if _, err := sqldb.ExecContext(t.Context(), `CREATE TABLE t (id INT)`); err != nil {
				t.Fatal(err)
			}
			if err := db.Sync(t.Context()); err != nil {
				t.Fatal(err)
			}
			if err := db.Replica.Sync(t.Context()); err != nil {
				t.Fatal(err)
			}

			client.arm()
			if _, err := sqldb.ExecContext(t.Context(), `INSERT INTO t (id) VALUES (1)`); err != nil {
				t.Fatal(err)
			}
			if err := db.Sync(t.Context()); err != nil {
				t.Fatal(err)
			}

			closed := make(chan error, 1)
			go func() { closed <- db.Close(context.Background()) }()

			select {
			case <-client.blocked:
			case err := <-closed:
				t.Fatalf("Close returned before the upload blocked: %v", err)
			case <-time.After(15 * time.Second):
				t.Fatal("no upload was started; nothing would be blocked")
			}

			close(done) // the second signal

			select {
			case err := <-closed:
				if err == nil {
					t.Fatal("Close reported success for a final sync the operator interrupted")
				}
			case <-time.After(15 * time.Second):
				t.Fatal("the second signal did not release the blocked transport")
			}

			if !client.wasAborted() {
				t.Fatal("the transport was never aborted; Close returned for some other reason")
			}
		})
	}
}
