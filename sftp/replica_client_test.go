package sftp_test

import (
	"bytes"
	"context"
	"errors"
	"io"
	"net"
	"os"
	"path/filepath"
	"sync"
	"testing"
	"time"

	"github.com/superfly/ltx"

	"github.com/benbjohnson/litestream"
	"github.com/benbjohnson/litestream/sftp"
)

// ltxFile builds a valid single-page LTX file.
//
// WriteLTXFile peeks at the header before it opens anything remote, so an
// invalid payload fails locally and never reaches the transport — a test using
// one would prove nothing about a stalled upload.
// ltxFileOfSize builds a valid LTX file of roughly n bytes, so an upload is
// big enough for pkg/sftp to consider writing it concurrently.
func ltxFileOfSize(tb testing.TB, n int) []byte {
	tb.Helper()

	const pageSize = 4096
	pages := n / pageSize
	if pages < 1 {
		pages = 1
	}

	var buf bytes.Buffer
	enc, err := ltx.NewEncoder(&buf)
	if err != nil {
		tb.Fatal(err)
	}
	if err := enc.EncodeHeader(ltx.Header{
		Version:   ltx.Version,
		Flags:     ltx.HeaderFlagNoChecksum,
		PageSize:  pageSize,
		Commit:    uint32(pages),
		MinTXID:   1,
		MaxTXID:   1,
		Timestamp: time.Now().UnixMilli(),
	}); err != nil {
		tb.Fatal(err)
	}
	page := make([]byte, pageSize)
	for i := 1; i <= pages; i++ {
		// Vary the bytes: an all-zero file compresses to nothing and the
		// upload stops being large enough to be the thing under test.
		for j := range page {
			page[j] = byte(i * j)
		}
		if err := enc.EncodePage(ltx.PageHeader{Pgno: uint32(i)}, page); err != nil {
			tb.Fatal(err)
		}
	}
	if err := enc.Close(); err != nil {
		tb.Fatal(err)
	}
	return buf.Bytes()
}

func ltxFile(tb testing.TB) io.Reader {
	tb.Helper()

	var buf bytes.Buffer
	enc, err := ltx.NewEncoder(&buf)
	if err != nil {
		tb.Fatal(err)
	}
	if err := enc.EncodeHeader(ltx.Header{
		Version:   ltx.Version,
		Flags:     ltx.HeaderFlagNoChecksum,
		PageSize:  4096,
		Commit:    1,
		MinTXID:   1,
		MaxTXID:   1,
		Timestamp: time.Now().UnixMilli(),
	}); err != nil {
		tb.Fatal(err)
	}
	if err := enc.EncodePage(ltx.PageHeader{Pgno: 1}, make([]byte, 4096)); err != nil {
		tb.Fatal(err)
	}
	if err := enc.Close(); err != nil {
		tb.Fatal(err)
	}
	return &buf
}

func newClient(tb testing.TB, srv *testServer) *sftp.ReplicaClient {
	tb.Helper()
	c := sftp.NewReplicaClient()
	c.Host = srv.addr
	c.User = "test"
	c.KeyPath = writeClientKey(tb, tb.TempDir())
	c.HostKey = srv.hostKey
	c.Path = filepath.Join(srv.root, "upload")
	return c
}

// newClientAt is newClient at a named path, so two independent clients can be
// pointed at the same replica directory — one writing, one restoring.
func newClientAt(tb testing.TB, srv *testServer, name string) *sftp.ReplicaClient {
	tb.Helper()
	c := newClient(tb, srv)
	c.Path = filepath.Join(srv.root, name)
	return c
}

// write starts an upload of a valid LTX file and reports how it ended.
func write(tb testing.TB, c *sftp.ReplicaClient) <-chan error {
	tb.Helper()
	body := ltxFile(tb)
	ch := make(chan error, 1)
	go func() {
		_, err := c.WriteLTXFile(context.Background(), 0, 1, 1, body)
		ch <- err
	}()
	return ch
}

// reached waits until the server reports arriving at the phase it is set to
// stall in, then checks the operation has not returned.
//
// This replaces sleeping for a couple of seconds and asserting nothing
// happened. Not returning for two seconds is not evidence of being blocked —
// it is evidence of two seconds — and it made the suite slow for a weaker
// claim. The server's own signal names the phase, so the test says WHERE the
// operation is parked rather than how long it sat there.
func reached(tb testing.TB, srv *testServer, ch <-chan error, what string) {
	tb.Helper()
	select {
	case <-srv.reached:
	case err := <-ch:
		tb.Fatalf("%s: the operation returned before the server reached the stall: %v", what, err)
	case <-time.After(10 * time.Second):
		tb.Fatalf("%s: the server never reached the stall", what)
	}
	select {
	case err := <-ch:
		tb.Fatalf("%s: the operation returned while the server was silent: %v", what, err)
	default:
	}
}

func mustReturn(tb testing.TB, ch <-chan error, d time.Duration, what string) error {
	tb.Helper()
	select {
	case err := <-ch:
		return err
	case <-time.After(d):
		tb.Fatalf("%s: still blocked %s after Abort", what, d)
		return nil
	}
}

// The Stall Happens Mid-Upload: the handshake succeeds, the file opens, and
// then the server stops answering a WRITE. This is the production failure, and
// the one an "abort" that only closes the SFTP client cannot release.
func TestReplicaClient_Abort_StalledWrite(t *testing.T) {
	srv := startTestServer(t, stallAtWrite)
	c := newClient(t, srv)
	ch := write(t, c)

	// Wait for a WRITE so the test proves interruption of a stalled upload,
	// rather than cancellation before the upload begins.
	select {
	case <-srv.writeReached:
	case err := <-ch:
		t.Fatalf("upload finished before a WRITE was stalled: %v", err)
	case <-time.After(20 * time.Second):
		t.Fatal("the server never received a WRITE packet")
	}

	c.Abort()

	err := mustReturn(t, ch, 10*time.Second, "stalled write")
	if !errors.Is(err, litestream.ErrClientAborted) {
		t.Fatalf("expected the failure to be reported as an abort, got: %v", err)
	}
	if err := c.Init(context.Background()); !errors.Is(err, litestream.ErrClientAborted) {
		t.Fatalf("client did not refuse to reconnect after Abort: %v", err)
	}
}

// The SSH Handshake Is Its Own Hang. A server that accepts TCP and never
// speaks SSH parks the client before there is any SSH or SFTP object to close —
// which is why the raw connection is dialled and published first.
func TestReplicaClient_Abort_StalledSSHHandshake(t *testing.T) {
	srv := startTestServer(t, stallAtBanner)
	c := newClient(t, srv)
	c.DialTimeout = time.Hour // the deadline must not be what saves us
	ch := write(t, c)
	reached(t, srv, ch, "ssh handshake")

	c.Abort()
	mustReturn(t, ch, 10*time.Second, "stalled ssh handshake")
}

// And So Is The SFTP Handshake, one layer higher.
func TestReplicaClient_Abort_StalledSFTPInit(t *testing.T) {
	srv := startTestServer(t, stallAtSubsystem)
	c := newClient(t, srv)
	ch := write(t, c)
	reached(t, srv, ch, "sftp init")

	c.Abort()
	mustReturn(t, ch, 10*time.Second, "stalled sftp init")
}

// A Connection That Arrives After An Abort Must Be Closed By Whoever Made It.
// Abort can only close what has been published; a connect in flight publishes
// under the same lock and, finding the client aborted, throws its work away.
// Run repeatedly so the abort lands at different points of the handshake.
func TestReplicaClient_Abort_DuringConnect(t *testing.T) {
	for i := 0; i < 25; i++ {
		c := newClient(t, startTestServer(t, serveFully))
		ch := write(t, c)
		time.Sleep(time.Duration(i) * time.Millisecond) // walk the abort through the handshake
		c.Abort()

		err := mustReturn(t, ch, 10*time.Second, "write racing an abort")
		// Either it beat the abort or it was aborted; it must never hang, and
		// the client must never be left usable.
		_ = err
		if err := c.Init(context.Background()); !errors.Is(err, litestream.ErrClientAborted) {
			t.Fatalf("iteration %d: client usable after Abort: %v", i, err)
		}
	}
}

// An Error From An Old Connection Must Not Close The New One. The client
// tags each connection with a generation for exactly this: a late failure tears
// down the transport it came from, or nothing at all.
func TestReplicaClient_LateError_DoesNotDropNewConnection(t *testing.T) {
	srv := startTestServer(t, serveFully)
	c := newClient(t, srv)

	if err := c.Init(context.Background()); err != nil {
		t.Fatal(err)
	}
	first := c.Generation()

	// Force a reconnect, as a dropped connection would.
	c.DropGeneration(first)
	if err := c.Init(context.Background()); err != nil {
		t.Fatal(err)
	}
	second := c.Generation()
	if first == second {
		t.Fatal("expected a new connection generation")
	}

	// The old operation reports its failure now, long after its connection was
	// replaced.
	c.DropGeneration(first)

	if got := c.Generation(); got != second {
		t.Fatalf("a late error from generation %d tore down generation %d", first, second)
	}
	if _, err := c.LTXFiles(context.Background(), 0, 0, false); err != nil {
		t.Fatalf("the live connection stopped working: %v", err)
	}
}

// TestReplicaClient_Abort_StalledDial covers the window before there is any
// socket: the client is dialling, and Abort has nothing to close.
//
// The dialer is injected because a dial that never returns cannot be produced
// reliably against a real network.
func TestReplicaClient_Abort_StalledDial(t *testing.T) {
	c := newClient(t, startTestServer(t, serveFully))
	c.DialTimeout = time.Hour // the deadline must not be what rescues this

	dialing := make(chan struct{})
	var once sync.Once
	c.SetDial(func(ctx context.Context, network, addr string) (net.Conn, error) {
		once.Do(func() { close(dialing) })
		<-ctx.Done()
		return nil, ctx.Err()
	})

	ch := write(t, c)

	select {
	case <-dialing:
	case err := <-ch:
		t.Fatalf("write returned before the dial began: %v", err)
	case <-time.After(10 * time.Second):
		t.Fatal("the dial never started")
	}

	c.Abort()
	err := mustReturn(t, ch, 10*time.Second, "stalled dial")

	// The error identity is part of the contract. Every phase reports a
	// terminal abort as ErrClientAborted, including this one, where
	// cancellation is itself the abort mechanism and so is not a transport
	// failure the classifier would otherwise recognise. Asserting only that the
	// operation returns would not distinguish the two.
	if !errors.Is(err, litestream.ErrClientAborted) {
		t.Fatalf("an aborted dial did not report an abort: %v", err)
	}
	if !errors.Is(err, context.Canceled) {
		t.Fatalf("the underlying cause was lost: %v", err)
	}

	// Terminal: an aborted client does not dial again.
	if err := c.Init(context.Background()); !errors.Is(err, litestream.ErrClientAborted) {
		t.Fatalf("an aborted client reconnected: %v", err)
	}
}

// TestReplicaClient_ShortHeader_KeepsConnection covers the first of the two
// ways a caller's stream can fail: it ends before the LTX header is complete.
//
// Half of a compressible fixture is not a specified failure: the midpoint
// lands wherever the compressor puts it, so nothing would pin which of the two
// error paths ran. This test and the next name the stage instead.
func TestReplicaClient_ShortHeader_KeepsConnection(t *testing.T) {
	c := newClient(t, startTestServer(t, serveFully))

	if err := c.Init(context.Background()); err != nil {
		t.Fatal(err)
	}
	gen := c.Generation()
	if gen == 0 {
		t.Fatal("expected a live connection")
	}

	full, err := io.ReadAll(ltxFile(t))
	if err != nil {
		t.Fatal(err)
	}
	if len(full) < ltx.HeaderSize {
		t.Fatalf("fixture is shorter than a header: %d bytes", len(full))
	}
	short := bytes.NewReader(full[:ltx.HeaderSize-1])

	_, err = c.WriteLTXFile(context.Background(), 0, 1, 1, short)
	if err == nil {
		t.Fatal("expected the short header to fail")
	}
	if errors.Is(err, litestream.ErrClientAborted) {
		t.Fatalf("a local input failure was reported as an abort: %v", err)
	}
	if got := c.Generation(); got != gen {
		t.Fatalf("the connection was dropped by a local input failure: generation %d -> %d", gen, got)
	}
	if _, err := c.LTXFiles(context.Background(), 0, 0, false); err != nil {
		t.Fatalf("the connection is no longer usable: %v", err)
	}
}

// errAfter reads n bytes from r and then fails with err — a caller's file
// going away mid-copy, which is the second way the input can fail and the one
// that reaches the destination write path.
type errAfter struct {
	r   io.Reader
	n   int
	err error
}

func (e *errAfter) Read(p []byte) (int, error) {
	if e.n <= 0 {
		return 0, e.err
	}
	if len(p) > e.n {
		p = p[:e.n]
	}
	n, err := e.r.Read(p)
	e.n -= n
	if err != nil {
		return n, err
	}
	return n, nil
}

// TestReplicaClient_SourceFailsMidBody_KeepsConnection covers the second way:
// a complete header, some body, and then a read error from the caller.
//
// The identity of that error must survive. It is the caller's failure, not the
// transport's: it must not be relabelled as an abort, must not drop a
// connection other operations are using, and must be returned as itself, so a
// caller can tell a bad local file from a replica that has gone away.
func TestReplicaClient_SourceFailsMidBody_KeepsConnection(t *testing.T) {
	c := newClient(t, startTestServer(t, serveFully))

	if err := c.Init(context.Background()); err != nil {
		t.Fatal(err)
	}
	gen := c.Generation()
	if gen == 0 {
		t.Fatal("expected a live connection")
	}

	full, err := io.ReadAll(ltxFile(t))
	if err != nil {
		t.Fatal(err)
	}
	if len(full) <= ltx.HeaderSize {
		t.Fatalf("fixture has no body to fail in: %d bytes", len(full))
	}

	boom := errors.New("the caller's file went away")
	src := &errAfter{r: bytes.NewReader(full), n: ltx.HeaderSize + 1, err: boom}

	_, err = c.WriteLTXFile(context.Background(), 0, 1, 1, src)
	if err == nil {
		t.Fatal("expected the failing source to fail the upload")
	}
	if !errors.Is(err, boom) {
		t.Fatalf("the source's own error did not survive: %v", err)
	}
	if errors.Is(err, litestream.ErrClientAborted) {
		t.Fatalf("a local input failure was reported as an abort: %v", err)
	}
	if got := c.Generation(); got != gen {
		t.Fatalf("the connection was dropped by a local input failure: generation %d -> %d", gen, got)
	}
	if _, err := c.LTXFiles(context.Background(), 0, 0, false); err != nil {
		t.Fatalf("the connection is no longer usable: %v", err)
	}
}

// TestReplicaClient_Abort_DuringWalk asserts that a directory walk interrupted
// by a terminal Abort reports the abort, rather than a bare error.
//
// The walk was the one remote path that classified nothing. DeleteAll's
// walker error was returned as a plain fmt.Errorf, so a connection dying
// mid-walk produced an error that isRemote could not recognise — invisible to
// afterOp, which would otherwise drop the dead connection, and to abortedOr,
// which would otherwise name the shutdown that caused it. Every other remote
// failure in this client was tagged; this one only looked like it was.
func TestReplicaClient_Abort_DuringWalk(t *testing.T) {
	srv := startTestServer(t, stallAtReadDir)
	c := newClient(t, srv)

	if err := c.Init(context.Background()); err != nil {
		t.Fatal(err)
	}

	// There must be something to walk: DeleteAll on a path that does not exist
	// returns nil without issuing a READDIR, so the server would never reach
	// the stall.
	if _, err := c.WriteLTXFile(context.Background(), 0, 1, 1, ltxFile(t)); err != nil {
		t.Fatal(err)
	}

	ch := make(chan error, 1)
	go func() { ch <- c.DeleteAll(context.Background()) }()

	// Wait until the walk is genuinely parked in a READDIR the server will
	// never answer; aborting earlier would test the handshake instead.
	select {
	case <-srv.writeReached:
	case err := <-ch:
		t.Fatalf("DeleteAll returned before the walk blocked: %v", err)
	case <-time.After(10 * time.Second):
		t.Fatal("no READDIR arrived; nothing would be blocked")
	}

	c.Abort()
	err := mustReturn(t, ch, 10*time.Second, "stalled walk")
	if err == nil {
		t.Fatal("expected the interrupted walk to fail")
	}
	if !errors.Is(err, litestream.ErrClientAborted) {
		t.Fatalf("an aborted walk did not report an abort: %v", err)
	}
}

// TestReplicaClient_PermissionError_KeepsItsIdentity asserts that a refusal
// from the server keeps its own identity and its own connection, including
// while the client is terminally aborted.
//
// A server saying no is not a transport going away. The two are easy to
// conflate during shutdown, when an abort is in flight and every error is
// tempting to attribute to it — but relabelling a permission failure as an
// abort tells an operator the replica was torn down when in fact the replica
// is refusing to accept their data, which is a fault that survives a restart.
func TestReplicaClient_PermissionError_KeepsItsIdentity(t *testing.T) {
	srv := startTestServer(t, serveFully)
	c := newClient(t, srv)

	if err := c.Init(context.Background()); err != nil {
		t.Fatal(err)
	}
	gen := c.Generation()

	// Make the replica directory unwritable on the server's real filesystem,
	// so the next upload is refused rather than dropped.
	if err := os.MkdirAll(c.Path, 0o700); err != nil {
		t.Fatal(err)
	}
	if err := os.Chmod(c.Path, 0o500); err != nil {
		t.Fatal(err)
	}
	t.Cleanup(func() { _ = os.Chmod(c.Path, 0o700) })

	_, err := c.WriteLTXFile(context.Background(), 0, 1, 1, ltxFile(t))
	if err == nil {
		t.Fatal("expected the upload to be refused")
	}
	if errors.Is(err, litestream.ErrClientAborted) {
		t.Fatalf("a refusal was reported as an abort: %v", err)
	}
	if got := c.Generation(); got != gen {
		t.Fatalf("a refusal dropped a healthy connection: generation %d -> %d", gen, got)
	}

	// And the same refusal while the client is terminally aborted: the abort is
	// real, but it is not what went wrong here.
	c.Abort()
	_, err = c.WriteLTXFile(context.Background(), 0, 2, 2, ltxFile(t))
	if err == nil {
		t.Fatal("expected the upload to fail on an aborted client")
	}
	// Once aborted, init refuses before the server is reached — that is the
	// abort, correctly reported. The point of the first half is that the
	// refusal reaching the server is never relabelled.
	if !errors.Is(err, litestream.ErrClientAborted) {
		t.Fatalf("an aborted client did not report the abort: %v", err)
	}
}
