package sftp_test

import (
	"context"
	"io"
	"testing"
	"time"
)

// The read-side counterpart of TestReplicaClient_Abort_StalledWrite: the
// handshake and the upload succeed, the file opens, and then the server stops
// answering a READ.
//
// The opened file is read through a buffer, so a six-byte consumer read becomes
// a much larger request on the wire. Abort must still release it.
func TestReplicaClient_Abort_StalledRead(t *testing.T) {
	srv := startTestServer(t, stallAtRead)
	c := newClient(t, srv)

	// Content to read back. Uploads are not stalled in this mode.
	if _, err := c.WriteLTXFile(context.Background(), 0, 1, 1, ltxFile(t)); err != nil {
		t.Fatalf("upload failed before anything was stalled: %v", err)
	}

	rc, err := c.OpenLTXFile(context.Background(), 0, 1, 1, 0, 0)
	if err != nil {
		t.Fatalf("open failed before the stall: %v", err)
	}
	// Abort first: Close on a reader parked in a stalled READ blocks, and a
	// cleanup that blocks turns a failed assertion below into a test timeout
	// rather than the failure it was.
	t.Cleanup(func() {
		c.Abort()
		_ = rc.Close()
	})

	// A read the size the LTX decoder asks for.
	ch := make(chan error, 1)
	go func() {
		buf := make([]byte, 6)
		_, err := io.ReadFull(rc, buf)
		ch <- err
	}()

	// The stall is where the test says it is.
	select {
	case <-srv.reached:
	case err := <-ch:
		t.Fatalf("the read returned before the server saw a READ: %v", err)
	case <-time.After(30 * time.Second):
		t.Fatal("no READ reached the server; nothing was stalled")
	}

	// Still blocked, so the deadline below means something.
	select {
	case err := <-ch:
		t.Fatalf("the read returned while the server was not answering: %v", err)
	case <-time.After(100 * time.Millisecond):
	}

	c.Abort()

	// Bounded claim: Abort releases the read. The error is recorded, not
	// classified.
	err = mustReturn(t, ch, 10*time.Second, "stalled read")
	if err == nil {
		t.Fatal("the read succeeded against a server that answered no READ")
	}
	t.Logf("released read returned: %v", err)
}
