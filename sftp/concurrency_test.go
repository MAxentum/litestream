package sftp_test

import (
	"bytes"
	"context"
	"testing"

	"github.com/superfly/ltx"
)

// TestWriteLTXFile_UploadsConcurrently asserts that an upload large enough to
// justify it actually issues overlapping WRITE requests.
//
// Reaching File.ReadFrom does not establish this on its own. pkg/sftp v1.13.6
// chooses between a concurrent upload and a sequential 32 KB loop by asking the
// reader how much is left (Len, Size, *io.LimitedReader or Stat), so a
// destination wrapper handing it a reader that exposes only Read gets the
// sequential path: the optimisation is entered and then declines itself. A test
// that only checks the upload succeeded passes either way.
//
// The server counts WRITE requests outstanding at the same moment. Sequential
// means exactly one.
func TestWriteLTXFile_UploadsConcurrently(t *testing.T) {
	srv := startTestServer(t, countConcurrentWrites)
	c := newClient(t, srv)
	c.ConcurrentWrites = true

	if err := c.Init(context.Background()); err != nil {
		t.Fatal(err)
	}

	// The source must not implement io.WriterTo, or this measures a different
	// mechanism: a *bytes.Reader writes itself into the destination in one
	// call, and File.Write parallelises a large single write on its own, so the
	// result would describe File.Write rather than File.ReadFrom.
	//
	// A reader that can state its size but cannot write itself is the case the
	// destination's ReadFrom exists for.
	body := ltxFileOfSize(t, 4<<20)
	if _, err := c.WriteLTXFile(context.Background(), 0, 1, 1, sizedSource{r: bytes.NewReader(body), n: int64(len(body))}); err != nil {
		t.Fatal(err)
	}

	if got := srv.counter.maxOutstanding(); got < 2 {
		t.Fatalf("the upload was sequential: at most %d WRITE request outstanding at once", got)
	} else {
		t.Logf("maximum outstanding WRITE requests: %d", got)
	}
}

// TestWriteLTXFile_SequentialWhenSizeUnknown records the other half of the
// contract: a source that cannot say how much is left gets the sequential
// path, exactly as it would without the wrapper. The wrapper must not invent
// a size.
func TestWriteLTXFile_SequentialWhenSizeUnknown(t *testing.T) {
	srv := startTestServer(t, countConcurrentWrites)
	c := newClient(t, srv)
	c.ConcurrentWrites = true

	if err := c.Init(context.Background()); err != nil {
		t.Fatal(err)
	}

	body := ltxFileOfSize(t, 4<<20)
	opaque := struct{ r *bytes.Reader }{bytes.NewReader(body)}
	if _, err := c.WriteLTXFile(context.Background(), 0, 1, 1, readOnly{opaque.r}); err != nil {
		t.Fatal(err)
	}

	if got := srv.counter.maxOutstanding(); got != 1 {
		t.Fatalf("expected the sequential path for a size-less source, saw %d outstanding", got)
	}
}

type readOnly struct{ r *bytes.Reader }

func (r readOnly) Read(p []byte) (int, error) { return r.r.Read(p) }

// sizedSource can say how big it is and cannot write itself: Read plus Size,
// deliberately no WriteTo.
type sizedSource struct {
	r *bytes.Reader
	n int64
}

func (s sizedSource) Read(p []byte) (int, error) { return s.r.Read(p) }
func (s sizedSource) Size() int64                { return s.n }

var _ = ltx.HeaderSize
