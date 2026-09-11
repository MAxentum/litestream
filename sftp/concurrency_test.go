package sftp_test

import (
	"bytes"
	"context"
	"testing"

	"github.com/superfly/ltx"
)

// TestWriteLTXFile_UploadsConcurrently asserts that an upload large enough to
// Justify it actually issues overlapping WRITE requests.
//
// This is The claim that reaching ReadFrom Does not establish. Pkg/sftp
// V1.13.6 chooses between a concurrent upload and a sequential 32 KB loop by
// Asking the reader how much is left (Len, Size, *io.LimitedReader or Stat).
// A destination wrapper that hands it a reader exposing only Read gets the
// Sequential path — the optimisation is entered and then declines itself, and
// Every test that only checks the upload succeeded passes either way.
//
// The server counts WRITE requests that are outstanding at the same moment.
// Sequential means exactly one.
func TestWriteLTXFile_UploadsConcurrently(t *testing.T) {
	srv := startTestServer(t, countConcurrentWrites)
	c := newClient(t, srv)
	c.ConcurrentWrites = true

	if err := c.Init(context.Background()); err != nil {
		t.Fatal(err)
	}

	// The source must not implement io.WriterTo, or this measures a
	// Different mechanism entirely. A *bytes.Reader writes itself into the
	// Destination in one 4 MB call, and File.Write parallelises a large single
	// Write on its own — so the first version of this test reported four
	// Concurrent requests with the fix removed, and with the whole ReadFrom
	// Method removed. It was measuring File.Write, not File.ReadFrom.
	//
	// A reader that can state its size but cannot write itself is exactly the
	// Case the destination's ReadFrom exists for, and the case the wrapper
	// Used to break.
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
// Contract: a source that cannot say how much is left gets the sequential
// Path, exactly as it would without the wrapper. The wrapper must not invent
// A size.
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

// SizedSource can say how big it is and cannot write itself: Read plus Size,
// Deliberately no WriteTo.
type sizedSource struct {
	r *bytes.Reader
	n int64
}

func (s sizedSource) Read(p []byte) (int, error) { return s.r.Read(p) }
func (s sizedSource) Size() int64                { return s.n }

var _ = ltx.HeaderSize
