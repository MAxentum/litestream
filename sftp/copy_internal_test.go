package sftp

import (
	"errors"
	"io"
	"strings"
	"testing"
)

type readerFromWriter struct {
	used bool
	n    int64
	err  error
}

func (w *readerFromWriter) Write(p []byte) (int, error) { return len(p), nil }
func (w *readerFromWriter) ReadFrom(r io.Reader) (int64, error) {
	w.used = true
	n, err := io.Copy(io.Discard, r)
	w.n = n
	if w.err != nil {
		return n, w.err
	}
	return n, err
}

// TestRemoteWriter_UsesReadFrom asserts that io.Copy still reaches the
// Destination's optimised path through the wrapper. Asserting that an upload
// Merely succeeds cannot see this: the slow path succeeds too.
func TestRemoteWriter_UsesReadFrom(t *testing.T) {
	dst := &readerFromWriter{}
	// The source must not implement io.WriterTo: io.Copy asks the source
	// First, so a reader with WriteTo never consults the destination and the
	// Dispatch under test is never exercised.
	n, err := io.Copy(remoteWriter{w: dst}, plainReader(strings.NewReader("hello")))
	if err != nil {
		t.Fatal(err)
	}
	if !dst.used {
		t.Fatal("io.Copy did not dispatch to ReadFrom: the wrapper hides the fast path")
	}
	if n != 5 {
		t.Fatalf("copied %d bytes", n)
	}
}

// TestRemoteWriter_AttributesThroughReadFrom asserts that the reason the
// Wrapper exists survives the fast path: a failure in the caller's stream is
// Not reported as the transport failing.
func TestRemoteWriter_AttributesThroughReadFrom(t *testing.T) {
	boom := errors.New("source failed")
	dst := &readerFromWriter{}

	_, err := io.Copy(remoteWriter{w: dst}, errReader{err: boom})
	if !errors.Is(err, boom) {
		t.Fatalf("the source error did not survive: %v", err)
	}
	if isRemote(err) {
		t.Fatal("a source failure was attributed to the remote end")
	}

	// And a destination failure still is remote.
	dst = &readerFromWriter{err: errors.New("write failed")}
	_, err = io.Copy(remoteWriter{w: dst}, plainReader(strings.NewReader("hello")))
	if !isRemote(err) {
		t.Fatalf("a destination failure was not attributed to the remote end: %v", err)
	}
}

type errReader struct{ err error }

func (r errReader) Read([]byte) (int, error) { return 0, r.err }

// PlainReader strips any WriteTo method from a reader.
func plainReader(r io.Reader) io.Reader { return struct{ io.Reader }{r} }
