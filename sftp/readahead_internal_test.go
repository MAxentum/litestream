package sftp

import (
	"bytes"
	"errors"
	"io"
	"testing"
)

// countingFile records every read the underlying file sees so a test can tell
// whether the consumer's reads were coalesced into larger requests.
type countingFile struct {
	r        *bytes.Reader
	reads    int
	maxRead  int
	closed   bool
	closeErr error
}

func newCountingFile(b []byte) *countingFile {
	return &countingFile{r: bytes.NewReader(b)}
}

func (f *countingFile) Read(p []byte) (int, error) {
	f.reads++
	if len(p) > f.maxRead {
		f.maxRead = len(p)
	}
	return f.r.Read(p)
}

func (f *countingFile) Close() error {
	f.closed = true
	return f.closeErr
}

// TestNewBufferedFile_CoalescesTinyReads checks that header-sized reads from
// the decoder do not each become a read on the file.
func TestNewBufferedFile_CoalescesTinyReads(t *testing.T) {
	const n = 64 << 10
	data := make([]byte, n)
	for i := range data {
		data[i] = byte(i)
	}

	f := newCountingFile(data)
	rc := newBufferedFile(f, 0)

	// Read the whole thing six bytes at a time, the LTX page header size.
	got := make([]byte, 0, n)
	buf := make([]byte, 6)
	for {
		m, err := rc.Read(buf)
		got = append(got, buf[:m]...)
		if err == io.EOF {
			break
		} else if err != nil {
			t.Fatalf("read: %v", err)
		}
	}

	if !bytes.Equal(got, data) {
		t.Fatal("read data does not match source")
	}
	// Without buffering this is n/6 reads. With a 256 KiB buffer it is one read
	// of the data plus the read that reports EOF.
	if f.reads > 3 {
		t.Fatalf("file saw %d reads for %d tiny consumer reads, expected them coalesced", f.reads, n/6)
	}
	if f.maxRead < n {
		t.Fatalf("largest read on the file was %d bytes, expected at least %d", f.maxRead, n)
	}
}

// TestNewBufferedFile_LimitBoundsReadAhead checks that read-ahead does not read
// past the requested size and that the consumer sees exactly that many bytes.
func TestNewBufferedFile_LimitBoundsReadAhead(t *testing.T) {
	data := make([]byte, 8192)
	for i := range data {
		data[i] = byte(i)
	}

	const size = 100
	f := newCountingFile(data)
	rc := newBufferedFile(f, size)

	got, err := io.ReadAll(rc)
	if err != nil {
		t.Fatalf("read all: %v", err)
	}
	if !bytes.Equal(got, data[:size]) {
		t.Fatalf("got %d bytes, want the first %d bytes of the file", len(got), size)
	}
	if f.maxRead > size {
		t.Fatalf("read %d bytes from the file, which is past the %d byte limit", f.maxRead, size)
	}
	// The source still holds the rest of the file.
	rest, err := io.ReadAll(f.r)
	if err != nil {
		t.Fatalf("read remainder: %v", err)
	}
	if len(rest) != len(data)-size {
		t.Fatalf("file advanced to %d bytes, expected exactly %d", len(data)-len(rest), size)
	}
}

// TestNewBufferedFile_OffsetIsPreserved checks that a file already positioned by
// the caller is buffered from its current offset.
func TestNewBufferedFile_OffsetIsPreserved(t *testing.T) {
	data := []byte("0123456789abcdef")
	f := newCountingFile(data)
	if _, err := f.r.Seek(10, io.SeekStart); err != nil {
		t.Fatalf("seek: %v", err)
	}

	got, err := io.ReadAll(newBufferedFile(f, 0))
	if err != nil {
		t.Fatalf("read all: %v", err)
	}
	if string(got) != "abcdef" {
		t.Fatalf("got %q, want %q", got, "abcdef")
	}
}

// TestNewBufferedFile_CloseClosesFile checks that Close reaches the file and
// that its error is returned, with and without a size limit.
func TestNewBufferedFile_CloseClosesFile(t *testing.T) {
	for _, size := range []int64{0, 4} {
		f := newCountingFile([]byte("data"))
		if err := newBufferedFile(f, size).Close(); err != nil {
			t.Fatalf("size %d: close: %v", size, err)
		}
		if !f.closed {
			t.Fatalf("size %d: file was not closed", size)
		}

		sentinel := errors.New("close failed")
		f = newCountingFile([]byte("data"))
		f.closeErr = sentinel
		if err := newBufferedFile(f, size).Close(); !errors.Is(err, sentinel) {
			t.Fatalf("size %d: close error = %v, want %v", size, err, sentinel)
		}
	}
}
