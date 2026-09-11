package sftp_test

import (
	"bytes"
	"context"
	"fmt"
	"io"
	"testing"
)

// The remoteWriter wrapper hides *sftp.File's ReadFrom from io.Copy's
// dispatch, which is the optimised (and with ConcurrentWrites, the parallel)
// upload path. This measures what hiding it costs.
func benchCopy(b *testing.B, wrap bool) {
	srv := startTestServer(b, serveFully)
	c := newClient(b, srv)
	c.ConcurrentWrites = true
	if err := c.Init(context.Background()); err != nil {
		b.Fatal(err)
	}
	cl := c.SFTPClientForTest()
	if cl == nil {
		b.Fatal("no live sftp client")
	}
	payload := bytes.Repeat([]byte("litestream-"), (8<<20)/11)

	b.SetBytes(int64(len(payload)))
	b.ResetTimer()
	for i := 0; i < b.N; i++ {
		f, err := cl.Create(fmt.Sprintf("bench-%d-%v", i, wrap))
		if err != nil {
			b.Fatal(err)
		}
		var dst io.Writer = f
		if wrap {
			// Same dispatch effect as remoteWriter: ReadFrom is not visible.
			dst = struct{ io.Writer }{f}
		}
		// The source must not implement io.WriterTo, or io.Copy never asks the
		// destination for ReadFrom and both arms measure the same path.
		src := struct{ io.Reader }{bytes.NewReader(payload)}
		if _, err := io.Copy(dst, src); err != nil {
			b.Fatal(err)
		}
		if err := f.Close(); err != nil {
			b.Fatal(err)
		}
	}
}

func BenchmarkCopy_ReadFromPath(b *testing.B)  { benchCopy(b, false) }
func BenchmarkCopy_WrappedWriter(b *testing.B) { benchCopy(b, true) }
