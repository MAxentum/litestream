package sftp_test

import (
	"crypto/ed25519"
	"crypto/rand"
	"encoding/binary"
	"encoding/pem"
	"io"
	"net"
	"os"
	"path/filepath"
	"sync"
	"testing"

	pkgsftp "github.com/pkg/sftp"
	gossh "golang.org/x/crypto/ssh"
)

// stall says how far a test server gets before it goes silent.
type stall int

const (
	stallAtBanner         stall = iota // accept TCP, never speak SSH
	stallAtSubsystem                   // finish SSH, accept the subsystem, answer no SFTP
	stallAtWrite                       // serve SFTP properly, then ignore the first WRITE
	serveFully                         // a working server
	countConcurrentWrites              // a working server that counts overlapping WRITEs
	stallAtReadDir                     // answers until the first READDIR, then goes quiet
	stallAtRead                        // serves uploads and OPENs, then ignores the first READ
)

// testServer is an SSH/SFTP server that can be made to stop answering at a
// chosen point. The point matters: each stall parks the client in a different
// place, and a fix that releases one can leave the others hanging.
type testServer struct {
	addr    string
	hostKey string

	// writeReached is closed once the server has actually received an
	// SSH_FXP_WRITE. A test that aborts before this has not tested a stalled
	// write.
	writeReached chan struct{}

	// reached is closed once the server has arrived at the phase it is
	// configured to stall in. A test waits on THIS rather than sleeping: a
	// second of not returning is not evidence of being blocked, and a signal
	// naming the phase is.
	reached   chan struct{}
	reachedAt sync.Once

	// root is the directory the server serves; the client's Path must be
	// inside it, since pkg/sftp resolves absolute paths against the real
	// filesystem.
	root string

	// counter is installed only in countConcurrentWrites mode.
	counter *countWrites
}

func startTestServer(tb testing.TB, mode stall) *testServer {
	tb.Helper()

	_, priv, err := ed25519.GenerateKey(rand.Reader)
	if err != nil {
		tb.Fatal(err)
	}
	signer, err := gossh.NewSignerFromKey(priv)
	if err != nil {
		tb.Fatal(err)
	}

	config := &gossh.ServerConfig{
		PublicKeyCallback: func(gossh.ConnMetadata, gossh.PublicKey) (*gossh.Permissions, error) {
			return &gossh.Permissions{}, nil
		},
	}
	config.AddHostKey(signer)

	ln, err := net.Listen("tcp", "127.0.0.1:0")
	if err != nil {
		tb.Fatal(err)
	}

	srv := &testServer{
		addr:         ln.Addr().String(),
		hostKey:      string(gossh.MarshalAuthorizedKey(signer.PublicKey())),
		writeReached: make(chan struct{}),
		reached:      make(chan struct{}),
		root:         tb.TempDir(),
		counter:      &countWrites{},
	}

	// Closing the listener does not close accepted sockets, and a parked
	// reader is not released by either; both are tracked and freed here.
	stop := make(chan struct{})
	var mu sync.Mutex
	var accepted []net.Conn
	closing := false // guarded by mu

	// Closing is a state, not a moment. Accept can return a socket in the
	// window between ln.Close() and the loop below draining accepted, and the
	// accepting goroutine would then register it after cleanup has finished,
	// leaving it open. The flag is read under the same mutex that guards the
	// slice, so a socket is either drained here or closed there.
	var serving sync.WaitGroup
	accepting := make(chan struct{})

	tb.Cleanup(func() {
		close(stop)
		mu.Lock()
		closing = true
		mu.Unlock()
		_ = ln.Close()
		<-accepting // the accept loop registers nothing after this
		mu.Lock()
		for _, c := range accepted {
			_ = c.Close()
		}
		mu.Unlock()
		// Join the session goroutines before this cleanup returns: t.TempDir
		// registered its own removal BEFORE this one, so LIFO runs it after —
		// but only if nothing is still serving out of that directory.
		serving.Wait()
	})

	root := srv.root

	go func() {
		defer close(accepting)
		for {
			conn, err := ln.Accept()
			if err != nil {
				return
			}
			mu.Lock()
			if closing {
				mu.Unlock()
				_ = conn.Close()
				return
			}
			accepted = append(accepted, conn)
			mu.Unlock()

			if mode == stallAtBanner {
				// Hold the socket open and say nothing at all.
				srv.reachedAt.Do(func() { close(srv.reached) })
				continue
			}
			serving.Add(1)
			go func() {
				defer serving.Done()
				serveSSH(conn, config, mode, root, stop, srv)
			}()
		}
	}()

	return srv
}

func serveSSH(conn net.Conn, config *gossh.ServerConfig, mode stall, root string, stop <-chan struct{}, srv *testServer) {
	serverConn, chans, reqs, err := gossh.NewServerConn(conn, config)
	if err != nil {
		return
	}

	// The children are the work: everything that touches the served directory
	// runs in goroutines started below, so joining only this function would let
	// t.TempDir remove a directory the server is still reading. Declared first
	// so it runs last, after the connection is closed; otherwise the wait never
	// ends.
	var children sync.WaitGroup
	defer children.Wait()
	defer serverConn.Close()

	children.Add(1)
	go func() {
		defer children.Done()
		gossh.DiscardRequests(reqs)
	}()

	for newChannel := range chans {
		if newChannel.ChannelType() != "session" {
			_ = newChannel.Reject(gossh.UnknownChannelType, "no")
			continue
		}
		ch, chReqs, err := newChannel.Accept()
		if err != nil {
			return
		}
		children.Add(1)
		go func() {
			defer children.Done()
			for req := range chReqs {
				if req.WantReply {
					_ = req.Reply(req.Type == "subsystem", nil)
				}
			}
		}()

		switch mode {
		case stallAtSubsystem:
			// Read the client's packets and answer none of them.
			srv.reachedAt.Do(func() { close(srv.reached) })
			children.Add(1)
			go func() {
				defer children.Done()
				_, _ = io.Copy(io.Discard, ch)
			}()
		default:
			children.Add(1)
			go func() {
				defer children.Done()
				defer ch.Close()
				var rw io.ReadWriter = ch
				switch mode {
				case stallAtWrite:
					rw = &stopAtWrite{r: ch, w: ch, stop: stop, seen: srv.writeReached}
				case stallAtReadDir:
					rw = &stopAtWrite{r: ch, w: ch, kind: fxpReadDir, stop: stop, seen: srv.writeReached}
				case stallAtRead:
					// A separate signal from writeReached: this mode lets
					// uploads through, so a test has to wait on the READ
					// rather than on a write that was always going to
					// succeed.
					rw = &stopAtWrite{r: ch, w: ch, kind: fxpRead, stop: stop, seen: srv.reached}
				case countConcurrentWrites:
					srv.counter.r, srv.counter.w = ch, ch
					rw = srv.counter
				}
				server, err := pkgsftp.NewServer(struct {
					io.Reader
					io.WriteCloser
				}{rw, nopWriteCloser{rw}}, pkgsftp.WithServerWorkingDirectory(root))
				if err != nil {
					return
				}
				defer server.Close()
				_ = server.Serve()
			}()
		}
	}
}

type nopWriteCloser struct{ w io.Writer }

func (n nopWriteCloser) Write(p []byte) (int, error) { return n.w.Write(p) }
func (n nopWriteCloser) Close() error                { return nil }

// stopAtWrite passes SFTP traffic through until the client sends its first
// WRITE packet, and then stops feeding the server, so the request is never
// answered and the client's write parks waiting for a reply.
//
// This is the production failure shape: the handshake succeeds, the file opens,
// and the stall happens mid-upload.
type stopAtWrite struct {
	r    io.Reader
	w    io.Writer
	kind byte            // the packet type to stop at; fxpWrite unless set
	stop <-chan struct{} // test cleanup; releases a parked Read
	seen chan struct{}   // closed when the awaited packet has arrived

	mu      sync.Mutex
	dead    bool
	pending []byte // bytes of the current packet not yet handed to the server
	once    sync.Once
}

const (
	fxpRead    = 5  // SSH_FXP_READ
	fxpWrite   = 6  // SSH_FXP_WRITE
	fxpReadDir = 12 // SSH_FXP_READDIR
)

// Read serves one SFTP packet at a time, buffering whatever does not fit in p.
//
// The server reads the four-byte length header separately from the body, so a
// wrapper that discards the remainder of a packet strands the body and stalls
// the handshake rather than the write. Leftovers are served on the next call.
func (s *stopAtWrite) Read(p []byte) (int, error) {
	s.mu.Lock()
	if len(s.pending) > 0 {
		n := copy(p, s.pending)
		s.pending = s.pending[n:]
		s.mu.Unlock()
		return n, nil
	}
	dead := s.dead
	s.mu.Unlock()

	if dead {
		<-s.stop
		return 0, io.EOF
	}

	var header [4]byte
	if _, err := io.ReadFull(s.r, header[:]); err != nil {
		return 0, err
	}
	length := binary.BigEndian.Uint32(header[:])
	body := make([]byte, length)
	if _, err := io.ReadFull(s.r, body); err != nil {
		return 0, err
	}

	kind := s.kind
	if kind == 0 {
		kind = fxpWrite
	}
	if length > 0 && body[0] == kind {
		s.once.Do(func() { close(s.seen) })
		s.mu.Lock()
		s.dead = true
		s.mu.Unlock()
		<-s.stop
		return 0, io.EOF
	}

	packet := append(header[:], body...)
	n := copy(p, packet)
	s.mu.Lock()
	s.pending = packet[n:]
	s.mu.Unlock()
	return n, nil
}

func (s *stopAtWrite) Write(p []byte) (int, error) { return s.w.Write(p) }

// countWrites passes everything through and reports the greatest number of
// WRITE requests the client had outstanding at once.
//
// Reaching File.ReadFrom does not make an upload concurrent. pkg/sftp decides
// that from what the reader says about its size, then writes either in parallel
// or in a sequential loop, and only the wire shows which happened: sequential
// means one request outstanding at a time.
type countWrites struct {
	r io.Reader
	w io.Writer

	mu      sync.Mutex
	pending []byte // request bytes not yet handed to the server
	replies []byte // reply bytes not yet framed
	out     int
	max     int
}

const fxpStatus = 101 // SSH_FXP_STATUS, the reply to a WRITE

func (c *countWrites) Read(p []byte) (int, error) {
	c.mu.Lock()
	if len(c.pending) > 0 {
		n := copy(p, c.pending)
		c.pending = c.pending[n:]
		c.mu.Unlock()
		return n, nil
	}
	c.mu.Unlock()

	var header [4]byte
	if _, err := io.ReadFull(c.r, header[:]); err != nil {
		return 0, err
	}
	length := binary.BigEndian.Uint32(header[:])
	body := make([]byte, length)
	if _, err := io.ReadFull(c.r, body); err != nil {
		return 0, err
	}
	if length > 0 && body[0] == fxpWrite {
		c.mu.Lock()
		c.out++
		if c.out > c.max {
			c.max = c.out
		}
		c.mu.Unlock()
	}

	packet := append(header[:], body...)
	n := copy(p, packet)
	c.mu.Lock()
	c.pending = packet[n:]
	c.mu.Unlock()
	return n, nil
}

// Write watches the replies, framing them the same way the request side is
// framed.
//
// Replies are a byte stream: a header can arrive without its body, and several
// packets can arrive in one call, so the reply side has to be framed the same
// way the request side is. Reading a fixed offset instead misses decrements,
// and the outstanding count then climbs on its own, reporting concurrency for
// a sequential upload.
func (c *countWrites) Write(p []byte) (int, error) {
	c.mu.Lock()
	c.replies = append(c.replies, p...)
	for {
		if len(c.replies) < 4 {
			break
		}
		length := binary.BigEndian.Uint32(c.replies[:4])
		if uint32(len(c.replies)) < 4+length {
			break
		}
		if length > 0 && c.replies[4] == fxpStatus && c.out > 0 {
			c.out--
		}
		c.replies = c.replies[4+length:]
	}
	c.mu.Unlock()
	return c.w.Write(p)
}

func (c *countWrites) maxOutstanding() int {
	c.mu.Lock()
	defer c.mu.Unlock()
	return c.max
}

func writeClientKey(tb testing.TB, dir string) string {
	tb.Helper()
	_, priv, err := ed25519.GenerateKey(rand.Reader)
	if err != nil {
		tb.Fatal(err)
	}
	block, err := gossh.MarshalPrivateKey(priv, "")
	if err != nil {
		tb.Fatal(err)
	}
	path := filepath.Join(dir, "id_ed25519")
	if err := os.WriteFile(path, pem.EncodeToMemory(block), 0600); err != nil {
		tb.Fatal(err)
	}
	return path
}
