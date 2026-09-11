package sftp

import (
	"bytes"
	"context"
	"errors"
	"fmt"
	"io"
	"log/slog"
	"net"
	"net/url"
	"os"
	"path"
	"strings"
	"sync"
	"syscall"
	"time"

	"github.com/pkg/sftp"
	"github.com/superfly/ltx"
	"golang.org/x/crypto/ssh"

	"github.com/benbjohnson/litestream"
	"github.com/benbjohnson/litestream/internal"
)

func init() {
	litestream.RegisterReplicaClientFactory("sftp", NewReplicaClientFromURL)
}

// ReplicaClientType is the client type for this package.
const ReplicaClientType = "sftp"

// Default settings for replica client.
const (
	DefaultDialTimeout = 30 * time.Second
)

var _ litestream.ReplicaClient = (*ReplicaClient)(nil)

// ReplicaClient is a client for writing LTX files over SFTP.
type ReplicaClient struct {
	// mu serialises connection setup. Abort must never take it: setup holds it
	// across the dial and both handshakes, which is when an abort is needed.
	mu sync.Mutex

	// connMu guards the fields below and is never held across a network call.
	// The abort flag and the published connection share it so that checking
	// for an abort and publishing a handle is one atomic step; otherwise a
	// connection established just after an abort is never closed.
	connMu  sync.Mutex
	aborted bool
	cur     *conn
	lastGen uint64

	// dial establishes the TCP connection. Replaceable in tests.
	dial func(ctx context.Context, network, addr string) (net.Conn, error)

	logger *slog.Logger

	// SFTP connection info
	Host        string
	User        string
	Password    string
	Path        string
	KeyPath     string
	HostKey     string
	DialTimeout time.Duration

	// ConcurrentWrites enables concurrent writes for better performance.
	// Note: This makes resuming failed transfers unsafe.
	ConcurrentWrites bool
}

// NewReplicaClient returns a new instance of ReplicaClient.
func NewReplicaClient() *ReplicaClient {
	return &ReplicaClient{
		logger:           slog.Default().WithGroup(ReplicaClientType),
		DialTimeout:      DefaultDialTimeout,
		ConcurrentWrites: true, // Default to true for better performance
	}
}

func (c *ReplicaClient) SetLogger(logger *slog.Logger) {
	c.logger = logger.WithGroup(ReplicaClientType)
}

// NewReplicaClientFromURL creates a new ReplicaClient from URL components.
// This is used by the replica client factory registration.
// URL format: sftp://[user[:password]@]host[:port]/path
func NewReplicaClientFromURL(scheme, host, urlPath string, query url.Values, userinfo *url.Userinfo) (litestream.ReplicaClient, error) {
	client := NewReplicaClient()

	// Extract credentials from userinfo
	if userinfo != nil {
		client.User = userinfo.Username()
		client.Password, _ = userinfo.Password()
	}

	client.Host = host
	client.Path = urlPath

	if client.Host == "" {
		return nil, fmt.Errorf("host required for sftp replica URL")
	}
	if client.User == "" {
		return nil, fmt.Errorf("user required for sftp replica URL")
	}

	return client, nil
}

// conn is one transport instance, from the TCP connection up. It carries a
// generation so that an error arriving late, from a connection that has since
// been replaced, tears down that connection and not its successor.
type conn struct {
	gen uint64

	// cancel aborts a dial that has not returned. Registered before dialling:
	// until DialContext returns there is no socket for Abort to close.
	cancel context.CancelFunc

	raw  net.Conn
	ssh  *ssh.Client
	sftp *sftp.Client
}

// close tears the instance down, raw connection first. Closing an SFTP client
// waits on its own receive goroutine, which is what a stalled peer leaves
// parked; dropping the socket underneath makes every layer above fail at once.
func (c *conn) close() {
	if c == nil {
		return
	}
	// Cancel first: a dial still in progress has no socket to close.
	if c.cancel != nil {
		c.cancel()
	}
	if c.raw != nil {
		_ = c.raw.Close()
	}
	if c.ssh != nil {
		_ = c.ssh.Close()
	}
	if c.sftp != nil {
		_ = c.sftp.Close()
	}
}

// begin registers a new connection instance, or reports that the client has
// been aborted. The caller must close what it holds if this returns an error.
func (c *ReplicaClient) begin() (*conn, error) {
	c.connMu.Lock()
	defer c.connMu.Unlock()
	if c.aborted {
		return nil, litestream.ErrClientAborted
	}
	c.lastGen++
	c.cur = &conn{gen: c.lastGen}
	return c.cur, nil
}

// publish stores a newly built layer on the connection instance, under the same
// lock Abort takes. It fails if the client was aborted or the instance is no
// longer current; the caller then closes what it built, since nobody else
// knows the connection exists.
func (c *ReplicaClient) publish(instance *conn, apply func(*conn)) error {
	c.connMu.Lock()
	defer c.connMu.Unlock()
	if c.aborted {
		return litestream.ErrClientAborted
	}
	if c.cur == nil || c.cur.gen != instance.gen {
		return litestream.ErrClientAborted
	}
	apply(c.cur)
	return nil
}

// live returns the usable SFTP client and its generation, if there is one.
func (c *ReplicaClient) live() (*sftp.Client, uint64) {
	c.connMu.Lock()
	defer c.connMu.Unlock()
	if c.cur == nil || c.cur.sftp == nil {
		return nil, 0
	}
	return c.cur.sftp, c.cur.gen
}

// drop tears down the connection instance of the given generation, if it is
// still current. Without the generation check, a slow operation reporting an
// error from a replaced transport would close its replacement.
func (c *ReplicaClient) drop(gen uint64) {
	c.connMu.Lock()
	var doomed *conn
	if c.cur != nil && c.cur.gen == gen {
		doomed, c.cur = c.cur, nil
	}
	c.connMu.Unlock()
	doomed.close()
}

// Abort closes the transport so that operations blocked inside it fail, and
// marks the client so nothing reconnects behind the shutdown.
//
// It implements litestream.ReplicaClientAborter.
func (c *ReplicaClient) Abort() {
	c.connMu.Lock()
	c.aborted = true
	doomed := c.cur
	c.cur = nil
	c.connMu.Unlock()
	doomed.close()
}

// Aborted reports whether Abort has been called.
func (c *ReplicaClient) Aborted() bool {
	c.connMu.Lock()
	defer c.connMu.Unlock()
	return c.aborted
}

// Type returns "sftp" as the client type.
func (c *ReplicaClient) Type() string {
	return ReplicaClientType
}

// Init initializes the connection to SFTP. No-op if already initialized.
func (c *ReplicaClient) Init(ctx context.Context) error {
	_, _, err := c.init(ctx)
	return err
}

// init initializes the connection and returns the SFTP client with the
// generation it belongs to.
func (c *ReplicaClient) init(ctx context.Context) (_ *sftp.Client, _ uint64, err error) {
	c.mu.Lock()
	defer c.mu.Unlock()

	if client, gen := c.live(); client != nil {
		return client, gen, nil
	}

	// Registering the instance is also the abort check: after Abort this
	// fails, so a shutdown is never followed by a fresh connection that the
	// next operation can park in.
	instance, err := c.begin()
	if err != nil {
		return nil, 0, err
	}
	// One cleanup for every failure below, including the ones that never
	// reach the network (no user, bad host key, unreadable key file): leaving a
	// registered instance with no handles behind would make `cur` describe a
	// connection that does not exist.
	defer func() {
		if err != nil {
			c.drop(instance.gen)
		}
	}()

	if c.User == "" {
		return nil, 0, fmt.Errorf("sftp user required")
	}

	// Build SSH configuration & auth methods
	var hostkey ssh.HostKeyCallback
	if c.HostKey != "" {
		var pubkey, _, _, _, err = ssh.ParseAuthorizedKey([]byte(c.HostKey))
		if err != nil {
			return nil, 0, fmt.Errorf("cannot parse sftp host key: %w", err)
		}
		hostkey = ssh.FixedHostKey(pubkey)
	} else {
		slog.Warn("sftp host key not verified", "host", c.Host)
		hostkey = ssh.InsecureIgnoreHostKey()
	}
	config := &ssh.ClientConfig{
		User:            c.User,
		HostKeyCallback: hostkey,
		BannerCallback:  ssh.BannerDisplayStderr(),
	}
	if c.Password != "" {
		config.Auth = append(config.Auth, ssh.Password(c.Password))
	}

	if c.KeyPath != "" {
		buf, err := os.ReadFile(c.KeyPath)
		if err != nil {
			return nil, 0, fmt.Errorf("sftp: cannot read sftp key path: %w", err)
		}

		signer, err := ssh.ParsePrivateKey(buf)
		if err != nil {
			return nil, 0, fmt.Errorf("sftp: cannot parse sftp key path: %w", err)
		}
		config.Auth = append(config.Auth, ssh.PublicKeys(signer))
	}

	// Append standard port, if necessary.
	host := c.Host
	if _, _, err := net.SplitHostPort(c.Host); err != nil {
		host = net.JoinHostPort(c.Host, "22")
	}

	// The raw connection is dialled and published before any handshake.
	//
	// ssh.Dial performs the TCP connect, banner exchange, key exchange and
	// authentication in one call and returns nothing until all of it finishes.
	// A server that accepts TCP and then goes quiet leaves that call parked
	// with no handle to close, so Abort has nothing to act on. Dialling here
	// means the socket exists, and is abortable, before any handshake byte is
	// sent.
	//
	// Register the cancel before dialling. Between here and DialContext
	// returning there is no socket, so cancellation is the only way an abort
	// can reach this connection.
	dialCtx, dialCancel := context.WithCancel(ctx)
	if err := c.publish(instance, func(cn *conn) { cn.cancel = dialCancel }); err != nil {
		dialCancel()
		return nil, 0, err
	}

	dial := c.dial
	if dial == nil {
		dialer := net.Dialer{Timeout: c.DialTimeout}
		dial = dialer.DialContext
	}
	raw, err := dial(dialCtx, "tcp", host)
	if err != nil {
		// An abort during the dial arrives as context.Canceled, which is not a
		// transport error. Attribute it here, at the boundary that owns the
		// cancel, rather than widening the classification elsewhere.
		if c.Aborted() && errors.Is(err, context.Canceled) {
			return nil, 0, fmt.Errorf("%w (%w)", litestream.ErrClientAborted, err)
		}
		return nil, 0, c.abortedOr(remote(err))
	}
	if err := c.publish(instance, func(cn *conn) { cn.raw = raw }); err != nil {
		_ = raw.Close()
		return nil, 0, err
	}

	// One deadline across Both handshakes. An unanswered key exchange and an
	// unanswered SFTP INIT are the same outage as an unanswered Syn; it is
	// cleared only once the transport is fully established, because a deadline
	// left on a long-lived connection would break replication later.
	if c.DialTimeout > 0 {
		_ = raw.SetDeadline(time.Now().Add(c.DialTimeout))
	}

	sshConn, chans, reqs, err := ssh.NewClientConn(raw, host, config)
	if err != nil {
		return nil, 0, c.abortedOr(remote(err))
	}
	sshClient := ssh.NewClient(sshConn, chans, reqs)
	if err := c.publish(instance, func(cn *conn) { cn.ssh = sshClient }); err != nil {
		_ = sshClient.Close()
		_ = raw.Close()
		return nil, 0, err
	}

	// Wrap connection with an SFTP client.
	// Configure options based on client settings
	opts := []sftp.ClientOption{}
	if c.ConcurrentWrites {
		opts = append(opts, sftp.UseConcurrentWrites(true))
	}

	sftpClient, err := sftp.NewClient(sshClient, opts...)
	if err != nil {
		return nil, 0, c.abortedOr(remote(err))
	}
	if err := c.publish(instance, func(cn *conn) { cn.sftp = sftpClient }); err != nil {
		_ = sftpClient.Close()
		_ = sshClient.Close()
		_ = raw.Close()
		return nil, 0, err
	}

	// Setup is complete: the transport is long-lived from here.
	_ = raw.SetDeadline(time.Time{})

	return sftpClient, instance.gen, nil
}

// remoteError marks an error as having come from the remote end — the dial, a
// handshake, or a call on the SSH or SFTP client — rather than from the
// caller's input reader or from LTX validation.
//
// Only remote errors may drop the connection or be reported as an abort.
// Without the distinction, a truncated LTX header reads as io.ErrUnexpectedEOF
// and would tear down a healthy connection that other operations are using.
type remoteError struct{ err error }

func (e remoteError) Error() string { return e.err.Error() }
func (e remoteError) Unwrap() error { return e.err }

// remote tags an error as coming from the remote end.
func remote(err error) error {
	if err == nil {
		return nil
	}
	return remoteError{err: err}
}

func isRemote(err error) bool {
	var r remoteError
	return errors.As(err, &r)
}

// remoteWriter tags write failures as remote, so that io.Copy's two error
// sources stay distinguishable: a failure writing to the replica is the
// transport, a failure reading the source is the caller's.
type remoteWriter struct{ w io.Writer }

func (w remoteWriter) Write(p []byte) (int, error) {
	n, err := w.w.Write(p)
	return n, remote(err)
}

// ReadFrom keeps io.Copy's optimised path reachable. Wrapping a destination
// hides its ReadFrom, and *sftp.File's ReadFrom is pkg/sftp's optimised
// upload, so a plain wrapper demotes uploads to a sequential loop.
//
// Attribution still has to survive: ReadFrom both reads the caller's stream
// and writes to the remote, so the single error it returns has two possible
// origins. The source is wrapped to record its own failure, and only an error
// that is not the source's is tagged remote.
func (w remoteWriter) ReadFrom(r io.Reader) (int64, error) {
	rf, ok := w.w.(io.ReaderFrom)
	if !ok {
		return io.Copy(struct{ io.Writer }{w}, r)
	}
	lr := &localReader{r: r}

	// pkg/sftp decides whether to write concurrently by asking the reader how
	// much is left (Len, Size, *io.LimitedReader or Stat; see File.ReadFrom).
	// A wrapper exposing none of those answers "unknown" and silently takes
	// the sequential path, so the size is carried across.
	var src io.Reader = lr
	if size, ok := readerSize(r); ok {
		src = sizedReader{localReader: lr, size: size}
	}

	n, err := rf.ReadFrom(src)
	if err != nil && lr.err != nil && errors.Is(err, lr.err) {
		return n, err // the caller's stream failed; not the transport's fault
	}
	return n, remote(err)
}

// readerSize reports what pkg/sftp would have learned from the unwrapped
// reader. The cases and their order mirror File.ReadFrom exactly; a negative
// result is meaningful there (it selects maximum concurrency) and is passed
// through rather than discarded.
func readerSize(r io.Reader) (int64, bool) {
	switch r := r.(type) {
	case interface{ Len() int }:
		return int64(r.Len()), true
	case interface{ Size() int64 }:
		return r.Size(), true
	case *io.LimitedReader:
		return r.N, true
	case interface{ Stat() (os.FileInfo, error) }:
		info, err := r.Stat()
		if err != nil {
			return 0, false
		}
		return info.Size(), true
	}
	return 0, false
}

// localReader remembers the error its source returned, so a failure that came
// out of the caller's file is not reported as the replica going away.
type localReader struct {
	r   io.Reader
	err error
}

func (l *localReader) Read(p []byte) (int, error) {
	n, err := l.r.Read(p)
	if err != nil && err != io.EOF {
		l.err = err
	}
	return n, err
}

// sizedReader is localReader with the size the original reader advertised.
// Size is the second case pkg/sftp probes and the one every other case can be
// expressed as, so one shape carries all four.
type sizedReader struct {
	*localReader
	size int64
}

func (s sizedReader) Size() int64 { return s.size }

// abortedOr reports err as an abort when the client has been aborted and the
// failure is the remote transport going away underneath it.
//
// Narrow deliberately: a permission error, a malformed LTX file or a local disk
// failure that coincides with a stop keeps its own identity and its own log
// level.
func (c *ReplicaClient) abortedOr(err error) error {
	if err == nil || !isRemote(err) || !c.Aborted() || !isTransportError(err) {
		return err
	}
	return fmt.Errorf("%w (%w)", litestream.ErrClientAborted, err)
}

// isTransportError reports whether err is the connection going away rather than
// the server refusing something. It is only ever consulted for remote errors.
func isTransportError(err error) bool {
	switch {
	case errors.Is(err, sftp.ErrSSHFxConnectionLost),
		errors.Is(err, net.ErrClosed),
		errors.Is(err, io.EOF),
		errors.Is(err, io.ErrUnexpectedEOF),
		errors.Is(err, syscall.ECONNRESET),
		errors.Is(err, syscall.EPIPE):
		return true
	}
	// x/crypto/ssh and pkg/sftp report some teardowns as bare strings.
	msg := err.Error()
	return strings.Contains(msg, "use of closed network connection") ||
		strings.Contains(msg, "connection lost")
}

// DeleteAll deletes all LTX files.
func (c *ReplicaClient) DeleteAll(ctx context.Context) (err error) {
	var gen uint64
	defer func() { err = c.afterOp(gen, err) }()

	sftpClient, gen, err := c.init(ctx)
	if err != nil {
		return err
	}

	var dirs []string
	walker := sftpClient.Walk(c.Path)
	for walker.Step() {
		if err := walker.Err(); os.IsNotExist(err) {
			continue
		} else if err != nil {
			// A walker error is a remote error: Walk issues ReadDir and Stat
			// over the wire, so a connection dying mid-walk surfaces here.
			// Untagged it is invisible to afterOp and to abortedOr.
			return remote(fmt.Errorf("sftp: cannot walk path %q: %w", walker.Path(), err))
		}
		if walker.Stat().IsDir() {
			dirs = append(dirs, walker.Path())
			continue
		}

		if err := sftpClient.Remove(walker.Path()); err != nil && !os.IsNotExist(err) {
			return remote(fmt.Errorf("sftp: cannot delete file %q: %w", walker.Path(), err))
		}

		internal.OperationTotalCounterVec.WithLabelValues(ReplicaClientType, "DELETE").Inc()
	}

	// Remove directories in reverse order after they have been emptied.
	for i := len(dirs) - 1; i >= 0; i-- {
		filename := dirs[i]
		if err := sftpClient.RemoveDirectory(filename); err != nil && !os.IsNotExist(err) {
			return remote(fmt.Errorf("sftp: cannot delete directory %q: %w", filename, err))
		}
	}

	// log.Printf("%s(%s): retainer: deleting all", r.db.Path(), r.Name())

	return nil
}

// LTXFiles returns an iterator over all available LTX files for a level.
// SFTP uses file ModTime for timestamps, which is set via Chtimes() to preserve original timestamp.
// The useMetadata parameter is ignored since ModTime always contains the accurate timestamp.
func (c *ReplicaClient) LTXFiles(ctx context.Context, level int, seek ltx.TXID, _ bool) (_ ltx.FileIterator, err error) {
	var gen uint64
	defer func() { err = c.afterOp(gen, err) }()

	sftpClient, gen, err := c.init(ctx)
	if err != nil {
		return nil, err
	}

	dir := litestream.LTXLevelDir(c.Path, level)
	fis, err := sftpClient.ReadDir(dir)
	if os.IsNotExist(err) {
		return ltx.NewFileInfoSliceIterator(nil), nil
	} else if err != nil {
		return nil, remote(err)
	}

	// Iterate over every file and convert to metadata.
	infos := make([]*ltx.FileInfo, 0, len(fis))
	for _, fi := range fis {
		minTXID, maxTXID, err := ltx.ParseFilename(path.Base(fi.Name()))
		if err != nil {
			continue
		} else if minTXID < seek {
			continue
		}

		infos = append(infos, &ltx.FileInfo{
			Level:     level,
			MinTXID:   minTXID,
			MaxTXID:   maxTXID,
			Size:      fi.Size(),
			CreatedAt: fi.ModTime().UTC(), // ModTime contains accurate timestamp from Chtimes()
		})
	}

	return ltx.NewFileInfoSliceIterator(infos), nil
}

// WriteLTXFile writes a LTX file from rd into a remote file.
func (c *ReplicaClient) WriteLTXFile(ctx context.Context, level int, minTXID, maxTXID ltx.TXID, rd io.Reader) (info *ltx.FileInfo, err error) {
	var gen uint64
	defer func() { err = c.afterOp(gen, err) }()

	sftpClient, gen, err := c.init(ctx)
	if err != nil {
		return nil, err
	}

	filename := litestream.LTXFilePath(c.Path, level, minTXID, maxTXID)

	// Use TeeReader to peek at LTX header while preserving data for upload
	var buf bytes.Buffer
	teeReader := io.TeeReader(rd, &buf)

	// Extract timestamp from LTX header
	hdr, _, err := ltx.PeekHeader(teeReader)
	if err != nil {
		return nil, fmt.Errorf("extract timestamp from LTX header: %w", err)
	}
	timestamp := time.UnixMilli(hdr.Timestamp).UTC()

	// Combine buffered data with rest of reader
	fullReader := io.MultiReader(&buf, rd)

	if err := sftpClient.MkdirAll(path.Dir(filename)); err != nil {
		return nil, remote(fmt.Errorf("sftp: cannot make parent snapshot directory %q: %w", path.Dir(filename), err))
	}

	tmpFilename := fmt.Sprintf("%s.%d.%d.tmp", filename, os.Getpid(), time.Now().UnixNano())
	f, err := sftpClient.OpenFile(tmpFilename, os.O_WRONLY|os.O_CREATE|os.O_TRUNC)
	if err != nil {
		return nil, remote(fmt.Errorf("sftp: cannot open temporary snapshot file for writing: %w", err))
	}
	defer func() {
		if f != nil {
			_ = f.Close()
		}
		if err != nil {
			_ = sftpClient.Remove(tmpFilename)
		}
	}()

	// The destination is tagged so that a failure writing to the replica is
	// distinguishable from a failure reading the caller's LTX stream.
	n, err := io.Copy(remoteWriter{w: f}, fullReader)
	if err != nil {
		return nil, err
	} else if err := f.Close(); err != nil {
		return nil, remote(err)
	}
	f = nil

	if err := sftpClient.Chtimes(tmpFilename, timestamp, timestamp); err != nil {
		return nil, remote(fmt.Errorf("sftp: cannot set file timestamps: %w", err))
	}

	if err := sftpClient.Rename(tmpFilename, filename); err != nil {
		return nil, remote(fmt.Errorf("sftp: cannot rename temporary ltx file %q to %q: %w", tmpFilename, filename, err))
	}

	internal.OperationTotalCounterVec.WithLabelValues(ReplicaClientType, "PUT").Inc()
	internal.OperationBytesCounterVec.WithLabelValues(ReplicaClientType, "PUT").Add(float64(n))

	return &ltx.FileInfo{
		Level:     level,
		MinTXID:   minTXID,
		MaxTXID:   maxTXID,
		Size:      n,
		CreatedAt: timestamp,
	}, nil
}

// OpenLTXFile returns a reader for an LTX file.
// Returns os.ErrNotExist if no matching position is found.
func (c *ReplicaClient) OpenLTXFile(ctx context.Context, level int, minTXID, maxTXID ltx.TXID, offset, size int64) (_ io.ReadCloser, err error) {
	var gen uint64
	defer func() { err = c.afterOp(gen, err) }()

	sftpClient, gen, err := c.init(ctx)
	if err != nil {
		return nil, err
	}

	filename := litestream.LTXFilePath(c.Path, level, minTXID, maxTXID)
	f, err := sftpClient.OpenFile(filename, os.O_RDONLY)
	if err != nil {
		return nil, remote(err)
	}

	if offset > 0 {
		if _, err := f.Seek(offset, io.SeekStart); err != nil {
			return nil, remote(err)
		}
	}

	internal.OperationTotalCounterVec.WithLabelValues(ReplicaClientType, "GET").Inc()

	if size > 0 {
		return internal.LimitReadCloser(f, size), nil
	}
	return f, nil
}

// DeleteLTXFiles deletes LTX files with at the given positions.
func (c *ReplicaClient) DeleteLTXFiles(ctx context.Context, a []*ltx.FileInfo) (err error) {
	var gen uint64
	defer func() { err = c.afterOp(gen, err) }()

	sftpClient, gen, err := c.init(ctx)
	if err != nil {
		return err
	}

	for _, info := range a {
		filename := litestream.LTXFilePath(c.Path, info.Level, info.MinTXID, info.MaxTXID)

		c.logger.Debug("deleting ltx file", "level", info.Level, "minTXID", info.MinTXID, "maxTXID", info.MaxTXID, "path", filename)

		if err := sftpClient.Remove(filename); err != nil && !os.IsNotExist(err) {
			return remote(fmt.Errorf("sftp: cannot delete ltx file %q: %w", filename, err))
		}
		internal.OperationTotalCounterVec.WithLabelValues(ReplicaClientType, "DELETE").Inc()
	}

	return nil
}

// Cleanup deletes path & directories after empty.
func (c *ReplicaClient) Cleanup(ctx context.Context) (err error) {
	var gen uint64
	defer func() { err = c.afterOp(gen, err) }()

	sftpClient, gen, err := c.init(ctx)
	if err != nil {
		return err
	}

	if err := sftpClient.RemoveDirectory(c.Path); err != nil && !os.IsNotExist(err) {
		return remote(fmt.Errorf("sftp: cannot delete path: %w", err))
	}
	return nil
}

// afterOp runs on the way out of every operation. It drops the connection the
// operation used — identified by generation, so a late failure cannot tear down
// a connection that has since replaced it — and classifies the error.
//
// Only remote transport failures drop the connection: a refusal from the server
// and a failure reading the caller's input both keep it.
func (c *ReplicaClient) afterOp(gen uint64, err error) error {
	if err == nil {
		return nil
	}
	if gen != 0 && isRemote(err) && isTransportError(err) {
		c.drop(gen)
	}
	return c.abortedOr(err)
}
