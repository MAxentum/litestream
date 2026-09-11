package sftp

import (
	"context"
	"net"

	"github.com/pkg/sftp"
)

// Generation reports the generation of the live connection, or zero.
//
// Test-only: connection lifecycle is the part of this client that is easy to
// get wrong and impossible to observe from outside.
func (c *ReplicaClient) Generation() uint64 {
	c.connMu.Lock()
	defer c.connMu.Unlock()
	if c.cur == nil {
		return 0
	}
	return c.cur.gen
}

// DropGeneration simulates an operation reporting a connection failure from the
// given generation.
func (c *ReplicaClient) DropGeneration(gen uint64) { c.drop(gen) }

// SetDial replaces the TCP dialer. Test-only: a dial that never returns cannot
// be produced reliably against a real network.
func (c *ReplicaClient) SetDial(fn func(ctx context.Context, network, addr string) (net.Conn, error)) {
	c.dial = fn
}

// SFTPClientForTest exposes the live pkg/sftp client, so a benchmark can
// measure the copy path without going through WriteLTXFile's LTX validation.
func (c *ReplicaClient) SFTPClientForTest() *sftp.Client {
	c.connMu.Lock()
	defer c.connMu.Unlock()
	if c.cur == nil {
		return nil
	}
	return c.cur.sftp
}
