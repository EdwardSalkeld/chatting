// Package idleconn bounds how long a connection may sit silent.
package idleconn

import (
	"net"
	"time"
)

// Conn pushes the deadline forward before every read and write, so a peer
// that stops responding fails within timeout while a slow but steady transfer
// (a large message body) is never cut short.
type Conn struct {
	net.Conn
	timeout time.Duration
}

func Wrap(conn net.Conn, timeout time.Duration) net.Conn {
	return &Conn{Conn: conn, timeout: timeout}
}

func (conn *Conn) Read(buffer []byte) (int, error) {
	if err := conn.Conn.SetReadDeadline(time.Now().Add(conn.timeout)); err != nil {
		return 0, err
	}
	return conn.Conn.Read(buffer)
}

func (conn *Conn) Write(buffer []byte) (int, error) {
	if err := conn.Conn.SetWriteDeadline(time.Now().Add(conn.timeout)); err != nil {
		return 0, err
	}
	return conn.Conn.Write(buffer)
}
