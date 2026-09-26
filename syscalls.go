package main

import (
	"net"
	"time"
)

var sys Syscalls = OS{}

type Syscalls interface {
	dial(network, address string, timeout time.Duration) (net.Conn, error)
	accepts(ln net.Listener) net.Listener
	connection(conn net.Conn, read, write string) net.Conn
}

type OS struct{}

func (OS) dial(network, address string, timeout time.Duration) (net.Conn, error) {
	return net.DialTimeout(network, address, timeout)
}

func (OS) accepts(ln net.Listener) net.Listener {
	return ln
}

func (OS) connection(conn net.Conn, read, write string) net.Conn {
	return conn
}
