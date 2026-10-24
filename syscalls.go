package main

import (
	"net"
	"os"
	"time"
)

var sys Syscalls = OS{}

type Syscalls interface {
	dial(network, address string, timeout time.Duration) (net.Conn, error)
	accepts(ln net.Listener) net.Listener
	connection(conn net.Conn, read, write string) net.Conn
	rename(oldpath, newpath string) error
}

type OS struct{}

func (OS) rename(oldpath, newpath string) error {
	return os.Rename(oldpath, newpath)
}

func (OS) dial(network, address string, timeout time.Duration) (net.Conn, error) {
	return net.DialTimeout(network, address, timeout)
}

func (OS) accepts(ln net.Listener) net.Listener {
	return ln
}

func (OS) connection(conn net.Conn, read, write string) net.Conn {
	return conn
}
