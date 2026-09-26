//go:build s3chaos

package main

import (
	"log/slog"
	"net"
	"os"
	"strconv"
	"strings"
	"sync"
	"sync/atomic"
	"syscall"
	"time"
)

var faults = map[string]error{
	"accept":     syscall.EMFILE,
	"cell read":  syscall.ECONNRESET,
	"cell write": syscall.EPIPE,
	"dial":       syscall.EHOSTUNREACH,
	"link read":  syscall.EIO,
	"link write": syscall.EPIPE,
}

func armChaos() {
	sys = newChaos()
}

type Chaos struct {
	OS
	seed     uint64
	rates    map[string]uint64
	calls    sync.Map
	announce sync.Once
}

func newChaos() Syscalls {
	spec := os.Getenv("S3_CHAOS")

	if spec == "" {
		return OS{}
	}

	c := &Chaos{seed: 1, rates: map[string]uint64{}}

	if seed := os.Getenv("S3_CHAOS_SEED"); seed != "" {
		c.seed = throw2(strconv.ParseUint(seed, 10, 64))
	}

	for _, item := range strings.Split(spec, ",") {
		if stripped, found := strings.CutPrefix(item, "-"); found {
			delete(c.rates, stripped)

			continue
		}

		name, rate := item, uint64(100)

		if at := strings.LastIndex(item, ":"); at >= 0 {
			name, rate = item[:at], throw2(strconv.ParseUint(item[at+1:], 10, 64))
		}

		if rate == 0 {
			throwFmt("chaos point %q needs a rate above zero", name)
		}

		if name == "all" {
			for point := range faults {
				c.rates[point] = rate
			}

			continue
		}

		if _, known := faults[name]; !known {
			throwFmt("unknown chaos point %q", name)
		}

		c.rates[name] = rate
	}

	return c
}

func (c *Chaos) due(what string) uint64 {
	rate := c.rates[what]

	if rate == 0 {
		return 0
	}

	counter, _ := c.calls.LoadOrStore(what, &atomic.Uint64{})
	call := counter.(*atomic.Uint64).Add(1)

	c.announce.Do(func() { slog.Warn("chaos armed", "seed", c.seed, "points", len(c.rates)) })

	if (call+mix(c.seed, what))%rate != 0 {
		return 0
	}

	return call
}

func (c *Chaos) failing(what string) error {
	call := c.due(what)

	if call == 0 {
		return nil
	}

	slog.Warn("chaos", "at", what, "call", call, "err", faults[what])

	return faults[what]
}

func mix(seed uint64, what string) uint64 {
	hash := seed ^ 14695981039346656037

	for _, b := range []byte(what) {
		hash = (hash ^ uint64(b)) * 1099511628211
	}

	return hash >> 7
}

func (c *Chaos) dial(network, address string, timeout time.Duration) (net.Conn, error) {
	if err := c.failing("dial"); err != nil {
		return nil, err
	}

	return c.OS.dial(network, address, timeout)
}

func (c *Chaos) accepts(ln net.Listener) net.Listener {
	return ChaosListener{Listener: ln, chaos: c}
}

func (c *Chaos) connection(conn net.Conn, read, write string) net.Conn {
	return ChaosConn{Conn: conn, chaos: c, reads: read, writes: write}
}

type ChaosListener struct {
	net.Listener
	chaos *Chaos
}

func (l ChaosListener) Accept() (net.Conn, error) {
	return l.accept()
}

func (l ChaosListener) accept() (net.Conn, error) {
	if err := l.chaos.failing("accept"); err != nil {
		return nil, err
	}

	return l.Listener.Accept()
}

type ChaosConn struct {
	net.Conn
	chaos  *Chaos
	reads  string
	writes string
}

func (c ChaosConn) Read(b []byte) (int, error) {
	return c.read(b)
}

func (c ChaosConn) read(b []byte) (int, error) {
	if err := c.chaos.failing(c.reads); err != nil {
		return 0, err
	}

	return c.Conn.Read(b)
}

func (c ChaosConn) Write(b []byte) (int, error) {
	return c.write(b)
}

func (c ChaosConn) write(b []byte) (int, error) {
	if err := c.chaos.failing(c.writes); err != nil {
		return 0, err
	}

	return c.Conn.Write(b)
}
