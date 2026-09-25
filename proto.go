package main

import (
	"encoding/binary"
	"errors"
	"io"
	"log/slog"
	"net"
	"sort"
	"syscall"
	"time"
)

var errLinkDown = errors.New("link down")

const (
	opAppend      = 1
	opRead        = 2
	opStatus      = 3
	opCancel      = 4
	opReply       = 0x80
	opFail        = 0xff
	codeFull      = 1
	codeRange     = 2
	codeIo        = 3
	codeCancelled = 4
	frameHead     = 4 + 1 + 8
	idlePace      = time.Second
	keepAlive     = 10 * time.Second
	inboxDepth    = 16
)

type frame struct {
	op   byte
	body []byte
}

func readFrame(r io.Reader) (frame, error) {
	var hdr [4]byte

	if _, err := io.ReadFull(r, hdr[:]); err != nil {
		return frame{}, err
	}

	n := binary.BigEndian.Uint32(hdr[:])

	if n == 0 || n > maxFrameSize {
		return frame{}, errors.New("proto: bad frame length")
	}

	buf := make([]byte, n)

	if _, err := io.ReadFull(r, buf); err != nil {
		return frame{}, err
	}

	return frame{op: buf[0], body: buf[1:]}, nil
}

func writeFrame(w io.Writer, op byte, id uint64, body []byte) error {
	var hdr [frameHead]byte

	binary.BigEndian.PutUint32(hdr[:], uint32(1+8+len(body)))
	hdr[4] = op
	binary.BigEndian.PutUint64(hdr[5:], id)

	bufs := net.Buffers{hdr[:], body}
	_, err := bufs.WriteTo(w)

	return err
}

func splitId(body []byte) (uint64, []byte, bool) {
	if len(body) < 8 {
		return 0, nil, false
	}

	return binary.BigEndian.Uint64(body), body[8:], true
}

type reply struct {
	op     byte
	id     uint64
	body   []byte
	silent bool
}

func (c *Cell) serve(conn net.Conn) {
	out := make(chan reply, 64)
	expect := make(chan int, 1)
	pushed := 0

	go c.drain(conn, out, expect)

	for {
		f, err := readFrame(conn)

		if err != nil {
			break
		}

		id, body, ok := splitId(f.body)

		if !ok {
			slog.Warn("cell: frame without an id", "peer", conn.RemoteAddr(), "op", f.op)

			break
		}

		pushed++

		switch f.op {
		case opAppend:
			c.writes <- WriteReq{id: id, data: body, out: out}
		case opCancel:
			target, _, ok := splitId(body)

			if !ok {
				pushed--

				break
			}

			c.writes <- WriteReq{id: target, cancel: true, out: out}
		default:
			go func() {
				out <- c.answer(f.op, id, body)
			}()
		}
	}

	conn.Close()

	expect <- pushed
}

func (c *Cell) drain(conn net.Conn, out chan reply, expect chan int) {
	alive := true
	total := -1
	seen := 0

	for total < 0 || seen < total {
		select {
		case r := <-out:
			seen++

			if alive && !r.silent && writeFrame(conn, r.op, r.id, r.body) != nil {
				alive = false

				conn.Close()
			}
		case total = <-expect:
		}
	}

	conn.Close()
}

func (c *Cell) answer(op byte, id uint64, body []byte) reply {
	r := reply{op: opFail, id: id, body: []byte{codeIo}}

	try(func() {
		r.op, r.body = c.handle(op, body)
	}).catch(func(exc *Exception) {
		slog.Warn("cell: request", "op", op, "err", exc.error())
	})

	return r
}

func (c *Cell) handle(op byte, body []byte) (byte, []byte) {
	switch op {
	case opRead:
		if len(body) != 12 {
			return opFail, []byte{codeIo}
		}

		off := int64(binary.BigEndian.Uint64(body))
		n := int64(binary.BigEndian.Uint32(body[8:]))
		data, err := c.read(off, n)

		if errors.Is(err, errRange) {
			return opFail, []byte{codeRange}
		}

		throw(err)

		return op | opReply, data
	case opStatus:
		head, free := c.status()
		out := binary.BigEndian.AppendUint64(nil, uint64(head))

		return op | opReply, binary.BigEndian.AppendUint64(out, uint64(free))
	}

	throwFmt("proto: unknown op %d", op)

	return opFail, nil
}

type outcome struct {
	tag  int
	op   byte
	body []byte
}

type message struct {
	tag   int
	op    byte
	body  []byte
	reply chan outcome
}

type Link struct {
	addr    string
	inbox   chan message
	waiting map[uint64]message
	last    uint64
}

func newLink(addr string) *Link {
	l := &Link{addr: addr, inbox: make(chan message, inboxDepth), waiting: map[uint64]message{}}

	go l.run()

	return l
}

func (l *Link) run() {
	for {
		conn := l.connect()

		l.talk(conn)
	}
}

func (l *Link) accept(m message) {
	l.last++
	l.waiting[l.last] = m
}

func (l *Link) connect() net.Conn {
	down := false

	for {
		conn, err := net.Dial("tcp", l.addr)

		if err == nil {
			if tcp, ok := conn.(*net.TCPConn); ok {
				tcp.SetKeepAlive(true)
				tcp.SetKeepAlivePeriod(keepAlive)
			}

			if down {
				slog.Info("link: connected", "cell", l.addr)
			}

			return conn
		}

		if !down {
			slog.Warn("link: down", "cell", l.addr, "err", err)

			down = true
		}

		if errors.Is(err, syscall.ECONNREFUSED) {
			for id, m := range l.waiting {
				m.reply <- outcome{tag: m.tag}

				delete(l.waiting, id)
			}
		}

		select {
		case m := <-l.inbox:
			if m.op == opCancel {
				l.forget(m.reply)
			} else {
				l.accept(m)
			}
		case <-time.After(idlePace):
		}
	}
}

func (l *Link) talk(conn net.Conn) {
	frames := make(chan frame, 64)

	go readFrames(conn, frames)

	defer conn.Close()

	ids := make([]uint64, 0, len(l.waiting))

	for id := range l.waiting {
		ids = append(ids, id)
	}

	sort.Slice(ids, func(i, j int) bool { return ids[i] < ids[j] })

	for _, id := range ids {
		m := l.waiting[id]

		if writeFrame(conn, m.op, id, m.body) != nil {
			return
		}
	}

	for {
		select {
		case m := <-l.inbox:
			if m.op == opCancel {
				for _, id := range l.forget(m.reply) {
					l.last++

					if writeFrame(conn, opCancel, l.last, binary.BigEndian.AppendUint64(nil, id)) != nil {
						return
					}
				}

				continue
			}

			l.accept(m)

			if writeFrame(conn, m.op, l.last, m.body) != nil {
				return
			}
		case f := <-frames:
			if f.op == 0 {
				slog.Warn("link: connection lost", "cell", l.addr, "inflight", len(l.waiting))

				return
			}

			id, body, ok := splitId(f.body)

			if !ok {
				slog.Warn("link: reply without an id", "cell", l.addr)

				return
			}

			if m, found := l.waiting[id]; found {
				delete(l.waiting, id)

				m.reply <- outcome{tag: m.tag, op: f.op, body: body}
			}
		}
	}
}

func readFrames(conn net.Conn, frames chan frame) {
	for {
		f, err := readFrame(conn)

		if err != nil {
			frames <- frame{}

			return
		}

		frames <- f
	}
}

func (l *Link) send(tag int, op byte, body []byte, reply chan outcome) {
	l.inbox <- message{tag: tag, op: op, body: body, reply: reply}
}

func (l *Link) cancel(reply chan outcome) {
	l.inbox <- message{op: opCancel, reply: reply}
}

func (l *Link) forget(reply chan outcome) []uint64 {
	var ids []uint64

	for id, m := range l.waiting {
		if m.reply == reply {
			ids = append(ids, id)

			delete(l.waiting, id)
		}
	}

	sort.Slice(ids, func(i, j int) bool { return ids[i] < ids[j] })

	return ids
}

func failCode(body []byte) byte {
	if len(body) == 1 {
		return body[0]
	}

	return codeIo
}

func appended(o outcome) (int64, error) {
	switch {
	case o.op == 0:
		return 0, errLinkDown
	case o.op == opAppend|opReply && len(o.body) == 8:
		return int64(binary.BigEndian.Uint64(o.body)), nil
	case o.op == opFail && failCode(o.body) == codeFull:
		return 0, errFull
	}

	return 0, errors.New("proto: append failed")
}

func readOut(o outcome) ([]byte, error) {
	switch {
	case o.op == 0:
		return nil, errLinkDown
	case o.op == opRead|opReply:
		return o.body, nil
	case o.op == opFail && failCode(o.body) == codeRange:
		return nil, errRange
	}

	return nil, errors.New("proto: read failed")
}
