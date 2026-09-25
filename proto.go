package main

import (
	"encoding/binary"
	"errors"
	"io"
	"log/slog"
	"net"
	"sync"
	"time"
)

const (
	opAppend    = 1
	opRead      = 2
	opStatus    = 3
	opReply     = 0x80
	opFail      = 0xff
	codeFull    = 1
	codeRange   = 2
	codeIo      = 3
	frameHead   = 4 + 1 + 8
	callTimeout = 60 * time.Second
	dialTimeout = 5 * time.Second
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
	op   byte
	id   uint64
	body []byte
}

func (c *Cell) serve(conn net.Conn) {
	out := make(chan reply, 64)

	var inflight sync.WaitGroup

	go func() {
		alive := true

		for r := range out {
			if alive && writeFrame(conn, r.op, r.id, r.body) != nil {
				alive = false

				conn.Close()
			}
		}

		conn.Close()
	}()

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

		inflight.Add(1)

		go func() {
			defer inflight.Done()

			out <- c.answer(f.op, id, body)
		}()
	}

	conn.Close()
	inflight.Wait()
	close(out)
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
	case opAppend:
		offset, err := c.append(body)

		if errors.Is(err, errFull) {
			return opFail, []byte{codeFull}
		}

		throw(err)

		return op | opReply, binary.BigEndian.AppendUint64(nil, uint64(offset))
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

type request struct {
	op    byte
	body  []byte
	reply chan frame
}

type CellClient struct {
	addr string
	send chan request
}

func newCellClient(addr string) *CellClient {
	cl := &CellClient{addr: addr, send: make(chan request)}

	go cl.run()

	return cl
}

func (cl *CellClient) run() {
	var conn net.Conn
	var frames chan frame
	var id uint64

	waiting := map[uint64]chan frame{}

	lost := func(why string) {
		conn.Close()
		conn, frames = nil, nil

		for i, ch := range waiting {
			ch <- frame{body: []byte(why)}

			delete(waiting, i)
		}
	}

	for {
		select {
		case req := <-cl.send:
			if conn == nil {
				c, err := net.DialTimeout("tcp", cl.addr, dialTimeout)

				if err != nil {
					req.reply <- frame{body: []byte(err.Error())}

					continue
				}

				conn, frames = c, make(chan frame, 64)

				go readFrames(conn, frames)
			}

			id++
			waiting[id] = req.reply

			if err := writeFrame(conn, req.op, id, req.body); err != nil {
				conn.Close()
			}
		case f := <-frames:
			if f.op == 0 {
				lost("connection lost")

				continue
			}

			rid, body, ok := splitId(f.body)

			if !ok {
				lost("reply without an id")

				continue
			}

			if ch, found := waiting[rid]; found {
				delete(waiting, rid)

				ch <- frame{op: f.op, body: body}
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

func (cl *CellClient) call(op byte, body []byte) (byte, []byte) {
	req := request{op: op, body: body, reply: make(chan frame, 1)}

	cl.send <- req

	select {
	case f := <-req.reply:
		if f.op == 0 {
			throwFmt("cell %s: %s", cl.addr, f.body)
		}

		return f.op, f.body
	case <-time.After(callTimeout):
		throwFmt("cell %s: no reply to op %d in %s", cl.addr, op, callTimeout)
	}

	return 0, nil
}

func failCode(body []byte) byte {
	if len(body) == 1 {
		return body[0]
	}

	return codeIo
}

func (cl *CellClient) append(data []byte) (int64, error) {
	op, resp := cl.call(opAppend, data)

	switch {
	case op == opAppend|opReply && len(resp) == 8:
		return int64(binary.BigEndian.Uint64(resp)), nil
	case op == opFail && failCode(resp) == codeFull:
		return 0, errFull
	}

	throwFmt("cell %s: append answered op %d", cl.addr, op)

	return 0, nil
}

func (cl *CellClient) read(off int64, n int64) ([]byte, error) {
	req := binary.BigEndian.AppendUint64(nil, uint64(off))
	op, resp := cl.call(opRead, binary.BigEndian.AppendUint32(req, uint32(n)))

	switch {
	case op == opRead|opReply:
		return resp, nil
	case op == opFail && failCode(resp) == codeRange:
		return nil, errRange
	}

	throwFmt("cell %s: read answered op %d", cl.addr, op)

	return nil, nil
}

func (cl *CellClient) status() (int64, int64) {
	op, resp := cl.call(opStatus, nil)

	if op != opStatus|opReply || len(resp) != 16 {
		throwFmt("cell %s: status answered op %d", cl.addr, op)
	}

	return int64(binary.BigEndian.Uint64(resp)), int64(binary.BigEndian.Uint64(resp[8:]))
}
