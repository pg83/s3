package main

import (
	"encoding/binary"
	"errors"
	"io"
	"net"
)

func readFrame(r io.Reader) (byte, []byte) {
	var hdr [5]byte

	throw2(io.ReadFull(r, hdr[:]))

	n := binary.BigEndian.Uint32(hdr[1:])

	if n > maxFrameSize {
		throwFmt("proto: frame of %d bytes", n)
	}

	payload := make([]byte, n)

	throw2(io.ReadFull(r, payload))

	return hdr[0], payload
}

func writeFrame(w io.Writer, kind byte, payload []byte) {
	var hdr [5]byte

	hdr[0] = kind
	binary.BigEndian.PutUint32(hdr[1:], uint32(len(payload)))

	throw2(w.Write(hdr[:]))
	throw2(w.Write(payload))
}

func (c *Cell) serve(conn net.Conn) {
	defer conn.Close()

	for {
		op, payload := readFrame(conn)
		status, resp := c.handle(op, payload)

		writeFrame(conn, status, resp)
	}
}

func (c *Cell) handle(op byte, payload []byte) (byte, []byte) {
	switch op {
	case opAppend:
		offset, err := c.append(payload)

		if errors.Is(err, errFull) {
			return statusFull, nil
		}

		throw(err)

		return statusOk, binary.BigEndian.AppendUint64(nil, uint64(offset))
	case opRead:
		if len(payload) != 12 {
			return statusIo, nil
		}

		off := int64(binary.BigEndian.Uint64(payload))
		n := int64(binary.BigEndian.Uint32(payload[8:]))
		data, err := c.read(off, n)

		if errors.Is(err, errRange) {
			return statusRange, nil
		}

		throw(err)

		return statusOk, data
	case opStatus:
		head, free := c.status()
		out := binary.BigEndian.AppendUint64(nil, uint64(head))

		return statusOk, binary.BigEndian.AppendUint64(out, uint64(free))
	}

	throwFmt("proto: unknown op %d", op)

	return statusIo, nil
}

type CellClient struct {
	conn net.Conn
}

func dialCell(addr string) *CellClient {
	return &CellClient{conn: throw2(net.Dial("tcp", addr))}
}

func (cl *CellClient) close() {
	cl.conn.Close()
}

func (cl *CellClient) call(op byte, payload []byte) (byte, []byte) {
	writeFrame(cl.conn, op, payload)

	return readFrame(cl.conn)
}

func (cl *CellClient) append(data []byte) (int64, error) {
	status, resp := cl.call(opAppend, data)

	switch status {
	case statusOk:
		return int64(binary.BigEndian.Uint64(resp)), nil
	case statusFull:
		return 0, errFull
	}

	throwFmt("cell %s: append status %d", cl.conn.RemoteAddr(), status)

	return 0, nil
}

func (cl *CellClient) read(off int64, n int64) ([]byte, error) {
	req := binary.BigEndian.AppendUint64(nil, uint64(off))
	status, resp := cl.call(opRead, binary.BigEndian.AppendUint32(req, uint32(n)))

	switch status {
	case statusOk:
		return resp, nil
	case statusRange:
		return nil, errRange
	}

	throwFmt("cell %s: read status %d", cl.conn.RemoteAddr(), status)

	return nil, nil
}

func (cl *CellClient) status() (int64, int64) {
	status, resp := cl.call(opStatus, nil)

	if status != statusOk || len(resp) != 16 {
		throwFmt("cell %s: status %d", cl.conn.RemoteAddr(), status)
	}

	return int64(binary.BigEndian.Uint64(resp)), int64(binary.BigEndian.Uint64(resp[8:]))
}
