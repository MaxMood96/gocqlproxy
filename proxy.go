package gocqlproxy

import (
	"bufio"
	"bytes"
	"context"
	"errors"
	"fmt"
	"net"
	"time"
)

func ProxyDefaultCompressor() Compressor {
	return &SnappyCompressor{}
}
const ProxyDefaultProtocolVersion = 4

// pickHost chooses a host using the session's policy.
// It is similar to the host-picking part of queryExecutor.do method.
// (See query_executor.go)
func (s *Session) pickHost(routingKey []byte) (*Conn, error) {
	partitioner := &murmur3Partitioner{}
	token := partitioner.Hash(routingKey)

	hostIter := s.policy.Pick(&Query{
		session:    s,
		routingKey: routingKey,
	})
	selectedHost := hostIter()

	for selectedHost != nil {
		host := selectedHost.Info()
		if host == nil || !host.IsUp() {
			selectedHost = hostIter()
			continue
		}

		pool, ok := s.pool.getPool(host)
		if !ok {
			selectedHost = hostIter()
			continue
		}

		conn := pool.Pick(token)
		if conn == nil {
			selectedHost = hostIter()
			continue
		}

		return conn, nil
	}
	return nil, ErrNoConnections
}

type writeProxiedFrame struct {
	head      frameHeader
	frameData []byte
}

func (e *writeProxiedFrame) String() string {
	return fmt.Sprintf("[proxiedFrame op=%d]", e.head.op)
}

func (e *writeProxiedFrame) writeFrame(fr *framer, streamID int) error {
	fr.writeHeader(e.head.flags, e.head.op, streamID)
	fr.wbuf = append(fr.wbuf, e.frameData...)
	return fr.finishWrite()
}

// MakeConn creates a Conn object to configure buffering and deadline used
// by the underlying gocql implementation.
func MakeConn(conn net.Conn) *Conn {
	return &Conn{
		r: bufio.NewReader(conn),
		w: &deadlineWriter{
			timeout: 2 * time.Second,
			w:       conn,
		},
	}
}

type query struct {
	clientFramer *framer
	conn         *Conn
	session      *Session
}

func (query *query) processProxyQuery(messageBytes []byte) (*framer, error) {
	// find the appropriate host to forward the frame to:
	partitionKey := query.clientFramer.readBytes()
	serverConn, err := query.session.pickHost(partitionKey)
	if err != nil {
		// TODO: we probably should return an error to the client here
		return nil, err
	}

	// parse the inner frame:
	nestedReader := bytes.NewReader(messageBytes)
	nestedHead, err := readHeader(nestedReader, query.conn.headerBuf[:])
	if err != nil {
		Logger.Println("reading nested frame header failed:", err)
		return nil, nil
	}
	if nestedHead.op != opQuery {
		return nil, errors.New("unknown nested frame type")
	}

	nestedFramer := newFramer(nestedReader, nil, &SnappyCompressor{}, 4)
	if err := nestedFramer.readFrame(&nestedHead); err != nil {
		// If nested frame is incorrect, there's still chance that next frame
		// will be fine, because the 'parent frame' was parsed correctly
		// - so we fail silently here.
		Logger.Println("reading nested frame failed:", err)
		return nil, nil
	}

	// send the inner frame to the Scylla host
	frameWriter := &writeProxiedFrame{
		head:      nestedHead,
		frameData: nestedFramer.rbuf,
	}
	responseFramer, err := serverConn.exec(context.TODO(), frameWriter, nil)
	if err != nil {
		// recoverable, because the 'parent frame' was parsed
		Logger.Println("exec failed", err)
		// TODO: we probably should return an error here
		return nil, nil
	}

	return responseFramer, nil
}

func (query *query) process() error {
	clientFramer := query.clientFramer
	outerHeader := query.clientFramer.header

	// read frame contents
	nestedBytes, err := clientFramer.readBytesInternal()
	if err != nil {
		// unrecoverable, because some bytes are missing before frame boundary
		return err
	}

	// create the response for the current frame:
	responseFramer, err := query.processProxyQuery(nestedBytes)
	if err != nil {
		return err
	}

	if responseFramer == nil {
		// TODO: we probably should return an error to the client instead of ignoring
		// the recoverable errors
		return nil
	}

	// write the response:
	clientFramer.writeHeader(
		responseFramer.header.flags,
		responseFramer.header.op,
		outerHeader.stream,
	)
	clientFramer.wbuf = append(
		clientFramer.wbuf,
		responseFramer.rbuf...
	)
	err = clientFramer.finishWrite()
	if err != nil {
		return err
	}

	return nil
}

// HandleConn is the implementation of connection processing in gocqlproxy,
// it is used in main.go in the main listening loop.
func (s *Session) HandleConn(clientConn net.Conn) {
	conn := MakeConn(clientConn)
	var err error
	for err == nil {
		head, err := readHeader(conn, conn.headerBuf[:])
		if err != nil {
			// Likely unrecoverable, because the failure can occur between
			// frame boundaries.
			Logger.Println("reading frame header failed:", err)
			break
		}
		framer := newFramer(
			conn, conn, ProxyDefaultCompressor(), ProxyDefaultProtocolVersion,
		)
		framer.header = &head
		if err := framer.readFrame(framer.header); err != nil {
			// Likely unrecoverable, because the failure can occur between
			// frame boundaries.
			Logger.Println(err)
			break
		}
		if head.stream < 0 {
			// we don't use negative stream numbers
			Logger.Println("unexpected negative stream id")
			continue
		}
		if head.op != opProxyQuery {
			Logger.Println("unknown frame type:", head.op)
			continue
		}
		query := &query{
			clientFramer: framer,
			conn:         conn,
			session:      s,
		}

		// send the frame to Scylla, pass the response back to the client
		if err = query.process(); err != nil {
			Logger.Println(err)
		}
	}
}
