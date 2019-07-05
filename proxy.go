package gocqlproxy

import (
	"bufio"
	"bytes"
	"context"
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

type proxyResponse struct {
	op        frameOp
	flags     byte
	frameData []byte
}

func makeResponse(framer *framer) *proxyResponse {
	return &proxyResponse{
		 op:        framer.header.op,
		 flags:     framer.header.flags,
		 frameData: framer.rbuf,
	}
}

func makeErrorResponse(template string, values ...interface{}) *proxyResponse {
	framer := newFramer(nil, nil, nil, 0)
	framer.writeInt(errServer)
	framer.writeString(fmt.Sprintf(template, values...))
	return &proxyResponse{
		 op:        errServer,
		 frameData: framer.wbuf,
	}
}


func (query *query) processProxyQuery(messageBytes []byte) *proxyResponse {
	// parse the inner frame:
	nestedReader := bytes.NewReader(messageBytes)
	nestedHead, err := readHeader(nestedReader, query.conn.headerBuf[:])
	if err != nil {
		return makeErrorResponse("reading nested frame header error: %s", err)
	}
	nestedOp := nestedHead.op
	if nestedOp != opQuery && nestedOp != opPrepare && nestedOp != opBatch {
		errorMessage := fmt.Sprintf("unsupported nested frame type: %s", nestedOp)
		return makeErrorResponse(errorMessage)
	}

	nestedFramer := newFramer(nestedReader, nil, &SnappyCompressor{}, 4)
	if err := nestedFramer.readFrame(&nestedHead); err != nil {
		// If nested frame is incorrect, there's still chance that next frame
		// will be fine, because the 'parent frame' was parsed correctly
		// - so we fail silently here.
		return makeErrorResponse("reading nested frame failed: %s", err)
	}

	// send the inner frame to the Scylla host
	frameWriter := &writeProxiedFrame{
		head:      nestedHead,
		frameData: nestedFramer.rbuf,
	}
	if nestedOp == opPrepare {
		var responseFramer *framer
		nextIter := query.session.policy.Pick(nil)
		host := nextIter()
		// send opPrepare messages to all the hosts
		for host != nil {
			hostInfo := host.Info()
			serverIP := hostInfo.ConnectAddress()
			pool, ok := query.session.pool.getPool(hostInfo)
			if !ok {
				return makeErrorResponse(
					"cannot find connection for %s", serverIP)
			}
			conn := pool.Pick(nil)
			responseFramer, err = conn.exec(context.TODO(), frameWriter, nil)
			if err != nil {
				return makeErrorResponse("prepare error: %s", err)
			}
			host = nextIter()
		}
		return makeResponse(responseFramer)
	}
	// find the appropriate host to forward the frame to:
	partitionKey := query.clientFramer.readBytes()
	serverConn, err := query.session.pickHost(partitionKey)
	if err != nil {
		return makeErrorResponse("pickHost error: %s", err)
	}
	responseFramer, err := serverConn.exec(context.TODO(), frameWriter, nil)
	if err != nil {
		return makeErrorResponse("exec failed: %s", err)
	}

	return makeResponse(responseFramer)
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

	// pass the inner data to Scylla server:
	response := query.processProxyQuery(nestedBytes)

	// return the response back to client:
	clientFramer.writeHeader(
		response.flags,
		response.op,
		outerHeader.stream,
	)
	clientFramer.wbuf = append(
		clientFramer.wbuf,
		response.frameData...
	)
	return clientFramer.finishWrite()
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
			Logger.Println("reading frame data failed:", err)
			break
		}
		if head.stream < 0 {
			// we don't use negative stream numbers
			Logger.Println("unexpected negative stream id:", head.stream)
			continue
		}
		if head.op != opProxyQuery {
			Logger.Println("unsupported frame type:", head.op)
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
