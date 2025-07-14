package rpc

import (
	"encoding/binary"
	"errors"
	"fmt"
	"io"
	"net"
	"strings"
	"sync"
	"sync/atomic"
	"time"

	"google.golang.org/protobuf/proto"
	pb "github.com/Shopify/yugabyte-db/shopify-yb-client/pb"
)

const (
	// RPC header magic bytes
	rpcHeaderMagic = "YB\x01"
	
	// Special call IDs
	ConnectionContextCallID = -3
	
	// Default timeout
	DefaultTimeout = 30 * time.Second
)

type Client struct {
	addr     string
	conn     net.Conn
	callID   int32
	mu       sync.Mutex
	pending  map[int32]chan *Response
	closed   bool
}

type Response struct {
	Header *pb.ResponseHeader
	Body   []byte
}

func NewClient(addr string) *Client {
	return &Client{
		addr:    addr,
		pending: make(map[int32]chan *Response),
	}
}

func (c *Client) Connect() error {
	c.mu.Lock()
	defer c.mu.Unlock()

	if c.conn != nil {
		return nil
	}

	conn, err := net.Dial("tcp", c.addr)
	if err != nil {
		return fmt.Errorf("failed to connect: %w", err)
	}

	// Send RPC header
	if _, err := conn.Write([]byte(rpcHeaderMagic)); err != nil {
		conn.Close()
		return fmt.Errorf("failed to write RPC header: %w", err)
	}

	// The Java client doesn't send ConnectionContextPB, so we skip it too

	c.conn = conn
	
	// Start response reader
	go c.readResponses()
	
	return nil
}


func (c *Client) writeMessage(conn net.Conn, header, body proto.Message) error {
	// Serialize header
	headerBytes, err := proto.Marshal(header)
	if err != nil {
		return fmt.Errorf("failed to marshal header: %w", err)
	}
	
	// Serialize body
	bodyBytes, err := proto.Marshal(body)
	if err != nil {
		return fmt.Errorf("failed to marshal body: %w", err)
	}
	
	// Calculate total size with varint prefixes
	headerSize := len(headerBytes)
	bodySize := len(bodyBytes)
	headerVarintSize := binary.PutUvarint(make([]byte, 10), uint64(headerSize))
	bodyVarintSize := binary.PutUvarint(make([]byte, 10), uint64(bodySize))
	totalSize := headerVarintSize + headerSize + bodyVarintSize + bodySize
	
	// Write total size (4 bytes, big-endian)
	if err := binary.Write(conn, binary.BigEndian, uint32(totalSize)); err != nil {
		return fmt.Errorf("failed to write total size: %w", err)
	}
	
	// Write header with varint size prefix
	if _, err := conn.Write(encodeUvarint(uint64(headerSize))); err != nil {
		return fmt.Errorf("failed to write header size: %w", err)
	}
	if _, err := conn.Write(headerBytes); err != nil {
		return fmt.Errorf("failed to write header: %w", err)
	}
	
	// Write body with varint size prefix
	if _, err := conn.Write(encodeUvarint(uint64(bodySize))); err != nil {
		return fmt.Errorf("failed to write body size: %w", err)
	}
	if _, err := conn.Write(bodyBytes); err != nil {
		return fmt.Errorf("failed to write body: %w", err)
	}
	
	return nil
}

func (c *Client) Call(service, method string, request, response proto.Message) error {
	c.mu.Lock()
	if c.closed {
		c.mu.Unlock()
		return errors.New("client is closed")
	}
	if c.conn == nil {
		c.mu.Unlock()
		return errors.New("not connected")
	}
	
	// Get next call ID
	callID := atomic.AddInt32(&c.callID, 1)
	
	// Create response channel
	respChan := make(chan *Response, 1)
	c.pending[callID] = respChan
	c.mu.Unlock()
	
	// Build request header
	header := &pb.RequestHeader{
		CallId: proto.Int32(callID),
		RemoteMethod: &pb.RemoteMethodPB{
			ServiceName: proto.String(service),
			MethodName:  proto.String(method),
		},
		TimeoutMillis: proto.Uint32(uint32(DefaultTimeout.Milliseconds())),
	}
	
	// Send request
	c.mu.Lock()
	err := c.writeMessage(c.conn, header, request)
	c.mu.Unlock()
	
	if err != nil {
		c.mu.Lock()
		delete(c.pending, callID)
		c.mu.Unlock()
		return err
	}
	
	// Wait for response
	select {
	case resp, ok := <-respChan:
		if !ok || resp == nil {
			return fmt.Errorf("connection closed")
		}
		
		if resp.Header.GetIsError() {
			var errStatus pb.ErrorStatusPB
			if err := proto.Unmarshal(resp.Body, &errStatus); err != nil {
				return fmt.Errorf("failed to unmarshal error: %w", err)
			}
			return fmt.Errorf("RPC error: %s", errStatus.GetMessage())
		}
		
		if err := proto.Unmarshal(resp.Body, response); err != nil {
			// Log the response type for debugging
			fmt.Printf("Failed to unmarshal response of type %T, body length: %d\n", response, len(resp.Body))
			if len(resp.Body) < 100 {
				fmt.Printf("Response body (hex): %x\n", resp.Body)
			}
			return fmt.Errorf("failed to unmarshal response: %w", err)
		}
		
		return nil
		
	case <-time.After(DefaultTimeout):
		c.mu.Lock()
		delete(c.pending, callID)
		c.mu.Unlock()
		return errors.New("RPC timeout")
	}
}

func (c *Client) readResponses() {
	defer func() {
		c.mu.Lock()
		c.closed = true
		c.conn.Close()
		for _, ch := range c.pending {
			close(ch)
		}
		c.mu.Unlock()
	}()
	
	for {
		// Read total size
		var totalSize uint32
		if err := binary.Read(c.conn, binary.BigEndian, &totalSize); err != nil {
			// EOF or connection closed is expected when server closes connection
			if err != io.EOF && !strings.Contains(err.Error(), "use of closed network connection") {
				fmt.Printf("Failed to read total size: %v\n", err)
			}
			return
		}
		
		// Read message
		msgBytes := make([]byte, totalSize)
		if _, err := io.ReadFull(c.conn, msgBytes); err != nil {
			fmt.Printf("Failed to read message: %v\n", err)
			return
		}
		
		// Parse header size
		headerSize, n := binary.Uvarint(msgBytes)
		if n <= 0 {
			fmt.Printf("Failed to parse header size\n")
			return
		}
		
		// Parse header
		headerEnd := n + int(headerSize)
		if headerEnd > len(msgBytes) {
			fmt.Printf("Invalid header size\n")
			return
		}
		
		var header pb.ResponseHeader
		if err := proto.Unmarshal(msgBytes[n:headerEnd], &header); err != nil {
			fmt.Printf("Failed to unmarshal header: %v\n", err)
			return
		}
		
		// Parse body size
		bodySize, n := binary.Uvarint(msgBytes[headerEnd:])
		if n <= 0 {
			fmt.Printf("Failed to parse body size\n")
			return
		}
		
		// Extract body
		bodyStart := headerEnd + n
		bodyEnd := bodyStart + int(bodySize)
		if bodyEnd > len(msgBytes) {
			fmt.Printf("Invalid body size\n")
			return
		}
		
		body := msgBytes[bodyStart:bodyEnd]
		
		// Deliver response
		c.mu.Lock()
		if ch, ok := c.pending[header.GetCallId()]; ok {
			delete(c.pending, header.GetCallId())
			ch <- &Response{
				Header: &header,
				Body:   body,
			}
		}
		c.mu.Unlock()
	}
}

func (c *Client) Close() error {
	c.mu.Lock()
	defer c.mu.Unlock()
	
	if c.conn != nil {
		return c.conn.Close()
	}
	return nil
}

func encodeUvarint(v uint64) []byte {
	buf := make([]byte, binary.MaxVarintLen64)
	n := binary.PutUvarint(buf, v)
	return buf[:n]
}