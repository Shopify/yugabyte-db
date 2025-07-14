package rpc

import (
	"bytes"
	"encoding/binary"
	"net"
	"testing"
	"time"

	"google.golang.org/protobuf/proto"
	pb "github.com/Shopify/yugabyte-db/shopify-yb-client/pb"
)

func TestEncodeUvarint(t *testing.T) {
	tests := []struct {
		name     string
		input    uint64
		expected []byte
	}{
		{"Zero", 0, []byte{0}},
		{"One", 1, []byte{1}},
		{"127", 127, []byte{127}},
		{"128", 128, []byte{128, 1}},
		{"300", 300, []byte{172, 2}},
	}

	for _, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {
			result := encodeUvarint(tt.input)
			if !bytes.Equal(result, tt.expected) {
				t.Errorf("encodeUvarint(%d) = %v, want %v", tt.input, result, tt.expected)
			}
		})
	}
}

func TestNewClient(t *testing.T) {
	client := NewClient("localhost:7100")
	if client == nil {
		t.Fatal("NewClient returned nil")
	}
	if client.addr != "localhost:7100" {
		t.Errorf("Expected addr to be localhost:7100, got %s", client.addr)
	}
	if client.pending == nil {
		t.Fatal("pending map not initialized")
	}
}

func TestRPCHeader(t *testing.T) {
	expected := []byte{'Y', 'B', 1}
	if !bytes.Equal([]byte(rpcHeaderMagic), expected) {
		t.Errorf("RPC header = %v, want %v", []byte(rpcHeaderMagic), expected)
	}
}

// MockServer simulates a YugabyteDB server for testing
type MockServer struct {
	listener net.Listener
	t        *testing.T
	handler  func(net.Conn)
}

func NewMockServer(t *testing.T, handler func(net.Conn)) (*MockServer, string) {
	listener, err := net.Listen("tcp", "127.0.0.1:0")
	if err != nil {
		t.Fatalf("Failed to create listener: %v", err)
	}

	server := &MockServer{
		listener: listener,
		t:        t,
		handler:  handler,
	}

	go server.serve()
	return server, listener.Addr().String()
}

func (s *MockServer) serve() {
	for {
		conn, err := s.listener.Accept()
		if err != nil {
			return
		}
		go s.handler(conn)
	}
}

func (s *MockServer) Close() {
	s.listener.Close()
}

func TestConnect(t *testing.T) {
	// Create a mock server that expects the RPC header
	server, addr := NewMockServer(t, func(conn net.Conn) {
		defer conn.Close()

		// Read RPC header
		header := make([]byte, 3)
		if _, err := conn.Read(header); err != nil {
			t.Errorf("Failed to read header: %v", err)
			return
		}

		if !bytes.Equal(header, []byte("YB\x01")) {
			t.Errorf("Invalid header: %v", header)
			return
		}

		// Keep connection open briefly
		time.Sleep(100 * time.Millisecond)
	})
	defer server.Close()

	client := NewClient(addr)
	err := client.Connect()
	if err != nil {
		t.Fatalf("Failed to connect: %v", err)
	}
	defer client.Close()

	// Verify connection was established
	if client.conn == nil {
		t.Error("Connection not established")
	}
}

func TestWriteMessage(t *testing.T) {
	// Create a buffer to capture the output
	var buf bytes.Buffer
	
	client := &Client{}
	
	header := &pb.RequestHeader{
		CallId: proto.Int32(1),
		RemoteMethod: &pb.RemoteMethodPB{
			ServiceName: proto.String("TestService"),
			MethodName:  proto.String("TestMethod"),
		},
	}
	
	body := &pb.ListTabletServersRequestPB{
		PrimaryOnly: proto.Bool(false),
	}
	
	// Use a writer that captures to buffer
	conn := &mockConn{Writer: &buf}
	err := client.writeMessage(conn, header, body)
	if err != nil {
		t.Fatalf("writeMessage failed: %v", err)
	}
	
	// Verify the message was written
	if buf.Len() == 0 {
		t.Error("No data written")
	}
	
	// Read and verify total size
	var totalSize uint32
	if err := binary.Read(&buf, binary.BigEndian, &totalSize); err != nil {
		t.Fatalf("Failed to read total size: %v", err)
	}
	
	if totalSize == 0 {
		t.Error("Total size is 0")
	}
}

// mockConn implements net.Conn for testing
type mockConn struct {
	*bytes.Buffer
	Writer *bytes.Buffer
}

func (m *mockConn) Write(b []byte) (n int, err error) {
	return m.Writer.Write(b)
}

func (m *mockConn) Close() error                       { return nil }
func (m *mockConn) LocalAddr() net.Addr                { return nil }
func (m *mockConn) RemoteAddr() net.Addr               { return nil }
func (m *mockConn) SetDeadline(t time.Time) error      { return nil }
func (m *mockConn) SetReadDeadline(t time.Time) error  { return nil }
func (m *mockConn) SetWriteDeadline(t time.Time) error { return nil }

func TestConnectionContextCallID(t *testing.T) {
	if ConnectionContextCallID != -3 {
		t.Errorf("ConnectionContextCallID = %d, want -3", ConnectionContextCallID)
	}
}

func TestDefaultTimeout(t *testing.T) {
	if DefaultTimeout != 30*time.Second {
		t.Errorf("DefaultTimeout = %v, want 30s", DefaultTimeout)
	}
}