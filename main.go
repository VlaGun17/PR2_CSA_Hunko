package main

import (
    "bufio"
    "log"
    "net"
    "strings"
	"time"
	"sync"
	"fmt"
)

type Server struct {
	mu sync.Mutex
	activeConnections map[net.Conn]bool
}

func main() {
	listener, err := net.Listen("tcp", ":9000");
	if err != nil {
		log.Fatal(err);
	}
	defer listener.Close()

	srv := &Server{
		activeConnections: make(map[net.Conn]bool),
	}

	for {
		conn, err := listener.Accept();
		if err != nil {
			break
		}
		go srv.handleConnection(conn);
	}
}

func (s *Server) addConnection(conn net.Conn) {
	s.mu.Lock()
	s.activeConnections[conn] = true
	activeCount := len(s.activeConnections)
	s.mu.Unlock()
	log.Printf("[SYSTEM]: New connection established %s. Active connections: %d\n", conn.RemoteAddr(), activeCount)
	joinMessage := "[SYSTEM]: " + conn.RemoteAddr().String() + " has joined the server(active: " + fmt.Sprint(activeCount) + ").\n"
	s.broadcastMessage(joinMessage, conn)
}

func (s *Server) removeConnection(conn net.Conn) {
	s.mu.Lock()
	if _, exists := s.activeConnections[conn]; !exists {
		s.mu.Unlock()
		return
	}
	delete(s.activeConnections, conn)
	activeCount := len(s.activeConnections)
	s.mu.Unlock()
	log.Printf("[SYSTEM]: Connection closed %s. Active connections: %d\n", conn.RemoteAddr(), activeCount)
	leaveMessage := "[SYSTEM]: " + conn.RemoteAddr().String() + " has left the server(active: " + fmt.Sprint(activeCount) + ").\n"
	s.broadcastMessage(leaveMessage, conn)
}

func (s *Server) broadcastMessage(message string, excludeConn net.Conn) {
	s.mu.Lock()
	defer s.mu.Unlock()
	for conn := range s.activeConnections {
		if conn != excludeConn {
			conn.Write([]byte(message))
		}
	}
}
 
func (s *Server) handleConnection(conn net.Conn) {
	s.addConnection(conn);
	defer conn.Close()
	defer s.removeConnection(conn);

	conn.Write([]byte("Welcome to the TCP server! Type 'echo', 'time', 'close', or 'quit' followed by your message.\n"));

	reader := bufio.NewReader(conn);
	for {
		message, err := reader.ReadString('\n');
		if err != nil {
			break
		}

		cleanMessage := strings.Map(func(r rune) rune {
			if r >= 32 {
				return r
			}
			return -1
		}, message)

		trimmedMessage := strings.TrimSpace(cleanMessage);
		parts := strings.SplitN(trimmedMessage, " ", 2)
		command := strings.ToLower(parts[0])

		switch command {
		case "echo":
			var response string
			if len(parts) > 1 {
				response = parts[1]
			}
			conn.Write([]byte("[ECHO]: " + response + "\n"));
		case "time":
			var response string
			response = "Current time is: " + time.Now().Format(time.RFC3339) + "\n"
			conn.Write([]byte(response));
		case "broadcast", "msg":
			if len(parts) < 2 || strings.TrimSpace(parts[1]) == "" {
				conn.Write([]byte("Usage: broadcast <message>\n"));
			} else {
				broadcastMessage := "[BROADCAST from " + conn.RemoteAddr().String() + "]: " + parts[1] + "\n"
				s.broadcastMessage(broadcastMessage, conn)
				conn.Write([]byte("Message broadcasted.\n"))
			}
		case "close", "quit":
			conn.Write([]byte("Closing connection.\n"));
			return
		default:
			conn.Write([]byte("Unknown command. Available commands: echo, time, close, quit.\n"));
		}
	}
}