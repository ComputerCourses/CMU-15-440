// Implementation of a MultiEchoServer. Students should write their code in this file.

package p0

import (
	"net"
	"strconv"
	"bufio"
)

const (
	msgConnect   = "connect"
	msgClose     = "close"
	msgBroadcast = "broadcast"
)

const writeBufferCapacity = 100
const commandChanCapacity = 1024

type multiEchoServer struct {
	listener net.Listener

	quitC       chan struct{}
	acceptQuitC chan struct{}
	commandC    chan command
	connections map[*clientConn]bool
}

// New creates and returns (but does not start) a new MultiEchoServer.
func New() MultiEchoServer {
	return &multiEchoServer{
		quitC:       make(chan struct{}, 1),
		acceptQuitC: make(chan struct{}, 1),
		commandC:    make(chan command, commandChanCapacity),
		connections: make(map[*clientConn]bool),
	}
}

func (mes *multiEchoServer) Start(port int) error {
	listener, err := net.Listen("tcp", ":"+strconv.Itoa(port))
	if err != nil {
		return err
	}
	mes.listener = listener
	go mes.start()
	go mes.handleMessage()
	return nil
}

func (mes *multiEchoServer) Close() {
	mes.quitC <- struct{}{}
}

func (mes *multiEchoServer) Count() int {
	return len(mes.connections)
}

func (mes *multiEchoServer) handleMessage() {
	for {
		select {
		case cmd := <-mes.commandC:
			switch cmd.cmd {
			case msgConnect:
				conn := cmd.data.(*clientConn)
				mes.connections[conn] = true
			case msgClose: // close connection between client
				conn := cmd.data.(*clientConn)
				conn.close()
				delete(mes.connections, conn)
			case msgBroadcast:
				msg := cmd.data.([]byte)
				for conn := range mes.connections {
					conn.write(msg)
				}
			default:
				panic("invalid command " + cmd.cmd)
			}
		case <-mes.quitC:
			for conn := range mes.connections {
				conn.close()
			}
			mes.listener.Close()
			mes.acceptQuitC <- struct{}{}
			return
		}
	}

}

func (mes *multiEchoServer) start() {

	for {
		select {
		case <-mes.acceptQuitC:
			return
		default:
			conn, err := mes.listener.Accept()
			if err != nil {
				continue
			}
			go newConn(conn, mes.commandC).start()
		}
	}
}

type clientConn struct {
	conn     net.Conn
	reader   *bufio.Reader
	commandC chan<- command
	writeC   chan []byte
	quitC    chan struct{}
}

func newConn(conn net.Conn, commandC chan<- command) *clientConn {
	return &clientConn{
		conn:     conn,
		reader:   bufio.NewReader(conn),
		commandC: commandC,
		writeC:   make(chan []byte, writeBufferCapacity),
		quitC:    make(chan struct{}, 1),
	}
}

func (conn *clientConn) close() {
	conn.quitC <- struct{}{}
	conn.conn.Close()
}

func (conn *clientConn) write(msg []byte) {
	select {
	case conn.writeC <- msg:
	default:
	}
}

func (conn *clientConn) start() {

	defer func() {
		select {
		case conn.commandC <- command{cmd: msgClose, data: conn}:
		default:
		}
	}()

	select {
	case conn.commandC <- command{cmd: msgConnect, data: conn}:
	case <-conn.quitC:
		return
	}

	go func() {
		for {
			select {
			case msg := <-conn.writeC:
				conn.conn.Write([]byte(msg))
			case <-conn.quitC:
				return
			}
		}
	}()

	for {
		text, err := conn.reader.ReadBytes('\n')
		if err != nil {
			return
		}
		conn.commandC <- command{cmd: msgBroadcast, data: text}
	}
}

type command struct {
	cmd  string
	data interface{}
}
