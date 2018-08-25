// Contains the implementation of a LSP server.

package lsp

import (
	"errors"
	"strconv"
	"sync"
	"encoding/json"
	"fmt"
	"container/list"
	"time"
	"github.com/cmu440/lspnet"
)

var (
	ErrNotExist     = errors.New("connection does not exist")
	ErrClosed       = errors.New("connection already closed")
	ErrClientClosed = errors.New("client already closed")
	ErrClientLost   = errors.New("some client connections has lost")
)

const (
	defaultReceiveCapacity = 1024
	defaultSendCapacity    = 1024

	msgClose MsgType = MsgType(0xff)
)

type receiveMsg struct {
	msg        *Message
	remoteAddr *lspnet.UDPAddr
}

func newReceiveMsg(msg *Message, addr *lspnet.UDPAddr) *receiveMsg {
	return &receiveMsg{msg, addr}
}

type sendMsg struct {
	msg        *Message
	remoteAddr *lspnet.UDPAddr
}

func newSendMsg(msg *Message, addr *lspnet.UDPAddr) *sendMsg {
	return &sendMsg{msg, addr}
}

type deliveryMsgType int

const (
	msgClientClosed deliveryMsgType = iota
	msgDataReceived
)

type deliveryMsg struct {
	type_  deliveryMsgType
	connId int
	data   []byte
}

func newDeliveryMsg(msgType deliveryMsgType, connId int, data []byte) *deliveryMsg {
	return &deliveryMsg{msgType, connId, data}
}

type server struct {
	params *Params
	nextID int
	conn   *lspnet.UDPConn

	readC chan *deliveryMsg // deliver message to Read function
	// spawned client connections notify server it has shutdown.
	// if server received false, client connections has lost
	notifyC chan bool

	receivedC chan *Message // received message but not have read by application
	msgSendC  chan *sendMsg

	inboundC  chan *receiveMsg
	outboundC chan *sendMsg

	isClosed bool
	inClose  bool
	closeC   chan struct{}

	mu          *sync.Mutex
	cond        *sync.Cond
	addr2id     map[string]int
	connections map[int]*clientConn
}

// NewServer creates, initiates, and returns a new server. This function should
// NOT block. Instead, it should spawn one or more goroutines (to handle things
// like accepting incoming client connections, triggering epoch events at
// fixed intervals, synchronizing events using a for-select loop like you saw in
// project 0, etc.) and immediately return. It should return a non-nil error if
// there was an error resolving or listening on the specified port number.
func NewServer(port int, params *Params) (Server, error) {

	udpAddr, err := lspnet.ResolveUDPAddr("udp", ":"+strconv.Itoa(port))
	if err != nil {
		return nil, err
	}
	conn, err := lspnet.ListenUDP("udp", udpAddr)
	if err != nil {
		return nil, err
	}

	server := server{
		params: params,
		conn:   conn,
		nextID: 0,

		readC:     make(chan *deliveryMsg, defaultReceiveCapacity),
		inboundC:  make(chan *receiveMsg, defaultReceiveCapacity),
		msgSendC:  make(chan *sendMsg, defaultSendCapacity),
		outboundC: make(chan *sendMsg, defaultSendCapacity),

		mu:          &sync.Mutex{},
		closeC:      make(chan struct{}, 1),
		addr2id:     make(map[string]int),
		connections: make(map[int]*clientConn),
	}

	server.cond = sync.NewCond(server.mu)

	debug("server start...\n")
	go server.start()

	return &server, nil
}

func (s *server) start() {

	go s.receiveMessage()

	for {
		select {
		case <-s.closeC:
			s.inClose = true
		case msg := <-s.inboundC:
			if s.isClosed {
				break
			}

			if msg.msg.Type == MsgConnect {
				if !s.inClose {
					s.handleConnect(msg.remoteAddr)
				}
			} else {
				// delivery receive message to corresponding clientConn to handle
				address := msg.remoteAddr.String()
				var (
					id   int
					conn *clientConn
					ok   bool
				)
				s.mu.Lock()
				id, ok = s.addr2id[address]
				if ok {
					conn, ok = s.connections[id]
				}
				s.mu.Unlock()
				if ok {
					conn.receiveMsg(msg.msg)
				}
			}

		case msg := <-s.outboundC:
			if data, err := json.Marshal(msg.msg); err != nil {
				debug("encode message error: %s\n", err.Error())
				panic(err)
			} else {
				debug("server send message: %s\n", msg.msg.String())
				s.conn.WriteToUDP(data, msg.remoteAddr)
			}
		}
	}
}

func (s *server) handleConnect(addr *lspnet.UDPAddr) {
	s.mu.Lock()
	id, ok := s.addr2id[addr.String()]
	if !ok {
		id = s.nextConnID()
		debug("server new client connection [%d]\n", id)
		address := addr.String()
		s.addr2id[address] = id
		client := &clientConn{
			id:              id,
			seqNum:          0,
			address:         address,
			addr:            addr,
			s:               s,
			windowSize:      s.params.WindowSize,
			params:          s.params,
			pendingBuf:      list.New(),
			sendBuf:         make(map[int]*inFlyMessage),
			lastAckSeq:      0,
			receiveBuf:      list.New(),
			receivedDataSeq: 0,
			receivedMsgC:    s.readC,
			inboundC:        make(chan *Message, defaultReceiveCapacity),
			outboundC:       make(chan *Message, defaultSendCapacity),

			inClose:  false,
			isClosed: false,
			closeC:   make(chan struct{}, 1),
		}
		s.connections[id] = client

		go client.start()
	}
	s.mu.Unlock()

	ack := NewAck(id, 0)
	s.send(ack, addr)
}

func (s *server) receiveMessage() {
	data := make([]byte, 65536)

	for {
		n, addr, err := s.conn.ReadFromUDP(data[0:])
		if err != nil {
			break
		}
		msg := new(Message)

		if err := json.Unmarshal(data[:n], msg); err != nil {
			debug("receive corrupted message from %s\n", addr.String())
			continue
		}

		s.inboundC <- newReceiveMsg(msg, addr)
	}
}

func (s *server) send(msg *Message, addr *lspnet.UDPAddr) {
	s.outboundC <- newSendMsg(msg, addr)
}

func (s *server) nextConnID() int {
	s.nextID++
	return s.nextID
}

func (s *server) Read() (int, []byte, error) {
	select {
	case <-s.closeC:
		return 0, nil, ErrClosed
	case msg := <-s.readC:
		switch msg.type_ {
		case msgDataReceived:
			return msg.connId, msg.data, nil
		case msgClientClosed:
			return msg.connId, nil, ErrClientClosed
		default:
			panic(fmt.Sprintf("unrecognized message type: %d", msg.type_))
		}
	}
}

func (s *server) Write(connID int, payload []byte) error {
	s.mu.Lock()
	client, ok := s.connections[connID]
	s.mu.Unlock()

	if !ok {
		return ErrClosed
	}

	client.write(payload, false)
	return nil
}

func (s *server) CloseConn(connID int) error {
	s.mu.Lock()
	client, ok := s.connections[connID]
	s.mu.Unlock()
	if !ok {
		return ErrNotExist
	}
	client.close()
	return nil
}

func (s *server) Close() error {

	debug("server closing...\n")

	s.closeC <- struct{}{}

	s.mu.Lock()

	for _, conn := range s.connections {
		conn.close()
	}

	for len(s.connections) != 0 {
		s.cond.Wait()
	}
	s.mu.Unlock()

	s.conn.Close()
	debug("server closed\n")
	return nil
}

func (s *server) removeClient(address string, connId int) {
	debug("server remove client [%d]\n", connId)
	s.mu.Lock()
	delete(s.connections, connId)
	delete(s.addr2id, address)
	s.cond.Signal()
	s.mu.Unlock()
}

type clientConn struct {
	id     int
	seqNum int

	address string
	addr    *lspnet.UDPAddr
	s       *server

	windowSize int
	params     *Params

	pendingBuf *list.List
	sendBuf    map[int]*inFlyMessage
	lastAckSeq int

	receiveBuf      *list.List
	receivedDataSeq int
	receivedMsgC    chan<- *deliveryMsg

	inboundC  chan *Message
	outboundC chan *Message

	timer *time.Timer
	mu    sync.Mutex

	inClose  bool
	isClosed bool
	closeC   chan struct{}
}

func (c *clientConn) close() {
	debug("server close connection between client %d...\n", c.id)
	c.closeC <- struct{}{}
}

func (c *clientConn) send(msg *Message) {
	c.s.send(msg, c.addr)
}

func (c *clientConn) sendData(msg *Message, count int) {
	c.send(msg)
	c.sendBuf[msg.SeqNum] = newInFlyMessage(
		time.Now().Add(time.Millisecond*time.Duration(c.params.EpochMillis)),
		count,
		msg)
}

func (c *clientConn) nextSeqNum() int {
	c.seqNum++
	return c.seqNum
}

func (c *clientConn) write(payload []byte, close bool) {

	var msg *Message
	if !close {
		msg = NewData(c.id, c.nextSeqNum(), len(payload), payload)
	} else {
		msg = &Message{Type: msgClose, ConnID: c.id, SeqNum: c.nextSeqNum()}
	}

	select {
	case c.outboundC <- msg:
	default:
		c.mu.Lock()
		defer c.mu.Unlock()
		c.pendingBuf.PushBack(msg)
	}
}

func (c *clientConn) onSendData(msg *Message) {
	c.mu.Lock()
	if c.windowSize > 0 && c.pendingBuf.Len() == 0 {
		c.mu.Unlock()
		c.sendData(msg, 0)
		c.windowSize--
	} else {
		debug("server client [%d] pending message: %s\n", c.id, msg.String())
		c.pendingBuf.PushBack(msg)
		c.mu.Unlock()
	}
}

func (c *clientConn) onReceiveData(msg *Message) {

	if msg.SeqNum == c.receivedDataSeq+1 {
		c.receivedDataSeq++
		c.send(NewAck(c.id, msg.SeqNum))
		c.receivedMsgC <- newDeliveryMsg(msgDataReceived, c.id, msg.Payload)

		if c.receiveBuf.Len() != 0 {
			toRemove := make([]*list.Element, 0)

			for elem := c.receiveBuf.Front(); elem != c.receiveBuf.Back(); elem = elem.Next() {
				m := elem.Value.(*Message)
				if m.SeqNum < c.receivedDataSeq+1 {
					toRemove = append(toRemove, elem)
				} else if m.SeqNum == c.receivedDataSeq+1 {
					toRemove = append(toRemove, elem)
					c.receivedDataSeq++
					c.send(NewAck(c.id, m.SeqNum))
					c.receivedMsgC <- newDeliveryMsg(msgDataReceived, c.id, m.Payload)
				} else {
					break
				}
			}
		}
	} else if msg.SeqNum > c.receivedDataSeq+1 {
		// receive out-of-order message
		if c.receiveBuf.Len() == 0 || msg.SeqNum < c.receiveBuf.Front().Value.(*Message).SeqNum {
			c.receiveBuf.PushFront(msg)
		} else if msg.SeqNum > c.receiveBuf.Back().Value.(*Message).SeqNum {
			c.receiveBuf.PushBack(msg)
		} else {
			for elem := c.receiveBuf.Front(); elem != c.receiveBuf.Back(); elem = elem.Next() {
				if elem.Value.(*Message).SeqNum > msg.SeqNum {
					c.receiveBuf.InsertBefore(msg, elem)
					break
				}
			}
		}
	} else {
		c.send(NewAck(c.id, c.receivedDataSeq))
	}
}

func (c *clientConn) onReceiveAck(msg *Message) {

	debug("server client [%d] receive ack: %s\n", c.id, msg.String())

	seq := msg.SeqNum

	if seq >= c.lastAckSeq+1 {
		c.windowSize += seq - c.lastAckSeq
		c.lastAckSeq = seq

		if c.windowSize > c.params.WindowSize {
			c.windowSize = c.params.WindowSize
		}

		toRemove := make([]int, 0)
		for sendSeq := range c.sendBuf {
			if sendSeq <= seq {
				toRemove = append(toRemove, sendSeq)
			}
		}
		for _, n := range toRemove {
			delete(c.sendBuf, n)
		}

		for c.windowSize > 0 && c.pendingBuf.Len() > 0 {
			front := c.pendingBuf.Front()
			c.sendData(front.Value.(*Message), 0)
			c.pendingBuf.Remove(front)
			c.windowSize--
		}
	}

	if c.inClose {
		if len(c.sendBuf) == 0 && c.pendingBuf.Len() == 0 {
			debug("server connection between client [%d] has closed\n", c.id)
			c.isClosed = true
		} else {
			debug("sendBuf: %d, pendingBuf: %d\n", len(c.sendBuf), c.pendingBuf.Len())
		}
	}
}

func (c *clientConn) onTimeout() {
	timeout := time.Millisecond * time.Duration(c.params.EpochMillis)
	now := time.Now()

	for _, t := range c.sendBuf {
		if t.expireTime.Before(now) {
			if t.epochCount > c.params.EpochLimit {
				debug("server client [%d] connection is lost", c.id)
				c.receivedMsgC <- newDeliveryMsg(msgClientClosed, c.id, nil)
				c.isClosed = true
				return
			} else {
				debug("server client [%d] %d-th resend message: %s\n", c.id, t.epochCount+1, t.msg.String())
				c.sendData(t.msg, t.epochCount+1)
			}
		} else {
			next := t.expireTime.Sub(now)
			if next < timeout {
				timeout = next
			}
		}
	}

	c.send(NewAck(c.id, c.receivedDataSeq))
	c.resetTimer(timeout)
}

func (c *clientConn) receiveMsg(msg *Message) {
	c.inboundC <- msg
}

func (c *clientConn) resetTimer(duration time.Duration) {
	if c.timer == nil {
		c.timer = time.NewTimer(duration)
	} else {
		if !c.timer.Stop() {
			select {
			case <-c.timer.C:
			default:
			}
		}
		c.timer.Reset(duration)
	}
}

func (c *clientConn) start() {

	defer func() {
		c.s.removeClient(c.address, c.id)
	}()

	c.resetTimer(time.Millisecond * time.Duration(c.params.EpochMillis))

	for {
		if c.isClosed {
			break
		}
		select {
		case <-c.closeC:
			debug("server client [%d] in closing...\n", c.id)
			c.inClose = true
			c.write(nil, true) // notify peer to close connection
		case <-c.timer.C:
			c.onTimeout()
			if c.isClosed {
				return
			}
		case msg := <-c.outboundC:
			if (c.inClose || c.isClosed) && msg.Type != msgClose {
				// stop send new data if current connection is decided to close
				continue
			}
			c.onSendData(msg)
		case msg := <-c.inboundC:
			debug("server [%d] receive message: %s\n", c.id, msg.String())
			switch msg.Type {
			case MsgConnect:
				debug("server [%d] receive connect after connection has established, ignore message", c.id)
			case MsgData:
				c.onReceiveData(msg)
			case MsgAck:
				c.onReceiveAck(msg)
			case msgClose:
				// connection closed by peer
			}
		}
	}
}
