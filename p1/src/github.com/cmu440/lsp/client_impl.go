// Contains the implementation of a LSP client.

package lsp

import (
	"errors"
	"time"
	"log"
	"encoding/json"
	"container/list"
	"sync/atomic"
	"net"
	"github.com/cmu440/lspnet"
)

const debugOn = true

func debug(format string, v ...interface{}) {
	if debugOn {
		log.Printf(format, v...)
	}
}

func init() {
	lspnet.EnableDebugLogs(true)
}

const (
	stateConnecting int32 = iota
	stateConnected
	stateActiveClosing
	statePassiveClosing
	stateActiveClosed
	statePassiveClosed
)

const (
	defaultClientSendChanCapacity    = 1024
	defaultClientReceiveChanCapacity = 1024
)

type inFlyMessage struct {
	expireTime time.Time
	epochCount int
	msg        *Message
}

func newInFlyMessage(expireTime time.Time, epochCount int, msg *Message) *inFlyMessage {
	return &inFlyMessage{expireTime, epochCount, msg}
}

type client struct {
	connID int

	seqNum     int
	windowSize int

	pendingBuf *list.List
	sendBuf    map[int]*inFlyMessage // message buffer that has been sent but not receive ack
	lastAckSeq int

	receiveBuf      *list.List // store unordered message received from peer
	receivedDataSeq int        // largest data seq has received and sent to application

	params *Params

	receiveMsgC  chan *Message
	sendMsgC     chan *Message
	receivedMsgC chan *Message // message already received from peer, but not has been read by application

	closeC  chan struct{} // Close function notify background goroutine to quit
	notifyC chan struct{} // background goroutine notify Close function the goroutine has quit

	timer     *time.Timer
	conn      *lspnet.UDPConn
	connState int32
}

// NewClient creates, initiates, and returns a new client. This function
// should return after a connection with the server has been established
// (i.e., the client has received an Ack message from the server in response
// to its connection request), and should return a non-nil error if a
// connection could not be made (i.e., if after K epochs, the client still
// hasn't received an Ack message from the server in response to its K
// connection requests).
//
// hostport is a colon-separated string identifying the server's host address
// and port number (i.e., "localhost:9999").
func NewClient(hostport string, params *Params) (Client, error) {

	udpAddr, err := lspnet.ResolveUDPAddr("udp", hostport)
	if err != nil {
		return nil, err
	}

	c, err := lspnet.DialUDP("udp", nil, udpAddr)
	if err != nil {
		return nil, err
	}

	client := &client{
		conn:       c,
		seqNum:     0,
		params:     params,
		windowSize: params.WindowSize,

		pendingBuf: list.New(),
		sendBuf:    make(map[int]*inFlyMessage),

		receiveBuf: list.New(),

		connState: stateConnecting,

		receiveMsgC:  make(chan *Message, defaultClientReceiveChanCapacity),
		receivedMsgC: make(chan *Message, defaultClientReceiveChanCapacity),
		sendMsgC:     make(chan *Message, defaultClientSendChanCapacity),

		closeC:  make(chan struct{}),
		notifyC: make(chan struct{}, 2),
	}

	id, err := client.connect(udpAddr)

	if err != nil {
		debug("client connect error: %s", err.Error())
		return nil, err
	}

	client.connID = id
	client.connState = stateConnected
	debug("new client with id: %d", id)

	go client.start()

	return client, nil
}

func (c *client) connect(addr *lspnet.UDPAddr) (int, error) {

	reqBytes, err := json.Marshal(NewConnect())
	if err != nil {
		return 0, err
	}

	decoder := json.NewDecoder(c.conn)

	var response Message

	reDial := func() error {
		conn, err := lspnet.DialUDP("udp", nil, addr)
		if err != nil {
			return err
		}
		c.conn = conn
		decoder = json.NewDecoder(c.conn)
		return nil
	}

	for i := 0; i < c.params.EpochLimit; i++ {

		debug("[client %d] connect try %d...", c.connID, i+1)

		timeout := time.Now().Add(time.Millisecond * time.Duration(c.params.EpochMillis))
		c.conn.SetDeadline(timeout)

		if _, err = c.conn.Write(reqBytes); err != nil {
			if err, ok := err.(net.Error); ok && err.Timeout() {
				debug("[client %d] write timeout", c.connID)
				continue
			} else {
				debug("[client %d] write udp error: %s\n", c.connID, err.Error())
				now := time.Now()
				if now.Before(timeout) {
					time.Sleep(timeout.Sub(now))
				}
				c.conn.Close()
				reDial()
				continue
			}

		}

		if err := decoder.Decode(&response); err != nil {
			if err, ok := err.(net.Error); ok && err.Timeout() {
				debug("[client %d] read timeout", c.connID)
				continue
			} else {
				debug("[client %d] read udp error: %s\n", c.connID, err.Error())
				now := time.Now()
				if now.Before(timeout) {
					time.Sleep(timeout.Sub(now))
				}
				c.conn.Close()
				reDial()
				continue
			}

		}

		if response.Type == MsgAck {
			c.conn.SetDeadline(time.Time{})
			debug("[client %d] connect succeeded\n", response.ConnID)
			return response.ConnID, nil
		}
	}

	return 0, errors.New("connect timeout")
}

func (c *client) send(msg *Message) {
	debug("[client %d] send message: %s\n", c.connID, msg.String())
	data, _ := json.Marshal(msg)
	c.conn.Write(data)
}

func (c *client) sendData(msg *Message, count int) {
	c.send(msg)
	c.sendBuf[msg.SeqNum] = newInFlyMessage(
		time.Now().Add(time.Millisecond*time.Duration(c.params.EpochMillis)),
		count,
		msg)
}

func (c *client) resetTimer(duration time.Duration) {

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

func (c *client) start() {

	readFunc := func() {
		defer func() {
			debug("[client %d] read goroutine exit\n", c.connID)
			c.notifyC <- struct{}{}
		}()

		decoder := json.NewDecoder(c.conn)

		for {
			msg := new(Message)

			if err := decoder.Decode(msg); err != nil {
				debug("[client %d] decode message error: %s\n", c.connID, err.Error())
				for state := atomic.LoadInt32(&c.connState); state < stateActiveClosed; {
					if state == stateActiveClosing &&
						atomic.CompareAndSwapInt32(&c.connState, state, stateActiveClosed) {
						break
					} else if state == statePassiveClosing &&
						atomic.CompareAndSwapInt32(&c.connState, state, statePassiveClosed) {
						break
					}
				}
				return
			} else {
				c.receiveMsgC <- msg
			}
		}
	}

	go readFunc()

	defer func() {
		debug("[client %d] message processing goroutine exit\n", c.connID)
		c.conn.Close()
		c.notifyC <- struct{}{}
	}()

	onReceiveData := func(msg *Message) {

		debug("[client %d] receive data: %s\n", c.connID, msg.String())

		if atomic.LoadInt32(&c.connState) >= stateActiveClosed {
			debug("[client %d] receive data after closed\n", c.connID)
			return
		}

		if msg.SeqNum == c.receivedDataSeq+1 {

			c.receivedDataSeq++

			c.send(NewAck(c.connID, msg.SeqNum))

			if msg.Type == msgClose {
				// current connection has closed my peer
				for state := atomic.LoadInt32(&c.connState); state < stateActiveClosed; {
					if atomic.CompareAndSwapInt32(&c.connState, state, statePassiveClosed) {
						close(c.closeC)
						break
					}
				}
				return
			}
			debug("[client %d] delivery message: %s\n", c.connID, msg.String())
			c.receivedMsgC <- msg

			if c.receiveBuf.Len() != 0 {
				toRemove := make([]*list.Element, 0)

				for elem := c.receiveBuf.Front(); elem != c.receiveBuf.Back(); elem = elem.Next() {
					m := elem.Value.(*Message)

					if m.SeqNum < c.receivedDataSeq+1 {
						toRemove = append(toRemove, elem)
					} else if m.SeqNum == c.receivedDataSeq+1 {
						toRemove = append(toRemove, elem)
						c.receivedDataSeq++
						c.send(NewAck(c.connID, m.SeqNum))

						if m.Type == msgClose {
							// current connection has closed my peer
							for state := atomic.LoadInt32(&c.connState); state < stateActiveClosed; {
								if atomic.CompareAndSwapInt32(&c.connState, state, statePassiveClosed) {
									close(c.closeC)
									break
								}
							}
							return
						}
						debug("[client %d] delivery message: %s\n", c.connID, m.String())
						c.receivedMsgC <- m
					} else {
						break
					}
				}

				for _, elem := range toRemove {
					c.receiveBuf.Remove(elem)
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
			c.send(NewAck(c.connID, c.receivedDataSeq))
		}
	}

	onReceiveAck := func(msg *Message) {

		debug("[client %d] receive ack: %s\n", c.connID, msg.String())
		seq := msg.SeqNum

		if seq >= c.lastAckSeq+1 {

			// message seq between [c.lastAckSeq+1,seq] has all received by peer
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
				debug("[client %d] send message: %s\n", c.connID, front.Value.(*Message).String())
				c.sendData(front.Value.(*Message), 0)
				c.pendingBuf.Remove(front)
				c.windowSize--
			}
		}

		state := atomic.LoadInt32(&c.connState)
		if state >= stateActiveClosing && c.pendingBuf.Len() == 0 && len(c.sendBuf) == 0 {
			debug("[client %d] closing, all pending messages have sent out\n", c.connID)

			for state := atomic.LoadInt32(&c.connState); state < stateActiveClosed; {
				if atomic.CompareAndSwapInt32(&c.connState, stateActiveClosing, stateActiveClosed) {
					break
				}
			}
		}
	}

	onSendData := func(msg *Message) {

		if c.windowSize > 0 && c.pendingBuf.Len() == 0 {
			debug("[client %d] send message: %s\n", c.connID, msg.String())
			c.sendData(msg, 0)
			c.windowSize--
		} else {
			debug("[client %d] pending message: %s\n", c.connID, msg.String())
			c.pendingBuf.PushBack(msg)
		}
	}

	onTimeout := func() {

		timeout := time.Millisecond * time.Duration(c.params.EpochMillis)
		now := time.Now()

		for _, t := range c.sendBuf {
			if t.expireTime.Before(now) {
				if t.epochCount > c.params.EpochLimit {
					// connection is assumed to be lost
					debug("[client %d] connection is lost\n", c.connID)
					for state := atomic.LoadInt32(&c.connState); state < stateActiveClosed; {
						if atomic.CompareAndSwapInt32(&c.connState, state, stateActiveClosed) {
							break
						}
					}
					return
				} else {
					debug("[client %d] %d-th resend message: %s\n", c.connID, t.epochCount+1, t.msg.String())
					c.sendData(t.msg, t.epochCount+1)
				}
			} else {
				// TODO(optimization: find the first time not before current time)
				next := t.expireTime.Sub(now)
				if next < timeout {
					timeout = next
				}
			}
		}

		c.send(NewAck(c.connID, c.receivedDataSeq))
		c.resetTimer(timeout)
	}

	c.resetTimer(time.Millisecond * time.Duration(c.params.EpochMillis))

	for {
		if atomic.LoadInt32(&c.connState) >= stateActiveClosed {
			break
		}

		select {
		case <-c.closeC:
			atomic.StoreInt32(&c.connState, stateActiveClosing)

			// send close message to peer
			c.write(nil, true)
		case <-c.timer.C:
			onTimeout()
			if atomic.LoadInt32(&c.connState) >= stateActiveClosed {
				return
			}
		case msg := <-c.receiveMsgC:
			switch msg.Type {
			case MsgAck:
				onReceiveAck(msg)
			case MsgConnect:
				debug("[client %d] receive connect after connection has established, ignore message\n", c.connID)
			case MsgData:
				onReceiveData(msg)
			case msgClose:
				// connection is closed by peer,
				debug("[client %d] connection is closed by peer\n", c.connID)
				for state := atomic.LoadInt32(&c.connState); state < stateActiveClosing; {
					if atomic.CompareAndSwapInt32(&c.connState, state, statePassiveClosing) {
						break
					}
				}
				onReceiveData(msg)
			}
		case msg := <-c.sendMsgC:
			//if atomic.LoadInt32(&c.connState) >= stateActiveClosing {
			//	continue
			//}
			onSendData(msg)
		}
	}
}

func (c *client) ConnID() int {
	return c.connID
}

func (c *client) Read() ([]byte, error) {

	select {
	case msg := <-c.receivedMsgC:
		debug("[client %d] Read() return message: %s\n", c.connID, msg.String())
		return msg.Payload, nil
	default:
		if state := atomic.LoadInt32(&c.connState); state >= stateActiveClosed {
			debug("[client %d] Read() return ErrClosed")
			return nil, ErrClosed
		}
	}

	select {
	case msg := <-c.receivedMsgC:
		debug("[client %d] Read() return message: %s\n", c.connID, msg.String())
		return msg.Payload, nil
	case <-c.closeC:
		debug("[client %d] Read() return ErrClosed")
		return nil, ErrClosed
	}
}

func (c *client) write(payload []byte, close bool) error {

	var msg *Message

	if close {
		c.seqNum++
		msg = &Message{Type: msgClose, ConnID: c.connID, SeqNum: c.seqNum}
	} else {
		if atomic.LoadInt32(&c.connState) >= stateActiveClosing {
			return ErrClosed
		}

		c.seqNum++
		msg = NewData(c.connID, c.seqNum, len(payload), payload)
	}
	c.sendMsgC <- msg
	return nil
}

func (c *client) Write(payload []byte) error {
	return c.write(payload, false)
}

func (c *client) Close() error {

	debug("[client %d] closing...\n", c.connID)

	state := atomic.LoadInt32(&c.connState)
	if state >= stateActiveClosing {
		return ErrClosed
	}

	atomic.StoreInt32(&c.connState, stateActiveClosing)

	c.closeC <- struct{}{}

	<-c.notifyC
	<-c.notifyC

	for state := atomic.LoadInt32(&c.connState); state < stateActiveClosed; {
		if atomic.CompareAndSwapInt32(&c.connState, state, stateActiveClosed) {
			close(c.closeC)
		}
	}

	debug("[client %d] closed\n", c.connID)
	return nil
}
