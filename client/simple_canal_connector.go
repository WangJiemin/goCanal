package client

import (
	"bufio"
	"bytes"
	"encoding/binary"
	"errors"
	"fmt"
	"io"
	"net"
	"strconv"
	"sync"

	pb "github.com/WangJiemin/goCanal/protocol"
	pto "github.com/golang/protobuf/proto"
)

type SimpleCanalConnector struct {
	Address           string
	Port              int
	UserName          string
	PassWord          string
	SoTime            int32
	IdleTimeOut       int32
	ClientIdentity    pb.ClientIdentity
	Connected         bool
	Running           bool
	Filter            string
	RollbackOnConnect bool
	LazyParseEntry    bool
}

var (
	conn  net.Conn
	mutex sync.Mutex
)

// NewSimpleCanalConnector 创建SimpleCanalConnector实例
func NewSimpleCanalConnector(address string, port int, username string, password string, destination string, soTimeOut int32, idleTimeOut int32) (*SimpleCanalConnector) {
	s := &SimpleCanalConnector{
		Address:  address,
		Port:     port,
		UserName: username,
		PassWord: password,
		ClientIdentity: pb.ClientIdentity{
			Destination: destination,
			ClientId:    1001,
		},
		SoTime:            soTimeOut,
		IdleTimeOut:       idleTimeOut,
		RollbackOnConnect: true,
	}
	return s
}

// Connect 连接Canal-server
func (c *SimpleCanalConnector) Connect() (error) {
	if c.Connected {
		return nil
	}
	if c.Running {
		return nil
	}

	err := c.doConnect()
	if err != nil {
		return err
	}

	if c.Filter != "" {
		c.Subscribe(c.Filter)
	}

	if c.RollbackOnConnect {
		c.waitClientRunning()

		c.RollBack(0)
	}

	c.Connected = true
	return nil
}

// quitelyClose 优雅关闭
func quitelyClost() () {
	if conn != nil {
		conn.Close()
	}
}

// DisConnection 关闭连接
func (c *SimpleCanalConnector) DisConnection() () {
	if c.RollbackOnConnect && c.Connected == true {
		c.RollBack(0)
	}
	c.Connected = false
	quitelyClost()
}

// doConnect 去连接Canal-Server
func (c *SimpleCanalConnector) doConnect() (error) {
	address := c.Address + ":" + fmt.Sprintf("%d", c.Port)
	con, err := net.Dial("tcp", address)
	if err != nil {
		return err
	}
	conn = con

	p := new(pb.Packet)
	data, err := readNextPacket()
	if err != nil {
		return err
	}
	err = pto.Unmarshal(data, p)
	if err != nil {
		return err
	}
	if p != nil {
		if p.GetVersion() != 1 {
			panic("unsupported version at this client.")
		}
		if p.GetType() != pb.PacketType_HANDSHAKE {
			panic("expect handshake but found other type.")
		}

		handshake := &pb.Handshake{}
		err = pto.Unmarshal(p.GetBody(), handshake)
		if err != nil {
			return err
		}

		pas := []byte(c.PassWord)
		ca := &pb.ClientAuth{
			Username: c.UserName,
			Password: pas,
			NetReadTimeoutPresent: &pb.ClientAuth_NetReadTimeout{
				NetReadTimeout: c.IdleTimeOut,
			},
			NetWriteTimeoutPresent: &pb.ClientAuth_NetWriteTimeout{
				NetWriteTimeout: c.IdleTimeOut,
			},
		}
		caByteArray, _ := pto.Marshal(ca)
		packet := &pb.Packet{
			Type: pb.PacketType_CLIENTAUTHENTICATION,
			Body: caByteArray,
		}
		packArray, _ := pto.Marshal(packet)
		WriteWithHeader(packArray)

		pp, err := readNextPacket()
		if err != nil {
			return err
		}
		pk := &pb.Packet{}
		err = pto.Unmarshal(pp, pk)
		if err != nil {
			return err
		}

		if pk.Type != pb.PacketType_ACK {
			panic("unexpected packet type when ack is expected")
		}

		ackBody := &pb.Ack{}
		err = pto.Unmarshal(pk.GetBody(), ackBody)
		if err != nil {
			return err
		}
		if ackBody.GetErrorCode() > 0 {
			panic(errors.New(fmt.Sprintf("something goes wrong when doing authentication:%s", ackBody.GetErrorMessage())))
		}

		c.Connected = true
	}
	return nil
}

// GetWithOutAck 获取数据不Ack
func (c *SimpleCanalConnector) GetWithOutAck(batchSize int32, timeOut *int64, units *int32) (*pb.Message, error) {
	c.waitClientRunning()
	if !c.Running {
		return nil, nil
	}

	var size int32
	if batchSize < 0 {
		size = 1000
	} else {
		size = batchSize
	}

	var time *int64
	var t int64
	t = -1
	if timeOut == nil {
		time = &t
	} else {
		time = timeOut
	}

	var i int32
	i = -1
	if units == nil {
		units = &i
	}

	get := new(pb.Get)
	get.AutoAckPresent = &pb.Get_AutoAck{AutoAck: false}
	get.Destination = c.ClientIdentity.Destination
	get.ClientId = strconv.Itoa(c.ClientIdentity.ClientId)
	get.FetchSize = size
	get.TimeoutPresent = &pb.Get_Timeout{Timeout: *time}
	get.UnitPresent = &pb.Get_Unit{Unit: *units}

	getBody, err := pto.Marshal(get)
	if err != nil {
		return nil, err
	}

	packet := new(pb.Packet)
	packet.Type = pb.PacketType_GET
	packet.Body = getBody
	pa, err := pto.Marshal(packet)
	if err != nil {
		return nil, err
	}

	WriteWithHeader(pa)
	message, err := c.receiveMessages()
	if err != nil {
		return nil, err
	}
	return message, nil
}

// Get 获取数据并且Ack数据
func (c *SimpleCanalConnector) Get(batchSize int32, timeOut *int64, units *int32) (*pb.Message, error) {
	message, err := c.GetWithOutAck(batchSize, timeOut, units)
	if err != nil {
		return nil, err
	}

	c.Ack(message.Id)
	return message, nil
}

// UnSubscribe 取消订阅
func (c *SimpleCanalConnector) UnSubscribe() (error) {
	c.waitClientRunning()
	if c.Running {
		return nil
	}

	us := new(pb.Unsub)
	us.Destination = c.ClientIdentity.Destination
	us.ClientId = strconv.Itoa(c.ClientIdentity.ClientId)

	unSub, err := pto.Marshal(us)
	if err != nil {
		return err
	}

	pa := new(pb.Packet)
	pa.Type = pb.PacketType_UNSUBSCRIPTION
	pa.Body = unSub

	pack, err := pto.Marshal(pa)
	WriteWithHeader(pack)

	p, err := readNextPacket()
	if err != nil {
		return err
	}
	pa = nil
	err = pto.Unmarshal(p, pa)
	if err != nil {
		return err
	}
	ack := new(pb.Ack)
	err = pto.Unmarshal(pa.Body, ack)
	if err != nil {
		return err
	}
	if ack.GetErrorCode() > 0 {
		panic(errors.New(fmt.Sprintf("failed to unSubscribe with reason:%s", ack.GetErrorMessage())))
	}
	return nil
}

// receiveMessages 接收Canal-Server返回的消息体
func (c *SimpleCanalConnector) receiveMessages() (*pb.Message, error) {
	data, err := readNextPacket()
	if err != nil {
		return nil, err
	}

	p := new(pb.Packet)
	err = pto.Unmarshal(data, p)
	if err != nil {
		return nil, err
	}

	messages := new(pb.Messages)
	message := new(pb.Message)

	length := len(messages.Messages)
	message.Entries = make([]pb.Entry, length)
	ack := new(pb.Ack)

	var items []pb.Entry
	var entry pb.Entry
	switch p.Type {
	case pb.PacketType_MESSAGES:
		if !(p.GetCompression() == pb.Compression_NONE) {
			panic("compression is not supported in this connector")
		}
		err := pto.Unmarshal(p.Body, messages)
		if err != nil {
			return nil, err
		}
		if c.LazyParseEntry {
			message.RawEntries = messages.Messages
		} else {

			for _, value := range messages.Messages {
				err := pto.Unmarshal(value, &entry)
				if err != nil {
					return nil, err
				}
				items = append(items, entry)
			}
		}
		message.Entries = items
		message.Id = messages.GetBatchId()
		return message, nil

	case pb.PacketType_ACK:
		err := pto.Unmarshal(p.Body, ack)
		if err != nil {
			return nil, err
		}
		panic(errors.New(fmt.Sprintf("something goes wrong with reason:%s", ack.GetErrorMessage())))
	default:
		panic(errors.New(fmt.Sprintf("unexpected packet type:%s", p.Type)))

	}
}

// Ack Ack Canal-server的数据（某些逻辑操作后删除canal-server端的数据）
func (c *SimpleCanalConnector) Ack(batchId int64) (error) {
	c.waitClientRunning()
	if !c.Running {
		return nil
	}

	ca := new(pb.ClientAck)
	ca.Destination = c.ClientIdentity.Destination
	ca.ClientId = strconv.Itoa(c.ClientIdentity.ClientId)
	ca.BatchId = batchId

	clientAck, err := pto.Marshal(ca)
	if err != nil {
		return err
	}

	pa := new(pb.Packet)
	pa.Type = pb.PacketType_CLIENTACK
	pa.Body = clientAck
	pack, err := pto.Marshal(pa)
	if err != nil {
		return err
	}

	WriteWithHeader(pack)
	return nil
}

// RollBack 回滚操作
func (c *SimpleCanalConnector) RollBack(batchId int64) (error) {
	c.waitClientRunning()
	cb := new(pb.ClientRollback)
	cb.Destination = c.ClientIdentity.Destination
	cb.ClientId = strconv.Itoa(c.ClientIdentity.ClientId)
	cb.BatchId = batchId

	clientBollBack, err := pto.Marshal(cb)
	if err != nil {
		return err
	}

	pa := new(pb.Packet)
	pa.Type = pb.PacketType_CLIENTROLLBACK
	pa.Body = clientBollBack
	pack, err := pto.Marshal(pa)
	if err != nil {
		return err
	}

	WriteWithHeader(pack)
	return nil
}

// readHeaderLength 读取protobuf的header字节，该字节存取了你要读的package的长度
func readHeaderLength() int {
	buf := make([]byte, 4)
	conn.Read(buf)
	bytesBuffer := bytes.NewBuffer(buf)
	var x int32
	binary.Read(bytesBuffer, binary.BigEndian, &x)
	return int(x)
}

// readNextPacket 通过长度去读取数据包
func readNextPacket() ([]byte, error) {
	mutex.Lock()
	defer func() {
		mutex.Unlock()
	}()

	rdr := bufio.NewReader(conn)
	data := make([]byte, 0, 4*1024)
	n, err := io.ReadFull(rdr, data[:4])
	if err != nil {
		return nil, err
	}

	data = data[:n]
	dataLen := binary.BigEndian.Uint32(data)
	if uint64(dataLen) > uint64(cap(data)) {
		data = make([]byte, 0, dataLen)
	}

	n, err = io.ReadFull(rdr, data[:dataLen])
	if err != nil {
		return nil, err
	}

	data = data[:n]
	return data, nil
}

// WriteWithHeader 写数据包的header+body
func WriteWithHeader(body []byte) () {
	mutex.Lock()
	lenth := len(body)
	bytes := getWriteHeaderBytes(lenth)
	conn.Write(bytes)
	conn.Write(body)
	mutex.Unlock()
}

// getWriteHeaderBytes 获取要写数据的长度
func getWriteHeaderBytes(lenth int) ([]byte) {
	x := int32(lenth)
	bytesBuffer := bytes.NewBuffer([]byte{})
	binary.Write(bytesBuffer, binary.BigEndian, x)
	return bytesBuffer.Bytes()
}

// Subscribe 订阅
func (c *SimpleCanalConnector) Subscribe(filter string) (error) {
	c.waitClientRunning()
	if !c.Running {
		return nil
	}
	body, _ := pto.Marshal(&pb.Sub{Destination: c.ClientIdentity.Destination, ClientId: strconv.Itoa(c.ClientIdentity.ClientId), Filter: c.Filter})
	pack := new(pb.Packet)
	pack.Type = pb.PacketType_SUBSCRIPTION
	pack.Body = body

	packet, _ := pto.Marshal(pack)
	WriteWithHeader(packet)

	p := new(pb.Packet)

	paBytes, err := readNextPacket()
	if err != nil {
		return err
	}
	err = pto.Unmarshal(paBytes, p)
	if err != nil {
		return err
	}
	ack := new(pb.Ack)
	err = pto.Unmarshal(p.Body, ack)
	if err != nil {
		return err
	}

	if ack.GetErrorCode() > 0 {

		panic(errors.New(fmt.Sprintf("failed to subscribe with reason::%s", ack.GetErrorMessage())))
	}

	c.Filter = filter
	return nil

}

// waitClientRunning 等待客户端跑
func (c *SimpleCanalConnector) waitClientRunning() () {
	c.Running = true
}

