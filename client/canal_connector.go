package client

import (
	pb "github.com/WangJiemin/goCanal/protocol"
)

type CanalConnector interface {
	Connect()
	Disconnect()
	CheckValid()
	Subscribe()
	UnSubscribe()
	Get(arg ...interface{}) pb.Message
	GetWithoutAck(arg ...interface{}) pb.Message
	Ack(batchId int64)
	Rollback(arg ...interface{})
	StopRunning()
}
