package broker

import (
	"context"
	"net"
	"time"

	"github.com/eclipse/paho.mqtt.golang/packets"
	pb "github.com/fhmq/hmq/grpc"
	"go.uber.org/zap"
	"google.golang.org/grpc"
	"google.golang.org/grpc/keepalive"
	"google.golang.org/grpc/reflection"
)

var (
	rpcClient = make(map[string]pb.HMQServiceClient)
)

func initRPCService() {
	lis, err := net.Listen("tcp", ":10011")
	if err != nil {
		log.Error("failed to listen: ", zap.Error(err))
		return
	}

	s := grpc.NewServer(grpc.KeepaliveParams(keepalive.ServerParameters{
		Time: 30 * time.Minute,
	}))
	pb.RegisterHMQServiceServer(s, &HMQ{})
	reflection.Register(s)
	if err := s.Serve(lis); err != nil {
		log.Error("failed to serve: ", zap.Error(err))
	}
}

func initRPCClient(url string) {
	conn, err := grpc.Dial(url,
		grpc.WithInsecure(),
		grpc.WithKeepaliveParams(keepalive.ClientParameters{ // avoid 'code = Unavailable desc = transport is closing' error
			Time: 30 * time.Minute,
		}))
	if err != nil {
		log.Error("create connect rpc service failed", zap.String("url", url), zap.Error(err))
	}

	cli := pb.NewHMQServiceClient(conn)
	rpcClient[url] = cli
}

type HMQ struct {
}

func (h *HMQ) QuerySubscribe(ctx context.Context, in *pb.QuerySubscribeRequest) (*pb.Response, error) {
	return nil, nil
}

func (h *HMQ) QueryConnect(ctx context.Context, in *pb.QueryConnectRequest) (*pb.Response, error) {
	return nil, nil
}

func (h *HMQ) DeliverMessage(ctx context.Context, in *pb.DeliverMessageRequest) (*pb.Response, error) {
	return nil, nil
}

func (b *Broker) DeliverMessage(packet *packets.PublishPacket) {
	//TODO Query and Deliver Message
}
