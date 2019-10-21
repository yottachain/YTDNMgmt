package YTDNMgmt

import (
	context "context"

	pb "github.com/yottachain/YTDNMgmt/pb"
	codes "google.golang.org/grpc/codes"
	status "google.golang.org/grpc/status"
)

// Server implemented server API for YTDNMgmt service.
type Server struct {
	NodeService *NodeDaoImpl
}

// NewNodeID implemented NewNodeID function of YTDNMgmtServer
func (server *Server) NewNodeID(ctx context.Context, req *pb.Empty) (*pb.Int32Msg, error) {
	id, err := server.NodeService.NewNodeID()
	if err != nil {
		return nil, status.Errorf(codes.Internal, err.Error())
	}
	return &pb.Int32Msg{Value: id}, nil
}

// PreRegisterNode implemented PreRegisterNode function of YTDNMgmtServer
func (server *Server) PreRegisterNode(ctx context.Context, req *pb.StringMsg) (*pb.Empty, error) {
	err := server.NodeService.PreRegisterNode(req.GetValue())
	if err != nil {
		return nil, status.Errorf(codes.Internal, err.Error())
	}
	return &pb.Empty{}, nil
}

// ChangeMinerPool implemented ChangeMinerPool function of YTDNMgmtServer
func (server *Server) ChangeMinerPool(ctx context.Context, req *pb.StringMsg) (*pb.Empty, error) {
	err := server.NodeService.ChangeMinerPool(req.GetValue())
	if err != nil {
		return nil, status.Errorf(codes.Internal, err.Error())
	}
	return &pb.Empty{}, nil
}

// RegisterNode implemented RegisterNode function of YTDNMgmtServer
func (server *Server) RegisterNode(ctx context.Context, req *pb.NodeMsg) (*pb.NodeMsg, error) {
	node := new(Node)
	node.Fillby(req)
	nodeRet, err := server.NodeService.RegisterNode(node)
	if err != nil {
		return nil, status.Errorf(codes.Internal, err.Error())
	}
	return nodeRet.Convert(), nil
}

// UpdateNodeStatus implemented UpdateNodeStatus function of YTDNMgmtServer
func (server *Server) UpdateNodeStatus(ctx context.Context, req *pb.NodeMsg) (*pb.NodeMsg, error) {
	node := new(Node)
	node.Fillby(req)
	nodeRet, err := server.NodeService.UpdateNodeStatus(node)
	if err != nil {
		return nil, status.Errorf(codes.Internal, err.Error())
	}
	return nodeRet.Convert(), nil
}

// IncrUsedSpace implemented IncrUsedSpace function of YTDNMgmtServer
func (server *Server) IncrUsedSpace(ctx context.Context, req *pb.IncrUsedSpaceReq) (*pb.Empty, error) {
	err := server.NodeService.IncrUsedSpace(req.GetId(), req.GetIncr())
	if err != nil {
		return nil, status.Errorf(codes.Internal, err.Error())
	}
	return &pb.Empty{}, nil
}

// AllocNodes implemented AllocNodes function of YTDNMgmtServer
func (server *Server) AllocNodes(ctx context.Context, req *pb.AllocNodesReq) (*pb.NodesResp, error) {
	nodes, err := server.NodeService.AllocNodes(req.ShardCount, req.ErrIDs)
	if err != nil {
		return nil, status.Errorf(codes.Internal, err.Error())
	}
	return &pb.NodesResp{Nodes: ConvertNodesToNodesMsg(nodes)}, nil
}

// SyncNode implemented SyncNode function of YTDNMgmtServer
func (server *Server) SyncNode(ctx context.Context, req *pb.NodeMsg) (*pb.Empty, error) {
	node := new(Node)
	node.Fillby(req)
	err := server.NodeService.SyncNode(node)
	if err != nil {
		return nil, status.Errorf(codes.Internal, err.Error())
	}
	return &pb.Empty{}, nil
}

// GetNodes implemented GetNodes function of YTDNMgmtServer
func (server *Server) GetNodes(ctx context.Context, req *pb.GetNodesReq) (*pb.NodesResp, error) {
	nodes, err := server.NodeService.GetNodes(req.GetNodeIDs())
	if err != nil {
		return nil, status.Errorf(codes.Internal, err.Error())
	}
	return &pb.NodesResp{Nodes: ConvertNodesToNodesMsg(nodes)}, nil
}

// GetSuperNodes implemented GetSuperNodes function of YTDNMgmtServer
func (server *Server) GetSuperNodes(ctx context.Context, req *pb.Empty) (*pb.SuperNodesResp, error) {
	superNodes, err := server.NodeService.GetSuperNodes()
	if err != nil {
		return nil, status.Errorf(codes.Internal, err.Error())
	}
	return &pb.SuperNodesResp{SuperNodes: ConvertSuperNodesToSuperNodesMsg(superNodes)}, nil
}

// GetSuperNodePrivateKey implemented GetSuperNodePrivateKey function of YTDNMgmtServer
func (server *Server) GetSuperNodePrivateKey(ctx context.Context, req *pb.Int32Msg) (*pb.StringMsg, error) {
	privateKey, err := server.NodeService.GetSuperNodePrivateKey(req.GetValue())
	if err != nil {
		return nil, status.Errorf(codes.Internal, err.Error())
	}
	return &pb.StringMsg{Value: privateKey}, nil
}

// GetNodeIDByPubKey implemented GetNodeIDByPubKey function of YTDNMgmtServer
func (server *Server) GetNodeIDByPubKey(ctx context.Context, req *pb.StringMsg) (*pb.Int32Msg, error) {
	id, err := server.NodeService.GetNodeIDByPubKey(req.GetValue())
	if err != nil {
		return nil, status.Errorf(codes.Internal, err.Error())
	}
	return &pb.Int32Msg{Value: id}, nil
}

// GetNodeByPubKey implemented GetNodeByPubKey function of YTDNMgmtServer
func (server *Server) GetNodeByPubKey(ctx context.Context, req *pb.StringMsg) (*pb.NodeMsg, error) {
	node, err := server.NodeService.GetNodeByPubKey(req.GetValue())
	if err != nil {
		return nil, status.Errorf(codes.Internal, err.Error())
	}
	return node.Convert(), nil
}

// GetSuperNodeIDByPubKey implemented GetSuperNodeIDByPubKey function of YTDNMgmtServer
func (server *Server) GetSuperNodeIDByPubKey(ctx context.Context, req *pb.StringMsg) (*pb.Int32Msg, error) {
	id, err := server.NodeService.GetSuperNodeIDByPubKey(req.GetValue())
	if err != nil {
		return nil, status.Errorf(codes.Internal, err.Error())
	}
	return &pb.Int32Msg{Value: id}, nil
}

// AddDNI implemented AddDNI function of YTDNMgmtServer
func (server *Server) AddDNI(ctx context.Context, req *pb.DNIReq) (*pb.Empty, error) {
	err := server.NodeService.AddDNI(req.GetId(), req.GetShard())
	if err != nil {
		return nil, status.Errorf(codes.Internal, err.Error())
	}
	return &pb.Empty{}, nil
}

// ActiveNodesList implemented ActiveNodesList function of YTDNMgmtServer
func (server *Server) ActiveNodesList(ctx context.Context, req *pb.Empty) (*pb.NodesResp, error) {
	nodes, err := server.NodeService.ActiveNodesList()
	if err != nil {
		return nil, status.Errorf(codes.Internal, err.Error())
	}
	return &pb.NodesResp{Nodes: ConvertNodesToNodesMsg(nodes)}, nil
}

// Statistics implemented Statistics function of YTDNMgmtServer
func (server *Server) Statistics(ctx context.Context, req *pb.Empty) (*pb.NodeStatMsg, error) {
	nodeStat, err := server.NodeService.Statistics()
	if err != nil {
		return nil, status.Errorf(codes.Internal, err.Error())
	}
	return nodeStat.Convert(), nil
}

// GetSpotCheckList implemented GetSpotCheckList function of YTDNMgmtServer
func (server *Server) GetSpotCheckList(ctx context.Context, req *pb.Empty) (*pb.GetSpotCheckListResp, error) {
	spotCheckLists, err := server.NodeService.GetSpotCheckList()
	if err != nil {
		return nil, status.Errorf(codes.Internal, err.Error())
	}
	return &pb.GetSpotCheckListResp{SpotCheckLists: ConvertSpotCheckListsToSpotCheckListsMsg(spotCheckLists)}, nil
}

// GetSTNode implemented GetSTNode function of YTDNMgmtServer
func (server *Server) GetSTNode(ctx context.Context, req *pb.Empty) (*pb.NodeMsg, error) {
	node, err := server.NodeService.GetSTNode()
	if err != nil {
		return nil, status.Errorf(codes.Internal, err.Error())
	}
	return node.Convert(), nil
}

// GetSTNodes implemented GetSTNodes function of YTDNMgmtServer
func (server *Server) GetSTNodes(ctx context.Context, req *pb.Int64Msg) (*pb.NodesResp, error) {
	nodes, err := server.NodeService.GetSTNodes(req.GetValue())
	if err != nil {
		return nil, status.Errorf(codes.Internal, err.Error())
	}
	return &pb.NodesResp{Nodes: ConvertNodesToNodesMsg(nodes)}, nil
}

// UpdateTaskStatus implemented UpdateTaskStatus function of YTDNMgmtServer
func (server *Server) UpdateTaskStatus(ctx context.Context, req *pb.UpdateTaskStatusReq) (*pb.Empty, error) {
	err := server.NodeService.UpdateTaskStatus(req.GetId(), req.GetInvalidNodeList())
	if err != nil {
		return nil, status.Errorf(codes.Internal, err.Error())
	}
	return &pb.Empty{}, nil
}

// GetInvalidNodes implemented GetInvalidNodes function of YTDNMgmtServer
func (server *Server) GetInvalidNodes(ctx context.Context, req *pb.Empty) (*pb.GetInvalidNodesResp, error) {
	shardCounts, err := server.NodeService.GetInvalidNodes()
	if err != nil {
		return nil, status.Errorf(codes.Internal, err.Error())
	}
	return &pb.GetInvalidNodesResp{ShardCounts: ConvertShardCountsToShardCountsMsg(shardCounts)}, nil
}

// GetRebuildItem implemented GetRebuildItem function of YTDNMgmtServer
func (server *Server) GetRebuildItem(ctx context.Context, req *pb.GetRebuildItemReq) (*pb.GetRebuildItemResp, error) {
	node, shards, err := server.NodeService.GetRebuildItem(req.GetMinerID(), req.GetIndex(), req.GetTotal())
	if err != nil {
		return nil, status.Errorf(codes.Internal, err.Error())
	}
	vnis := make([][]byte, len(shards))
	for i, s := range shards {
		vnis[i] = s.Data
	}
	return &pb.GetRebuildItemResp{Node: node.Convert(), Shards: vnis}, nil
}

// GetRebuildNode implemented GetRebuildNode function of YTDNMgmtServer
func (server *Server) GetRebuildNode(ctx context.Context, req *pb.Int64Msg) (*pb.NodeMsg, error) {
	node, err := server.NodeService.GetRebuildNode(req.GetValue())
	if err != nil {
		return nil, status.Errorf(codes.Internal, err.Error())
	}
	return node.Convert(), nil
}

// DeleteDNI implemented DeleteDNI function of YTDNMgmtServer
func (server *Server) DeleteDNI(ctx context.Context, req *pb.DNIReq) (*pb.Empty, error) {
	err := server.NodeService.DeleteDNI(req.GetId(), req.GetShard())
	if err != nil {
		return nil, status.Errorf(codes.Internal, err.Error())
	}
	return &pb.Empty{}, nil
}
