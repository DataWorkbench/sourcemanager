package server

import (
	"context"

	"github.com/DataWorkbench/gproto/pkg/smpb"

	"github.com/DataWorkbench/common/constants"
	"github.com/DataWorkbench/sourcemanager/executor"
)

// SourceManagerServer implements grpc server Interface wkspb.SourceManagerServer
type SourceManagerServer struct {
	smpb.UnimplementedSourcemanagerServer
	executor   *executor.SourcemanagerExecutor
	emptyReply *smpb.EmptyReply
}

func ToInfoReplay(s executor.SourcemanagerInfo) (p smpb.InfoReply) {
	p.Comment = s.Comment
	p.CreateTime = s.CreateTime
	p.Creator = s.Creator
	p.ID = s.ID
	p.Name = s.Name
	p.SourceType = s.SourceType
	p.SpaceID = s.SpaceID
	p.Url = string(s.Url)
	p.UpdateTime = s.UpdateTime
	p.EngineType = s.EngineType
	p.Direction = s.Direction
	return
}

func ToSotInfoReplay(s executor.SourceTablesInfo) (p smpb.SotInfoReply) {
	p.ID = s.ID
	p.SourceID = s.SourceID
	p.Name = s.Name
	p.Comment = s.Comment
	p.Url = string(s.Url)
	p.CreateTime = s.CreateTime
	p.UpdateTime = s.UpdateTime
	return
}

func CrToSmInfo(p *smpb.CreateRequest) (s executor.SourcemanagerInfo) {
	s.Comment = p.GetComment()
	s.Creator = p.GetCreator()
	s.Name = p.GetName()
	s.SourceType = p.GetSourceType()
	s.SpaceID = p.GetSpaceID()
	s.Url = constants.JSONString(p.GetUrl())
	s.ID = p.GetID()
	s.EngineType = p.GetEngineType()
	return
}

func CrToSotInfo(p *smpb.SotCreateRequest) (s executor.SourceTablesInfo) {
	s.ID = p.GetID()
	s.SourceID = p.GetSourceID()
	s.Name = p.GetName()
	s.Comment = p.GetComment()
	s.Url = constants.JSONString(p.GetUrl())
	return
}

// NewSourceManagerServer
func NewSourceManagerServer(executor *executor.SourcemanagerExecutor) *SourceManagerServer {
	return &SourceManagerServer{
		executor:   executor,
		emptyReply: &smpb.EmptyReply{},
	}
}

// SourceManager
func (s *SourceManagerServer) Create(ctx context.Context, req *smpb.CreateRequest) (*smpb.EmptyReply, error) {
	err := s.executor.Create(ctx, CrToSmInfo(req))
	return s.emptyReply, err
}

func (s *SourceManagerServer) Update(ctx context.Context, req *smpb.UpdateRequest) (*smpb.EmptyReply, error) {
	err := s.executor.Update(ctx, executor.SourcemanagerInfo{ID: req.GetID(), SourceType: req.GetSourceType(), Name: req.GetName(), Comment: req.GetComment(), Url: constants.JSONString(req.GetUrl())})
	return s.emptyReply, err
}

func (s *SourceManagerServer) Delete(ctx context.Context, req *smpb.DeleteRequest) (*smpb.EmptyReply, error) {
	err := s.executor.Delete(ctx, req.GetID(), true)
	return s.emptyReply, err
}

func (s *SourceManagerServer) List(ctx context.Context, req *smpb.ListsRequest) (*smpb.ListsReply, error) {
	total, infos, err := s.executor.Lists(ctx, req.GetLimit(), req.GetOffset(), req.GetSpaceID())
	if err != nil {
		return nil, err
	}

	reply := new(smpb.ListsReply)
	reply.Total = total
	reply.Infos = make([]*smpb.InfoReply, len(infos))
	for i := range infos {
		t := ToInfoReplay(*infos[i])
		if tmperr := s.executor.PingSource(ctx, t.SourceType, t.Url, t.EngineType); tmperr != nil {
			t.Connected = constants.SourceConnectedFailed
		} else {
			t.Connected = constants.SourceConnectedSuccess
		}
		reply.Infos[i] = &t
	}
	return reply, nil
}

func (s *SourceManagerServer) Describe(ctx context.Context, req *smpb.DescribeRequest) (*smpb.InfoReply, error) {
	info, err := s.executor.Describe(ctx, req.GetID(), true)
	if err != nil {
		return nil, err
	}
	reply := ToInfoReplay(info)

	if tmperr := s.executor.PingSource(ctx, reply.SourceType, reply.Url, reply.EngineType); tmperr != nil {
		reply.Connected = constants.SourceConnectedFailed
	} else {
		reply.Connected = constants.SourceConnectedSuccess
	}

	return &reply, nil
}

func (s *SourceManagerServer) PingSource(ctx context.Context, req *smpb.PingSourceRequest) (*smpb.EmptyReply, error) {
	err := s.executor.PingSource(ctx, req.GetSourceType(), req.GetUrl(), req.GetEngineType())
	return s.emptyReply, err
}

// Source Tables
func (s *SourceManagerServer) SotCreate(ctx context.Context, req *smpb.SotCreateRequest) (*smpb.EmptyReply, error) {
	err := s.executor.SotCreate(ctx, CrToSotInfo(req))
	return s.emptyReply, err
}

func (s *SourceManagerServer) SotUpdate(ctx context.Context, req *smpb.SotUpdateRequest) (*smpb.EmptyReply, error) {
	err := s.executor.SotUpdate(ctx, executor.SourceTablesInfo{ID: req.GetID(), Name: req.GetName(), Comment: req.GetComment(), Url: constants.JSONString(req.GetUrl())})
	return s.emptyReply, err
}

func (s *SourceManagerServer) SotDelete(ctx context.Context, req *smpb.SotDeleteRequest) (*smpb.EmptyReply, error) {
	err := s.executor.SotDelete(ctx, req.GetID())
	return s.emptyReply, err
}

func (s *SourceManagerServer) SotList(ctx context.Context, req *smpb.SotListsRequest) (*smpb.SotListsReply, error) {
	total, infos, err := s.executor.SotLists(ctx, req.GetSourceID(), req.GetLimit(), req.GetOffset())
	if err != nil {
		return nil, err
	}

	reply := new(smpb.SotListsReply)
	reply.Total = total
	reply.Infos = make([]*smpb.SotInfoReply, len(infos))
	for i := range infos {
		t := ToSotInfoReplay(*infos[i])
		reply.Infos[i] = &t
	}
	return reply, nil
}

func (s *SourceManagerServer) SotDescribe(ctx context.Context, req *smpb.SotDescribeRequest) (*smpb.SotInfoReply, error) {
	info, err := s.executor.SotDescribe(ctx, req.GetID())
	if err != nil {
		return nil, err
	}
	reply := ToSotInfoReplay(info)

	return &reply, nil
}

func (s *SourceManagerServer) EngineMap(ctx context.Context, req *smpb.EngingMapRequest) (*smpb.EngineMapReply, error) {
	info, err := s.executor.EngineMap(ctx, req.GetEngineType())
	if err != nil {
		return nil, err
	}

	reply := smpb.EngineMapReply{EngineType: info.EngineType, SourceType: info.SourceType}
	return &reply, nil
}

func (s *SourceManagerServer) DeleteAll(ctx context.Context, req *smpb.DeleteAllRequest) (*smpb.EmptyReply, error) {
	err := s.executor.DeleteAll(ctx, req.GetSpaceID())
	return s.emptyReply, err
}

func (s *SourceManagerServer) Enable(ctx context.Context, req *smpb.StateRequest) (*smpb.EmptyReply, error) {
	err := s.executor.ChangeSourceState(ctx, req.GetID(), constants.SourceEnableState)
	return s.emptyReply, err
}

func (s *SourceManagerServer) Disable(ctx context.Context, req *smpb.StateRequest) (*smpb.EmptyReply, error) {
	err := s.executor.ChangeSourceState(ctx, req.GetID(), constants.SourceDisableState)
	return s.emptyReply, err
}
