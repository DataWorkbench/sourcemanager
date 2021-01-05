package server

import (
	"context"

	"github.com/DataWorkbench/gproto/pkg/smpb"

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
	p.Url = s.Url
	p.UpdateTime = s.UpdateTime
	p.EngineType = s.EngineType
	return
}

func ToSotInfoReplay(s executor.SourceTablesInfo) (p smpb.SotInfoReply) {
	p.ID = s.ID
	p.SourceID = s.SourceID
	p.TabType = s.TabType
	p.Name = s.Name
	p.Comment = s.Comment
	p.Url = s.Url
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
	s.Url = p.GetUrl()
	s.ID = p.GetID()
	s.EngineType = p.GetEngineType()
	return
}

func CrToSotInfo(p *smpb.SotCreateRequest) (s executor.SourceTablesInfo) {
	s.ID = p.GetID()
	s.SourceID = p.GetSourceID()
	s.Name = p.GetName()
	s.Comment = p.GetComment()
	s.Url = p.GetUrl()
	s.TabType = p.GetTabType()
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
	err := s.executor.Update(ctx, executor.SourcemanagerInfo{ID: req.GetID(), SourceType: req.GetSourceType(), Name: req.GetName(), Comment: req.GetComment(), Url: req.GetUrl()})
	return s.emptyReply, err
}

func (s *SourceManagerServer) Delete(ctx context.Context, req *smpb.DeleteRequest) (*smpb.EmptyReply, error) {
	err := s.executor.Delete(ctx, req.GetID())
	return s.emptyReply, err
}

func (s *SourceManagerServer) List(ctx context.Context, req *smpb.ListsRequest) (*smpb.ListsReply, error) {
	infos, err := s.executor.Lists(ctx, req.GetLimit(), req.GetOffset(), req.GetSpaceID())
	if err != nil {
		return nil, err
	}

	reply := new(smpb.ListsReply)
	reply.Infos = make([]*smpb.InfoReply, len(infos))
	for i := range infos {
		t := ToInfoReplay(*infos[i])
		reply.Infos[i] = &t
	}
	return reply, nil
}

func (s *SourceManagerServer) Describe(ctx context.Context, req *smpb.DescribeRequest) (*smpb.InfoReply, error) {
	info, err := s.executor.Describe(ctx, req.GetID())
	if err != nil {
		return nil, err
	}
	reply := ToInfoReplay(info)

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
	err := s.executor.SotUpdate(ctx, executor.SourceTablesInfo{ID: req.GetID(), Name: req.GetName(), Comment: req.GetComment(), Url: req.GetUrl(), TabType: req.GetTabType()})
	return s.emptyReply, err
}

func (s *SourceManagerServer) SotDelete(ctx context.Context, req *smpb.SotDeleteRequest) (*smpb.EmptyReply, error) {
	err := s.executor.SotDelete(ctx, req.GetID())
	return s.emptyReply, err
}

func (s *SourceManagerServer) SotList(ctx context.Context, req *smpb.SotListsRequest) (*smpb.SotListsReply, error) {
	infos, err := s.executor.SotLists(ctx, req.GetSourceID(), req.GetLimit(), req.GetOffset())
	if err != nil {
		return nil, err
	}

	reply := new(smpb.SotListsReply)
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
