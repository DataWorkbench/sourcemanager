package server

import (
	"context"

	"github.com/DataWorkbench/common/constants"
	"github.com/DataWorkbench/gproto/pkg/model"
	"github.com/DataWorkbench/gproto/pkg/request"
	"github.com/DataWorkbench/gproto/pkg/response"
	"github.com/DataWorkbench/gproto/pkg/smpb"

	"github.com/DataWorkbench/sourcemanager/executor"
)

type SourceManagerServer struct {
	smpb.UnimplementedSourcemanagerServer
	executor *executor.SourcemanagerExecutor
}

func NewSourceManagerServer(executor *executor.SourcemanagerExecutor) *SourceManagerServer {
	return &SourceManagerServer{
		executor: executor,
	}
}

func (s *SourceManagerServer) Create(ctx context.Context, req *request.CreateSource) (*model.EmptyStruct, error) {
	err := s.executor.Create(ctx, req)
	return &model.EmptyStruct{}, err
}

func (s *SourceManagerServer) Describe(ctx context.Context, req *request.DescribeSource) (*response.DescribeSource, error) {
	var ret response.DescribeSource

	info, err := s.executor.Describe(ctx, req.GetSourceID(), true)
	if err != nil {
		return nil, err
	}

	if err = s.executor.PingSource(ctx, info.SourceType, info.Url); err != nil {
		ret.Connected = constants.SourceConnectedFailed
	} else {
		ret.Connected = constants.SourceConnectedSuccess
	}
	ret.Info = &info

	return &ret, nil
}

func (s *SourceManagerServer) Update(ctx context.Context, req *request.UpdateSource) (*model.EmptyStruct, error) {
	err := s.executor.Update(ctx, req)
	return &model.EmptyStruct{}, err
}

func (s *SourceManagerServer) PingSource(ctx context.Context, req *request.PingSource) (*model.EmptyStruct, error) {
	err := s.executor.PingSource(ctx, req.GetSourceType(), req.GetUrl())
	return &model.EmptyStruct{}, err
}

func (s *SourceManagerServer) Enable(ctx context.Context, req *request.EnableSource) (*model.EmptyStruct, error) {
	err := s.executor.ChangeSourceState(ctx, req.GetSourceIDs(), constants.SourceEnableState)
	return &model.EmptyStruct{}, err
}

func (s *SourceManagerServer) Disable(ctx context.Context, req *request.DisableSource) (*model.EmptyStruct, error) {
	err := s.executor.ChangeSourceState(ctx, req.GetSourceIDs(), constants.SourceDisableState)
	return &model.EmptyStruct{}, err
}

func (s *SourceManagerServer) SourceKind(ctx context.Context, req *model.EmptyStruct) (*response.SourceKind, error) {
	info, err := s.executor.SourceKind(ctx)
	if err != nil {
		return nil, err
	}

	return &info, nil
}

func (s *SourceManagerServer) DataFormat(ctx context.Context, req *model.EmptyStruct) (*response.JsonList, error) {
	info, err := s.executor.DataFormat(ctx)
	if err != nil {
		return nil, err
	}

	return &info, nil
}

func (s *SourceManagerServer) DataType(ctx context.Context, req *model.EmptyStruct) (*response.JsonList, error) {
	info, err := s.executor.DataType(ctx)
	if err != nil {
		return nil, err
	}

	return &info, nil
}

func (s *SourceManagerServer) CreateTable(ctx context.Context, req *request.CreateTable) (*model.EmptyStruct, error) {
	err := s.executor.CreateTable(ctx, req)
	return &model.EmptyStruct{}, err
}

func (s *SourceManagerServer) DescribeTable(ctx context.Context, req *request.DescribeTable) (*response.DescribeTable, error) {
	var ret response.DescribeTable

	info, err := s.executor.DescribeTable(ctx, req.GetTableID())
	if err != nil {
		return nil, err
	}
	ret.Info = &info

	return &ret, nil
}

func (s *SourceManagerServer) UpdateTable(ctx context.Context, req *request.UpdateTable) (*model.EmptyStruct, error) {
	err := s.executor.UpdateTable(ctx, req)
	return &model.EmptyStruct{}, err
}

func (s *SourceManagerServer) DeleteTable(ctx context.Context, req *request.DeleteTable) (*model.EmptyStruct, error) {
	err := s.executor.DeleteTable(ctx, req.GetTableIDs())
	return &model.EmptyStruct{}, err
}

func (s *SourceManagerServer) List(ctx context.Context, req *request.ListSource) (*response.ListSource, error) {
	resp, err := s.executor.List(ctx, req)
	if err != nil {
		return nil, err
	}

	return &resp, nil
}

func (s *SourceManagerServer) ListTable(ctx context.Context, req *request.ListTable) (*response.ListTable, error) {
	resp, err := s.executor.ListTable(ctx, req)
	if err != nil {
		return nil, err
	}

	return &resp, nil
}

func (s *SourceManagerServer) Delete(ctx context.Context, req *request.DeleteSource) (*model.EmptyStruct, error) {
	err := s.executor.Delete(ctx, req.GetSourceIDs(), true)
	return &model.EmptyStruct{}, err
}

func (s *SourceManagerServer) DeleteAll(ctx context.Context, req *request.DeleteAllSource) (*model.EmptyStruct, error) {
	err := s.executor.DeleteAll(ctx, req.GetSpaceIDs())
	return &model.EmptyStruct{}, err
}

func (s *SourceManagerServer) SourceTables(ctx context.Context, req *request.SourceTables) (*response.JsonList, error) {
	resp, err := s.executor.SourceTables(ctx, req)
	if err != nil {
		return nil, err
	}

	return &resp, nil
}

func (s *SourceManagerServer) TableColumns(ctx context.Context, req *request.TableColumns) (*response.TableColumns, error) {
	resp, err := s.executor.TableColumns(ctx, req)
	if err != nil {
		return nil, err
	}

	return &resp, nil
}
