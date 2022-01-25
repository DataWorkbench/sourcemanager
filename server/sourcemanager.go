package server

import (
	"context"

	"github.com/DataWorkbench/gproto/pkg/model"
	"github.com/DataWorkbench/gproto/pkg/request"
	"github.com/DataWorkbench/gproto/pkg/response"
	"github.com/DataWorkbench/gproto/pkg/smpb"
	"github.com/DataWorkbench/sourcemanager/handler"

	"github.com/DataWorkbench/sourcemanager/executor"
)

type SourceManagerServer struct {
	smpb.UnimplementedSourcemanagerServer
	executor *executor.SourcemanagerExecutor
	h        *handler.DataSourceHandler
}

func NewSourceManagerServer(executor *executor.SourcemanagerExecutor) *SourceManagerServer {
	return &SourceManagerServer{
		executor: executor,
		h:        &handler.DataSourceHandler{},
	}
}

func (s *SourceManagerServer) CreateDataSource(ctx context.Context, req *request.CreateDataSource) (*response.CreateDataSource, error) {
	return s.h.CreateDataSource(ctx, req)
}
func (s *SourceManagerServer) UpdateDataSource(ctx context.Context, req *request.UpdateDataSource) (*model.EmptyStruct, error) {
	return s.h.UpdateDataSource(ctx, req)
}
func (s *SourceManagerServer) DescribeDataSource(ctx context.Context, req *request.DescribeDataSource) (*response.DescribeDataSource, error) {
	return s.h.DescribeDataSource(ctx, req)
}
func (s *SourceManagerServer) ListDataSources(ctx context.Context, req *request.ListDataSources) (*response.ListDataSources, error) {
	return s.h.ListDataSources(ctx, req)
}
func (s *SourceManagerServer) DisableDataSources(ctx context.Context, req *request.DisableDataSources) (*model.EmptyStruct, error) {
	return s.h.DisableDataSources(ctx, req)
}
func (s *SourceManagerServer) EnableDataSources(ctx context.Context, req *request.EnableDataSources) (*model.EmptyStruct, error) {
	return s.h.EnableDataSources(ctx, req)
}
func (s *SourceManagerServer) DeleteDataSources(ctx context.Context, req *request.DeleteDataSources) (*model.EmptyStruct, error) {
	return s.h.DeleteDataSources(ctx, req)
}
func (s *SourceManagerServer) DeleteDataSourcesBySpaceId(ctx context.Context, req *request.DeleteWorkspaces) (*model.EmptyStruct, error) {
	return s.h.DeleteDataSourcesBySpaceId(ctx, req)
}

func (s *SourceManagerServer) DescribeDataSourceKinds(ctx context.Context, req *model.EmptyStruct) (*response.DescribeDataSourceKinds, error) {
	return s.h.DescribeDataSourceKinds(ctx, req)
}

func (s *SourceManagerServer) PingDataSourceConnection(ctx context.Context, req *request.PingDataSourceConnection) (*response.PingDataSourceConnection, error) {
	return s.h.PingDataSourceConnection(ctx, req)
}

func (s *SourceManagerServer) ListDataSourceConnections(ctx context.Context, req *request.ListDataSourceConnections) (*response.ListDataSourceConnections, error) {
	return s.h.ListDataSourceConnections(ctx, req)
}

//func (s *SourceManagerServer) Create(ctx context.Context, req *request.CreateSource) (*model.EmptyStruct, error) {
//	err := s.executor.Create(ctx, req)
//	return &model.EmptyStruct{}, err
//}

//func (s *SourceManagerServer) Describe(ctx context.Context, req *request.DescribeSource) (*response.DescribeSource, error) {
//	var ret response.DescribeSource
//
//	info, err := s.executor.Describe(ctx, req.GetSourceId(), true)
//	if err != nil {
//		return nil, err
//	}
//	ret.Info = &info
//
//	return &ret, nil
//}
//
//func (s *SourceManagerServer) Update(ctx context.Context, req *request.UpdateSource) (*model.EmptyStruct, error) {
//	err := s.executor.Update(ctx, req)
//	return &model.EmptyStruct{}, err
//}
//
//func (s *SourceManagerServer) Enable(ctx context.Context, req *request.EnableSource) (*model.EmptyStruct, error) {
//	err := s.executor.ChangeSourceState(ctx, req.GetSourceIds(), model.DataSource_Enabled)
//	return &model.EmptyStruct{}, err
//}
//
//func (s *SourceManagerServer) Disable(ctx context.Context, req *request.DisableSource) (*model.EmptyStruct, error) {
//	err := s.executor.ChangeSourceState(ctx, req.GetSourceIds(), model.DataSource_Disabled)
//	return &model.EmptyStruct{}, err
//}

//func (s *SourceManagerServer) List(ctx context.Context, req *request.ListSource) (*response.ListSource, error) {
//	resp, err := s.executor.List(ctx, req)
//	if err != nil {
//		return nil, err
//	}
//
//	return &resp, nil
//}
//
//func (s *SourceManagerServer) Delete(ctx context.Context, req *request.DeleteSource) (*model.EmptyStruct, error) {
//	err := s.executor.Delete(ctx, req.GetSourceIds(), true)
//	return &model.EmptyStruct{}, err
//}
//
//func (s *SourceManagerServer) DeleteAll(ctx context.Context, req *request.DeleteWorkspaces) (*model.EmptyStruct, error) {
//	err := s.executor.DeleteAll(ctx, req.GetSpaceIds())
//	return &model.EmptyStruct{}, err
//}

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

	info, err := s.executor.DescribeTable(ctx, req.GetTableId())
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
	err := s.executor.DeleteTable(ctx, req.GetTableIds())
	return &model.EmptyStruct{}, err
}

func (s *SourceManagerServer) ListTable(ctx context.Context, req *request.ListTable) (*response.ListTable, error) {
	resp, err := s.executor.ListTable(ctx, req)
	if err != nil {
		return nil, err
	}

	return &resp, nil
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
