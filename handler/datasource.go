package handler

import (
	"context"
	"time"

	"github.com/DataWorkbench/common/gormwrap"
	"github.com/DataWorkbench/gproto/pkg/model"
	"github.com/DataWorkbench/gproto/pkg/request"
	"github.com/DataWorkbench/gproto/pkg/response"
	"github.com/DataWorkbench/sourcemanager/executor/datasource"
	"gorm.io/gorm"
)

type DataSourceHandler struct{}

func (h *DataSourceHandler) CreateDataSource(ctx context.Context, input *request.CreateDataSource) (output *response.CreateDataSource, err error) {
	if err = h.checkDataSourceURL(input.Type, input.Url); err != nil {
		return
	}

	var id string
	id, err = datasourceIdGenerator.Take()
	if err != nil {
		return
	}

	err = gormwrap.ExecuteFuncWithTxn(ctx, dbConn, func(tx *gorm.DB) error {
		var subErr error
		// TODO: check quota.
		now := time.Now().Unix()
		info := &model.DataSource{
			SpaceId:        input.SpaceId,
			Id:             id,
			Name:           input.Name,
			Desc:           input.Desc,
			Type:           input.Type,
			Url:            input.Url,
			Status:         model.DataSource_Enabled,
			CreatedBy:      input.CreateBy,
			Created:        now,
			Updated:        now,
			LastConnection: nil,
		}
		if subErr = datasource.CreateDataSource(tx, info); subErr != nil {
			return subErr
		}

		if input.LastConnection != nil {
			input.LastConnection.SourceId = id
			if subErr = datasource.CreateConnection(tx, input.LastConnection); subErr != nil {
				return subErr
			}
		}
		return nil
	})

	output = &response.CreateDataSource{Id: id}
	return
}

func (h *DataSourceHandler) UpdateDataSource(ctx context.Context, input *request.UpdateDataSource) (output *model.EmptyStruct, err error) {
	if err = h.checkDataSourceURL(input.Type, input.Url); err != nil {
		return
	}

	err = gormwrap.ExecuteFuncWithTxn(ctx, dbConn, func(tx *gorm.DB) error {
		now := time.Now().Unix()
		info := &model.DataSource{
			SpaceId:        input.SpaceId,
			Id:             input.SourceId,
			Name:           input.Name,
			Desc:           input.Desc,
			Type:           input.Type,
			Url:            input.Url,
			Status:         model.DataSource_StatusUnset,
			CreatedBy:      "",
			Created:        0,
			Updated:        now,
			LastConnection: nil,
		}
		if subErr := datasource.UpdateDataSource(tx, info); subErr != nil {
			return subErr
		}
		return nil
	})
	output = &model.EmptyStruct{}
	return
}

func (h *DataSourceHandler) DescribeDataSource(ctx context.Context, input *request.DescribeDataSource) (output *response.DescribeDataSource, err error) {
	tx := dbConn.WithContext(ctx)
	var info *model.DataSource
	info, err = datasource.DescribeDataSource(tx, input.SourceId)
	if err != nil {
		return
	}
	output = &response.DescribeDataSource{
		Info: info,
	}
	return
}

func (h *DataSourceHandler) ListDataSources(ctx context.Context, input *request.ListDataSources) (output *response.ListDataSources, err error) {
	tx := dbConn.WithContext(ctx)
	output, err = datasource.ListDataSources(tx, input)
	if err != nil {
		return
	}
	verbose := input.Verbose

	if verbose > 0 {
		cache := make(map[string]*model.Network, 0)

		var lastConn *model.DataSourceConnection
		for _, info := range output.Infos {
			lastConn, err = datasource.DescribeLastConnection(tx, info.Id)
			if err != nil {
				return
			}
			if verbose-1 > 0 && lastConn != nil {
				networkInfo, ok := cache[lastConn.NetworkId]
				if !ok {
					var reply *response.DescribeNetwork
					reply, err = EngineClient.DescribeNetwork(ctx, &request.DescribeNetwork{
						NetworkId: lastConn.NetworkId,
					})
					if err != nil {
						return
					}
					networkInfo = reply.Info
					cache[lastConn.NetworkId] = networkInfo
				}
				lastConn.NetworkInfo = networkInfo
			}
			info.LastConnection = lastConn
		}
	}
	return
}

func (h *DataSourceHandler) DisableDataSources(ctx context.Context, input *request.DisableDataSources) (output *model.EmptyStruct, err error) {
	err = gormwrap.ExecuteFuncWithTxn(ctx, dbConn, func(tx *gorm.DB) error {
		return datasource.UpdateDataSourceStatus(tx, input.SourceIds, model.DataSource_Disabled)
	})
	if err != nil {
		return
	}
	output = &model.EmptyStruct{}
	return
}

func (h *DataSourceHandler) EnableDataSources(ctx context.Context, input *request.EnableDataSources) (output *model.EmptyStruct, err error) {
	err = gormwrap.ExecuteFuncWithTxn(ctx, dbConn, func(tx *gorm.DB) error {
		return datasource.UpdateDataSourceStatus(tx, input.SourceIds, model.DataSource_Enabled)
	})
	if err != nil {
		return
	}
	output = &model.EmptyStruct{}
	return
}

func (h *DataSourceHandler) DeleteDataSources(ctx context.Context, input *request.DeleteDataSources) (output *model.EmptyStruct, err error) {
	err = gormwrap.ExecuteFuncWithTxn(ctx, dbConn, func(tx *gorm.DB) error {
		subErr := datasource.DeleteDataSourceBySourceIds(tx, input.SourceIds)
		if subErr != nil {
			return subErr
		}
		subErr = datasource.DeleteConnectionBySourceIds(tx, input.SourceIds)
		if err != nil {
			return subErr
		}
		return nil
	})
	if err != nil {
		return
	}
	output = &model.EmptyStruct{}
	return
}
func (h *DataSourceHandler) DeleteDataSourcesBySpaceId(ctx context.Context, input *request.DeleteWorkspaces) (output *model.EmptyStruct, err error) {
	err = gormwrap.ExecuteFuncWithTxn(ctx, dbConn, func(tx *gorm.DB) error {
		subErr := datasource.DeleteDataSourceBySpaceIds(tx, input.SpaceIds)
		if subErr != nil {
			return subErr
		}
		subErr = datasource.DeleteConnectionBySpaceIds(tx, input.SpaceIds)
		if err != nil {
			return subErr
		}
		return nil
	})
	if err != nil {
		return
	}
	output = &model.EmptyStruct{}
	return
}

func (h *DataSourceHandler) DescribeDataSourceKinds(ctx context.Context, input *model.EmptyStruct) (output *response.DescribeDataSourceKinds, err error) {
	output = &response.DescribeDataSourceKinds{
		Kinds: datasource.SupportedSourceKinds,
	}
	return
}

func (h *DataSourceHandler) ListDataSourceConnections(ctx context.Context, input *request.ListDataSourceConnections) (output *response.ListDataSourceConnections, err error) {
	tx := dbConn.WithContext(ctx)
	output, err = datasource.ListDataSourceConnections(tx, input)
	if err != nil {
		return
	}
	if input.Verbose > 0 {
		cache := make(map[string]*model.Network, 0)
		for _, info := range output.Infos {
			networkInfo, ok := cache[info.NetworkId]
			if !ok {
				var reply *response.DescribeNetwork
				reply, err = EngineClient.DescribeNetwork(ctx, &request.DescribeNetwork{
					NetworkId: info.NetworkId,
				})
				if err != nil {
					return
				}
				networkInfo = reply.Info
				cache[info.NetworkId] = networkInfo
			}
			info.NetworkInfo = networkInfo
		}
	}
	return
}

func (h *DataSourceHandler) PingDataSourceConnection(ctx context.Context, input *request.PingDataSourceConnection) (output *response.PingDataSourceConnection, err error) {
	var sourceType model.DataSource_Type
	var sourceURL *model.DataSource_URL

	if input.Stage == request.PingDataSourceConnection_BeforeCreate {
		sourceType = input.Type
		sourceURL = input.Url
		if err = h.checkDataSourceURL(input.Type, input.Url); err != nil {
			return
		}
	} else {
		var info *model.DataSource
		info, err = datasource.DescribeDataSource(dbConn.WithContext(ctx), input.SourceId)
		if err != nil {
			return
		}
		sourceType = info.Type
		sourceURL = info.Url
	}

	var connInfo *model.DataSourceConnection
	connInfo, err = datasource.PingDataSourceConnection(ctx, sourceType, sourceURL)
	if err != nil {
		return
	}

	connInfo.SpaceId = input.SpaceId
	connInfo.NetworkId = input.NetworkId
	connInfo.SourceId = input.SourceId

	if input.Stage == request.PingDataSourceConnection_AfterCreate {
		err = gormwrap.ExecuteFuncWithTxn(ctx, dbConn, func(tx *gorm.DB) error {
			return datasource.CreateConnection(tx, connInfo)
		})
		if err != nil {
			return
		}
	} else {
		connInfo.SourceId = "som-0000000000000000" // Useless. only to avoids Validator waring.
	}

	output = &response.PingDataSourceConnection{
		Info: connInfo,
	}
	return
}

func (h *DataSourceHandler) checkDataSourceURL(sourceType model.DataSource_Type, sourceURL *model.DataSource_URL) (err error) {
	sourceURL.Type = sourceType
	return sourceURL.Validate()
}
