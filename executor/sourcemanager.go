package executor

import (
	"context"
	"encoding/json"
	"errors"
	"fmt"
	"time"

	"strings"

	"github.com/DataWorkbench/common/constants"
	"github.com/DataWorkbench/common/qerror"
	"github.com/DataWorkbench/common/utils/idgenerator"
	"github.com/DataWorkbench/glog"
	"github.com/DataWorkbench/gproto/pkg/datasourcepb"
	"github.com/DataWorkbench/gproto/pkg/flinkpb"
	"github.com/DataWorkbench/gproto/pkg/model"
	"github.com/DataWorkbench/gproto/pkg/request"
	"github.com/DataWorkbench/gproto/pkg/response"
	"gorm.io/gorm"
	"gorm.io/gorm/clause"
)

const (
	SourceTableName      = "source_manager"
	SourceUtileTableName = "source_utile"
	TableName            = "source_tables"
	MAXRows              = 100000000
)

type SourceUtileInfo struct {
	EngineType string `gorm:"column:engine_type;"`
	DataType   string `gorm:"column:data_type;"`
	DataFormat string `gorm:"column:data_format;"`
	SourceKind string `gorm:"column:source_kind;"`
}

type SourcemanagerExecutor struct {
	db               *gorm.DB
	idGenerator      *idgenerator.IDGenerator
	idGeneratorTable *idgenerator.IDGenerator
	logger           *glog.Logger
	engineClient     EngineClient
}

func NewSourceManagerExecutor(db *gorm.DB, l *glog.Logger, eClient EngineClient) *SourcemanagerExecutor {
	ex := &SourcemanagerExecutor{
		db:               db,
		idGenerator:      idgenerator.New(constants.SourceManagerIDPrefix),
		idGeneratorTable: idgenerator.New(constants.SourceTablesIDPrefix),
		logger:           l,
		engineClient:     eClient,
	}
	return ex
}

func (ex *SourcemanagerExecutor) GetSourceNetwork(url *datasourcepb.DataSourceURL, sourcetype model.DataSource_Type) (network *datasourcepb.DatasourceNetwork, err error) {
	if sourcetype == model.DataSource_MySQL {
		network = url.GetMysql().GetNetwork()
	} else if sourcetype == model.DataSource_PostgreSQL {
		network = url.GetPostgresql().GetNetwork()
	} else if sourcetype == model.DataSource_Kafka {
		network = url.GetKafka().GetNetwork()
	} else if sourcetype == model.DataSource_S3 {
		//network = url.GetS3().GetNetwork()
		ex.logger.Error().Error("not support source type", fmt.Errorf("unknow source type %s", sourcetype)).Fire()
		err = qerror.NotSupportSourceType.Format(sourcetype)
	} else if sourcetype == model.DataSource_ClickHouse {
		network = url.GetClickhouse().GetNetwork()
	} else if sourcetype == model.DataSource_HBase {
		network = url.GetHbase().GetNetwork()
	} else if sourcetype == model.DataSource_Ftp {
		network = url.GetFtp().GetNetwork()
	} else if sourcetype == model.DataSource_HDFS {
		network = url.GetHdfs().GetNetwork()
	} else {
		ex.logger.Error().Error("not support source type", fmt.Errorf("unknow source type %s", sourcetype)).Fire()
		err = qerror.NotSupportSourceType.Format(sourcetype)
		return
	}

	return
}

func (ex *SourcemanagerExecutor) checkSourceInfo(url *datasourcepb.DataSourceURL, sourcetype model.DataSource_Type) (err error) {
	if sourcetype == model.DataSource_MySQL {
		if err = url.GetMysql().Validate(); err != nil {
			ex.logger.Error().Error("check source mysql url", err).Fire()
			err = qerror.InvalidJSON
			return
		}
	} else if sourcetype == model.DataSource_PostgreSQL {
		if err = url.GetPostgresql().Validate(); err != nil {
			ex.logger.Error().Error("check source postgresql url", err).Fire()
			err = qerror.InvalidJSON
			return
		}
	} else if sourcetype == model.DataSource_Kafka {
		if err = url.GetKafka().Validate(); err != nil {
			ex.logger.Error().Error("check source kafka url", err).Fire()
			err = qerror.InvalidJSON
			return
		}
	} else if sourcetype == model.DataSource_S3 {
		if err = url.GetS3().Validate(); err != nil {
			ex.logger.Error().Error("check source s3 url", err).Fire()
			err = qerror.InvalidJSON
			return
		}
	} else if sourcetype == model.DataSource_ClickHouse {
		if err = url.GetClickhouse().Validate(); err != nil {
			ex.logger.Error().Error("check source clickhouse url", err).Fire()
			err = qerror.InvalidJSON
			return
		}
	} else if sourcetype == model.DataSource_HBase {
		if err = url.GetHbase().Validate(); err != nil {
			ex.logger.Error().Error("check source hbase url", err).Fire()
			err = qerror.InvalidJSON
			return
		}
	} else if sourcetype == model.DataSource_Ftp {
		if err = url.GetFtp().Validate(); err != nil {
			ex.logger.Error().Error("check source ftp url", err).Fire()
			err = qerror.InvalidJSON
			return
		}
	} else if sourcetype == model.DataSource_HDFS {
		if err = url.GetHdfs().Validate(); err != nil {
			ex.logger.Error().Error("check source clickhouse url", err).Fire()
			err = qerror.InvalidJSON
			return
		}
	} else {
		ex.logger.Error().Error("not support source type", fmt.Errorf("unknow source type %s", sourcetype)).Fire()
		err = qerror.NotSupportSourceType.Format(sourcetype)
		return
	}

	return nil
}

func (ex *SourcemanagerExecutor) CheckName(ctx context.Context, spaceid string, name string, table string) (err error) {
	var x string

	if len(strings.Split(name, ".")) != 1 {
		ex.logger.Error().Error("invalid name", fmt.Errorf("can't use '.' in name")).Fire()
		err = qerror.InvalidSourceName
		return
	}

	db := ex.db.WithContext(ctx)
	err = db.Table(table).Select("space_id").Where("space_id = ? AND name = ?", spaceid, name).Take(&x).Error
	if err == nil {
		err = qerror.ResourceAlreadyExists
		return
	} else if errors.Is(err, gorm.ErrRecordNotFound) {
		err = nil
		return
	}

	return
}

// SourceManager
func (ex *SourcemanagerExecutor) Create(ctx context.Context, req *request.CreateSource) (err error) {
	var info model.DataSource

	info.SourceId = req.GetSourceId()
	if info.SourceId == "" {
		info.SourceId, _ = ex.idGenerator.Take()
	}
	info.SpaceId = req.GetSpaceId()
	info.SourceType = req.GetSourceType()
	info.Name = req.GetName()
	info.Comment = req.GetComment()
	info.Url = req.GetUrl()
	info.Status = model.DataSource_Enabled
	info.Created = time.Now().Unix()
	info.Updated = info.Created
	if tmperr := ex.PingSource(ctx, info.SourceType, info.Url); tmperr != nil {
		info.Connection = model.DataSource_Failed
	} else {
		info.Connection = model.DataSource_Success
	}

	if err = ex.checkSourceInfo(info.Url, info.SourceType); err != nil {
		return
	}

	if err = ex.CheckName(ctx, info.SpaceId, info.Name, SourceTableName); err != nil {
		return
	}

	db := ex.db.WithContext(ctx)
	err = db.Table(SourceTableName).Select("source_id", "space_id", "source_type", "name", "comment", "url", "created", "updated", "status", "connection").Create(&info).Error
	if err != nil {
		ex.logger.Error().Error("create source", err).Fire()
		err = qerror.Internal
	}
	return
}

func (ex *SourcemanagerExecutor) CheckSourceState(ctx context.Context, sourceID string) (err error) {
	descInfo, _ := ex.Describe(ctx, sourceID, false)
	if descInfo.Status == model.DataSource_Disabled {
		err = qerror.SourceIsDisable
		return
	}
	return
}

func (ex *SourcemanagerExecutor) Describe(ctx context.Context, sourceID string, checkState bool) (info model.DataSource, err error) {
	db := ex.db.WithContext(ctx)

	if checkState == true {
		if err = ex.CheckSourceState(ctx, sourceID); err != nil {
			return
		}
	}

	err = db.Table(SourceTableName).Where("source_id = ? ", sourceID).Scan(&info).Error
	if err != nil {
		ex.logger.Error().Error("describe source", err).Fire()
		err = qerror.Internal
		return
	}

	if info.SourceType != model.DataSource_S3 {
		network, _ := ex.GetSourceNetwork(info.Url, info.SourceType)
		if network.GetType() == datasourcepb.DatasourceNetwork_Vpc {
			var resp *response.DescribeNetwork
			resp, err = ex.engineClient.client.DescribeNetwork(ctx, &request.DescribeNetwork{NetworkId: network.GetVpcNetwork().GetNetworkId()})
			info.NetworkName = resp.Info.GetName()
		}
	}

	return
}

func (ex *SourcemanagerExecutor) Update(ctx context.Context, req *request.UpdateSource) (err error) {
	var info model.DataSource

	info.SourceId = req.GetSourceId()
	info.SourceType = req.GetSourceType()
	info.Name = req.GetName()
	info.Comment = req.GetComment()
	info.Url = req.GetUrl()
	info.Updated = time.Now().Unix()

	if err = ex.CheckSourceState(ctx, info.SourceId); err != nil {
		return
	}

	if err = ex.checkSourceInfo(info.Url, info.SourceType); err != nil {
		return
	}

	descInfo, _ := ex.Describe(ctx, info.SourceId, false)

	if err = ex.CheckName(ctx, info.SpaceId, info.Name, SourceTableName); err != nil {
		if err == qerror.InvalidSourceName {
			return
		}

		if err == qerror.ResourceAlreadyExists && descInfo.Name != info.Name {
			return
		}
		// it is self.
		err = nil
	}
	if tmperr := ex.PingSource(ctx, info.SourceType, info.Url); tmperr != nil {
		info.Connection = model.DataSource_Failed
	} else {
		info.Connection = model.DataSource_Success
	}

	db := ex.db.WithContext(ctx)
	err = db.Table(SourceTableName).Select("source_type", "name", "comment", "url", "updated", "connection").Where("source_id = ? ", info.SourceId).Updates(info).Error
	if err != nil {
		ex.logger.Error().Error("update source", err).Fire()
		err = qerror.Internal
	}
	return
}

func (ex *SourcemanagerExecutor) ChangeSourceState(ctx context.Context, sourceIDs []string, state model.DataSource_Status) (err error) {
	if len(sourceIDs) == 0 {
		return
	}

	db := ex.db.WithContext(ctx)
	tx := db.Begin().WithContext(ctx)
	if err = tx.Error; err != nil {
		return
	}
	defer func() {
		if err == nil {
			err = tx.Commit().Error
		}
		if err != nil {
			tx.Rollback()
		}
	}()

	eqExpr := make([]clause.Expression, len(sourceIDs))
	for i := 0; i < len(sourceIDs); i++ {
		eqExpr[i] = clause.Eq{Column: "source_id", Value: sourceIDs[i]}
	}
	err = tx.Table(SourceTableName).Clauses(clause.Where{
		Exprs: []clause.Expression{
			clause.Or(eqExpr...),
		},
	}).Updates(map[string]interface{}{"status": state, "updated": time.Now().Unix()}).Error
	if err != nil {
		return
	}

	return
}

func (ex *SourcemanagerExecutor) Delete(ctx context.Context, sourceIDs []string, checkState bool) (err error) {
	for _, id := range sourceIDs {
		var lt request.ListTable

		lt.Limit = MAXRows
		lt.SourceId = id

		/* if status is disable. allow delete the source.
		if checkState == true {
			if err = ex.CheckSourceState(ctx, id); err != nil {
				return
			}
		}
		*/

		resp, _ := ex.ListTable(ctx, &lt)
		if resp.Total > 0 {
			err = qerror.ResourceIsUsing
			return
		}

		eqExpr := make([]clause.Expression, len(sourceIDs))
		for i := 0; i < len(sourceIDs); i++ {
			eqExpr[i] = clause.Eq{Column: "source_id", Value: sourceIDs[i]}
		}
		var expr clause.Expression
		if len(eqExpr) == 1 {
			expr = eqExpr[0]
		} else {
			expr = clause.Or(eqExpr...)
		}

		currentTime := time.Now().Unix()
		if err = ex.db.WithContext(ctx).Table(SourceTableName).Clauses(clause.Where{
			Exprs: []clause.Expression{
				clause.Neq{Column: "status", Value: model.DataSource_Deleted},
				expr,
			},
		}).Updates(map[string]interface{}{"status": model.DataSource_Deleted, "updated": currentTime, "deleted": currentTime}).Error; err != nil {
			return
		}
		//db := ex.db.WithContext(ctx)
		//err = db.Table(SourceTableName).Where("source_id = ? ", id).Delete("").Error
		//if err != nil {
		//	ex.logger.Error().Error("delete source", err).Fire()
		//	err = qerror.Internal
		//}
	}
	return
}

func (ex *SourcemanagerExecutor) DeleteAll(ctx context.Context, spaceIDs []string) (err error) {
	if len(spaceIDs) == 0 {
		return
	}

	eqExpr := make([]clause.Expression, len(spaceIDs))
	for i := 0; i < len(spaceIDs); i++ {
		eqExpr[i] = clause.Eq{Column: "space_id", Value: spaceIDs[i]}
	}
	var expr clause.Expression
	if len(eqExpr) == 1 {
		expr = eqExpr[0]
	} else {
		expr = clause.Or(eqExpr...)
	}
	currentTime := time.Now().Unix()
	tx := ex.db.WithContext(ctx).Begin()
	defer func() {
		if err == nil {
			err = tx.Commit().Error
		} else {
			tx.Rollback()
		}
	}()
	if err = tx.Table(TableName).Clauses(clause.Where{
		Exprs: []clause.Expression{
			clause.Neq{Column: "status", Value: model.TableInfo_Deleted},
			expr,
		},
	}).Updates(map[string]interface{}{"status": model.TableInfo_Deleted, "updated": currentTime, "deleted": currentTime}).Error; err != nil {
		return
	}
	if err = tx.Table(SourceTableName).Clauses(clause.Where{
		Exprs: []clause.Expression{
			clause.Neq{Column: "status", Value: model.TableInfo_Deleted},
			expr,
		},
	}).Updates(map[string]interface{}{"status": model.DataSource_Deleted, "updated": currentTime, "deleted": currentTime}).Error; err != nil {
		return
	}

	//eqExpr := make([]clause.Expression, len(spaceIDs))
	//for i := 0; i < len(spaceIDs); i++ {
	//	eqExpr[i] = clause.Eq{Column: "space_id", Value: spaceIDs[i]}
	//}
	//
	//db := ex.db.WithContext(ctx)
	//err = db.Table(TableName).
	//	Clauses(clause.Where{Exprs: []clause.Expression{clause.Or(eqExpr...)}}).
	//	Delete("").Error
	//if err != nil {
	//	return
	//}
	//
	//err = db.Table(SourceTableName).
	//	Clauses(clause.Where{Exprs: []clause.Expression{clause.Or(eqExpr...)}}).
	//	Delete("").Error
	//if err != nil {
	//	return
	//}
	return
}

func (ex *SourcemanagerExecutor) SourceKind(ctx context.Context) (ret response.SourceKind, err error) {
	var x string

	db := ex.db.WithContext(ctx)
	err = db.Table(SourceUtileTableName).Select("source_kind").Where("engine_type = ?", "flink").Take(&x).Error
	if err != nil {
		ex.logger.Error().Error("get source kind", err).Fire()
		err = qerror.Internal
		return
	}

	if err = json.Unmarshal([]byte(x), &ret); err != nil {
		ex.logger.Error().Error("json sourcekind to struct failed", err).Fire()
		err = qerror.InvalidJSON
		return
	}

	return
}

func (ex *SourcemanagerExecutor) DataFormat(ctx context.Context) (ret response.JsonList, err error) {
	var x string

	db := ex.db.WithContext(ctx)
	err = db.Table(SourceUtileTableName).Select("data_format").Where("engine_type = ?", "flink").Take(&x).Error
	if err != nil {
		ex.logger.Error().Error("get data_format", err).Fire()
		err = qerror.Internal
		return
	}

	if err = json.Unmarshal([]byte(x), &ret); err != nil {
		ex.logger.Error().Error("json dataformat to struct failed", err).Fire()
		err = qerror.InvalidJSON
		return
	}

	return
}
func (ex *SourcemanagerExecutor) DataType(ctx context.Context) (ret response.JsonList, err error) {
	var x string

	db := ex.db.WithContext(ctx)
	err = db.Table(SourceUtileTableName).Select("data_type").Where("engine_type = ?", "flink").Take(&x).Error
	if err != nil {
		ex.logger.Error().Error("get datatype", err).Fire()
		err = qerror.Internal
		return
	}

	if err = json.Unmarshal([]byte(x), &ret); err != nil {
		ex.logger.Error().Error("json datatype to struct failed", err).Fire()
		err = qerror.InvalidJSON
		return
	}

	return
}

func (ex *SourcemanagerExecutor) checkSourcetablesUrl(ctx context.Context, url *flinkpb.TableSchema, sourceid string) (err error) {
	descInfo, _ := ex.Describe(ctx, sourceid, false)

	sourcetype := descInfo.SourceType
	if sourcetype == model.DataSource_MySQL {
		if err = url.GetMysql().Validate(); err != nil {
			ex.logger.Error().Error("check source mysql define url", err).Fire()
			err = qerror.InvalidJSON
			return
		}
	} else if sourcetype == model.DataSource_PostgreSQL {
		if err = url.GetPostgresql().Validate(); err != nil {
			ex.logger.Error().Error("check source postgres define url", err).Fire()
			err = qerror.InvalidJSON
			return
		}
	} else if sourcetype == model.DataSource_Kafka {
		if err = url.GetKafka().Validate(); err != nil {
			ex.logger.Error().Error("check source kafka define url", err).Fire()
			err = qerror.InvalidJSON
			return err
		}
	} else if sourcetype == model.DataSource_S3 {
		if err = url.GetS3().Validate(); err != nil {
			ex.logger.Error().Error("check source s3 define url", err).Fire()
			err = qerror.InvalidJSON
			return err
		}
	} else if sourcetype == model.DataSource_ClickHouse {
		if err = url.GetClickhouse().Validate(); err != nil {
			ex.logger.Error().Error("check source clickhouse define url", err).Fire()
			err = qerror.InvalidJSON
			return err
		}
	} else if sourcetype == model.DataSource_HBase {
		if err = url.GetHbase().Validate(); err != nil {
			ex.logger.Error().Error("check source hbase define url", err).Fire()
			err = qerror.InvalidJSON
			return err
		}
	} else if sourcetype == model.DataSource_Ftp {
		if err = url.GetFtp().Validate(); err != nil {
			ex.logger.Error().Error("check source ftp define url", err).Fire()
			err = qerror.InvalidJSON
			return err
		}
	} else if sourcetype == model.DataSource_HDFS {
		if err = url.GetHdfs().Validate(); err != nil {
			ex.logger.Error().Error("check source clickhouse define url", err).Fire()
			err = qerror.InvalidJSON
			return err
		}
	} else {
		ex.logger.Error().Error("not support source type", fmt.Errorf("unknow source type %s", sourcetype)).Fire()
		err = qerror.NotSupportSourceType.Format(sourcetype)
		return
	}

	return nil
}

func (ex *SourcemanagerExecutor) CreateTable(ctx context.Context, req *request.CreateTable) (err error) {
	var info model.TableInfo

	info.TableId = req.GetTableId()
	if info.TableId == "" {
		info.TableId, _ = ex.idGeneratorTable.Take()
	}
	info.SourceId = req.GetSourceId()
	info.SpaceId = req.GetSpaceId()
	info.Name = req.GetName()
	info.Comment = req.GetComment()
	info.TableSchema = req.GetTableSchema()
	info.TableKind = req.GetTableKind()
	info.Created = time.Now().Unix()
	info.Updated = info.Created
	info.Status = model.TableInfo_Enabled

	if err = ex.CheckSourceState(ctx, info.SourceId); err != nil {
		return
	}

	if err = ex.checkSourcetablesUrl(ctx, info.TableSchema, info.SourceId); err != nil {
		return
	}

	if err = ex.CheckName(ctx, info.SpaceId, info.Name, TableName); err != nil {
		return
	}

	db := ex.db.WithContext(ctx)
	err = db.Table(TableName).Select("table_id", "source_id", "space_id", "name", "comment", "table_schema", "created", "updated", "table_kind", "status").Create(&info).Error
	if err != nil {
		ex.logger.Error().Error("create source table", err).Fire()
		err = qerror.Internal
	}
	return
}

func (ex *SourcemanagerExecutor) DescribeTable(ctx context.Context, tableID string) (info model.TableInfo, err error) {
	var sourceInfo model.DataSource
	db := ex.db.WithContext(ctx)

	err = db.Table(TableName).Where("table_id = ? ", tableID).Scan(&info).Error
	if err != nil {
		ex.logger.Error().Error("describe source table", err).Fire()
		err = qerror.Internal
	}

	//check source disable state.
	sourceInfo, err = ex.Describe(ctx, info.SourceId, true)
	if err != nil {
		return
	}
	info.SourceName = sourceInfo.Name
	info.Connection = sourceInfo.Connection
	return
}

func (ex *SourcemanagerExecutor) UpdateTable(ctx context.Context, req *request.UpdateTable) (err error) {
	var (
		info     model.TableInfo
		selfInfo model.TableInfo
	)

	info.TableId = req.GetTableId()
	info.Name = req.GetName()
	info.Comment = req.GetComment()
	info.TableSchema = req.GetTableSchema()
	info.TableKind = req.GetTableKind()
	info.Updated = time.Now().Unix()

	// DescribeTable will Check source State
	selfInfo, err = ex.DescribeTable(ctx, info.TableId)
	if err != nil {
		return
	}

	if err = ex.checkSourcetablesUrl(ctx, info.TableSchema, selfInfo.SourceId); err != nil {
		return
	}

	if err = ex.CheckName(ctx, selfInfo.SpaceId, info.Name, TableName); err != nil {
		if err == qerror.InvalidSourceName {
			return
		}

		if err == qerror.ResourceAlreadyExists && selfInfo.Name != info.Name {
			return
		}
		// it is self.
		err = nil
	}

	db := ex.db.WithContext(ctx)
	err = db.Table(TableName).Select("table_kind", "name", "comment", "url", "updated").Where("table_id = ? ", info.TableId).Updates(info).Error
	if err != nil {
		ex.logger.Error().Error("update source table", err).Fire()
		err = qerror.Internal
	}
	return
}

func (ex *SourcemanagerExecutor) DeleteTable(ctx context.Context, tableIDs []string) (err error) {
	//for _, id := range tableIDs {
	//	//DescribeTable will check source disable state
	//	_, err = ex.DescribeTable(ctx, id)
	//	if err != nil {
	//		return
	//	}
	//
	//	db := ex.db.WithContext(ctx)
	//	err = db.Table(TableName).Where("table_id = ? ", id).Delete("").Error
	//	if err != nil {
	//		ex.logger.Error().Error("delete source table", err).Fire()
	//		err = qerror.Internal
	//	}
	//}
	if len(tableIDs) == 0 {
		return qerror.InvalidParams.Format("tableIDs")
	}
	eqExpr := make([]clause.Expression, len(tableIDs))
	for i := 0; i < len(tableIDs); i++ {
		eqExpr[i] = clause.Eq{Column: "table_id", Value: tableIDs[i]}
	}
	var expr clause.Expression
	if len(eqExpr) == 1 {
		expr = eqExpr[0]
	} else {
		expr = clause.Or(eqExpr...)
	}
	currentTime := time.Now().Unix()
	if err = ex.db.Where(ctx).Table(TableName).Clauses(clause.Where{
		Exprs: []clause.Expression{
			clause.Neq{Column: "status", Value: model.TableInfo_Deleted},
			expr,
		},
	}).Updates(map[string]interface{}{"status": model.TableInfo_Deleted, "updated": currentTime, "deleted": currentTime}).Error; err != nil {
		ex.logger.Error().Error("delete source table", err).Fire()
		return qerror.Internal
	}
	return
}

var validListSortBy = map[string]bool{
	"source_id": true,
	"table_id":  true,
	"name":      true,
	"created":   true,
	"updated":   true,
}

func checkList(SortBy string) error {
	if SortBy != "" {
		fields := strings.Split(SortBy, ",")
		for _, field := range fields {
			if !validListSortBy[field] {
				return qerror.InvalidParams.Format(field)
			}
		}
	}

	return nil
}

func (ex *SourcemanagerExecutor) List(ctx context.Context, input *request.ListSource) (resp response.ListSource, err error) {
	if err = checkList(input.GetSortBy()); err != nil {
		return
	}

	order := input.SortBy
	if order == "" {
		order = "updated"
	}
	if input.Reverse {
		order += " DESC"
	} else {
		order += " ASC"
	}

	// Build where exprs.
	exprs := []clause.Expression{clause.Eq{Column: "space_id", Value: input.SpaceId}}
	if len(input.Search) > 0 {
		exprs = append(exprs, clause.Like{
			Column: "name",
			Value:  "%" + input.Search + "%",
		})
	}
	exprs = append(exprs, clause.Neq{Column: "status", Value: model.DataSource_Deleted})

	db := ex.db.WithContext(ctx)
	err = db.Table(SourceTableName).Select("*").Clauses(clause.Where{Exprs: exprs}).
		Limit(int(input.Limit)).Offset(int(input.Offset)).Order(order).
		Scan(&resp.Infos).Error
	if err != nil {
		return
	}

	for index := range resp.Infos {
		if resp.Infos[index].SourceType != model.DataSource_S3 {
			network, _ := ex.GetSourceNetwork(resp.Infos[index].Url, resp.Infos[index].SourceType)
			if network.GetType() == datasourcepb.DatasourceNetwork_Vpc {
				var resp_network *response.DescribeNetwork
				resp_network, err = ex.engineClient.client.DescribeNetwork(ctx, &request.DescribeNetwork{NetworkId: network.GetVpcNetwork().GetNetworkId()})
				resp.Infos[index].NetworkName = resp_network.Info.GetName()
			}
		}
	}

	err = db.Table(SourceTableName).Select("count(*)").Clauses(clause.Where{Exprs: exprs}).Count(&resp.Total).Error
	if err != nil {
		return
	}
	return
}

func (ex *SourcemanagerExecutor) ListTable(ctx context.Context, input *request.ListTable) (resp response.ListTable, err error) {
	if err = checkList(input.GetSortBy()); err != nil {
		return
	}

	order := input.SortBy
	if order == "" {
		order = "updated"
	}
	if input.Reverse {
		order += " DESC"
	} else {
		order += " ASC"
	}

	// Build where exprs.
	exprs := []clause.Expression{}
	if len(input.SpaceId) > 0 {
		exprs = append(exprs, clause.Eq{
			Column: "space_id",
			Value:  input.SpaceId,
		})
	}
	if len(input.SourceId) > 0 {
		exprs = append(exprs, clause.Eq{
			Column: "source_id",
			Value:  input.SourceId,
		})
	}
	if input.TableKind > model.TableInfo__ {
		exprs = append(exprs, clause.Eq{
			Column: "table_kind",
			Value:  input.TableKind,
		})
	}
	if len(input.Search) > 0 {
		exprs = append(exprs, clause.Like{
			Column: "name",
			Value:  "%" + input.Search + "%",
		})
	}
	exprs = append(exprs, clause.Neq{
		Column: "status",
		Value:  model.TableInfo_Deleted,
	})

	db := ex.db.WithContext(ctx)
	err = db.Table(TableName).Select("*").Clauses(clause.Where{Exprs: exprs}).
		Limit(int(input.Limit)).Offset(int(input.Offset)).Order(order).
		Scan(&resp.Infos).Error
	if err != nil {
		return
	}

	for index := range resp.Infos {
		//don't check disable state
		sourceInfo, _ := ex.Describe(ctx, resp.Infos[index].SourceId, false)
		resp.Infos[index].SourceName = sourceInfo.Name
		resp.Infos[index].Connection = sourceInfo.Connection
	}

	err = db.Table(TableName).Select("count(*)").Clauses(clause.Where{Exprs: exprs}).Count(&resp.Total).Error
	if err != nil {
		return
	}
	return
}

func (ex *SourcemanagerExecutor) SourceTables(ctx context.Context, req *request.SourceTables) (resp response.JsonList, err error) {
	sourceInfo, _ := ex.Describe(ctx, req.GetSourceId(), true)
	if err != nil {
		return
	}
	if sourceInfo.SourceType == model.DataSource_PostgreSQL {
		resp, err = GetPostgreSQLSourceTables(sourceInfo.GetUrl().GetPostgresql())
	} else if sourceInfo.SourceType == model.DataSource_MySQL {
		resp, err = GetMysqlSourceTables(sourceInfo.GetUrl().GetMysql())
	} else if sourceInfo.SourceType == model.DataSource_ClickHouse {
		resp, err = GetClickHouseSourceTables(sourceInfo.GetUrl().GetClickhouse())
	} else {
		err = qerror.NotSupportSourceType.Format(sourceInfo.SourceType)
	}

	return
}

func (ex *SourcemanagerExecutor) TableColumns(ctx context.Context, req *request.TableColumns) (resp response.TableColumns, err error) {
	sourceInfo, _ := ex.Describe(ctx, req.GetSourceId(), true)
	if err != nil {
		return
	}
	if sourceInfo.SourceType == model.DataSource_PostgreSQL {
		resp, err = GetPostgreSQLSourceTableColumns(sourceInfo.GetUrl().GetPostgresql(), req.GetTableName())
	} else if sourceInfo.SourceType == model.DataSource_MySQL {
		resp, err = GetMysqlSourceTableColumns(sourceInfo.GetUrl().GetMysql(), req.GetTableName())
	} else if sourceInfo.SourceType == model.DataSource_ClickHouse {
		resp, err = GetClickHouseSourceTableColumns(sourceInfo.GetUrl().GetClickhouse(), req.GetTableName())
	} else {
		err = qerror.NotSupportSourceType.Format(sourceInfo.SourceType)
	}

	return
}
