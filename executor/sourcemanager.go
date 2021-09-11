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
	"github.com/DataWorkbench/gproto/pkg/model"
	"github.com/DataWorkbench/gproto/pkg/request"
	"github.com/DataWorkbench/gproto/pkg/response"
	"gorm.io/gorm"
	"gorm.io/gorm/clause"
)

const (
	SourceTableName      = "sourcemanager"
	SourceUtileTableName = "sourceutile"
	TableName            = "sourcetables"
	MAXRows              = 100000000
)

type SourceUtileInfo struct {
	EngineType string `gorm:"column:enginetype;"`
	DataType   string `gorm:"column:datatype;"`
	DataFormat string `gorm:"column:dataformat;"`
	SourceKind string `gorm:"column:sourcekind;"`
}

type SourcemanagerExecutor struct {
	db               *gorm.DB
	idGenerator      *idgenerator.IDGenerator
	idGeneratorTable *idgenerator.IDGenerator
	logger           *glog.Logger
}

func NewSourceManagerExecutor(db *gorm.DB, l *glog.Logger) *SourcemanagerExecutor {
	ex := &SourcemanagerExecutor{
		db:               db,
		idGenerator:      idgenerator.New(constants.SourceManagerIDPrefix),
		idGeneratorTable: idgenerator.New(constants.SourceTablesIDPrefix),
		logger:           l,
	}
	return ex
}

func (ex *SourcemanagerExecutor) checkSourceInfo(url *model.SourceUrl, sourcetype string) (err error) {
	if sourcetype == constants.SourceTypeMysql {
		if err = url.GetMySQL().Validate(); err != nil {
			ex.logger.Error().Error("check source mysql url", err).Fire()
			err = qerror.InvalidJSON
			return
		}
	} else if sourcetype == constants.SourceTypePostgreSQL {
		if err = url.GetPostgreSQL().Validate(); err != nil {
			ex.logger.Error().Error("check source postgresql url", err).Fire()
			err = qerror.InvalidJSON
			return
		}
	} else if sourcetype == constants.SourceTypeKafka {
		if err = url.GetKafka().Validate(); err != nil {
			ex.logger.Error().Error("check source kafka url", err).Fire()
			err = qerror.InvalidJSON
			return
		}
	} else if sourcetype == constants.SourceTypeS3 {
		if err = url.GetS3().Validate(); err != nil {
			ex.logger.Error().Error("check source s3 url", err).Fire()
			err = qerror.InvalidJSON
			return
		}
	} else if sourcetype == constants.SourceTypeClickHouse {
		if err = url.GetClickHouse().Validate(); err != nil {
			ex.logger.Error().Error("check source clickhouse url", err).Fire()
			err = qerror.InvalidJSON
			return
		}
	} else if sourcetype == constants.SourceTypeHbase {
		if err = url.GetHbase().Validate(); err != nil {
			ex.logger.Error().Error("check source hbase url", err).Fire()
			err = qerror.InvalidJSON
			return
		}
	} else if sourcetype == constants.SourceTypeFtp {
		if err = url.GetFtp().Validate(); err != nil {
			ex.logger.Error().Error("check source ftp url", err).Fire()
			err = qerror.InvalidJSON
			return
		}
	} else if sourcetype == constants.SourceTypeHDFS {
		if err = url.GetHDFS().Validate(); err != nil {
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
	err = db.Table(table).Select("spaceid").Where("spaceid = ? AND name = ?", spaceid, name).Take(&x).Error
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
	var info model.SourceInfo

	info.SourceID = req.GetSourceID()
	if info.SourceID == "" {
		info.SourceID, _ = ex.idGenerator.Take()
	}
	info.SpaceID = req.GetSpaceID()
	info.SourceType = req.GetSourceType()
	info.Name = req.GetName()
	info.Comment = req.GetComment()
	info.Url = req.GetUrl()
	info.State = constants.SourceEnableState
	info.CreateTime = time.Now().Unix()
	info.UpdateTime = info.CreateTime

	if err = ex.checkSourceInfo(info.Url, info.SourceType); err != nil {
		return
	}

	if err = ex.CheckName(ctx, info.SpaceID, info.Name, SourceTableName); err != nil {
		return
	}

	db := ex.db.WithContext(ctx)
	err = db.Table(SourceTableName).Create(info).Error
	if err != nil {
		ex.logger.Error().Error("create source", err).Fire()
		err = qerror.Internal
	}
	return
}

func (ex *SourcemanagerExecutor) CheckSourceState(ctx context.Context, sourceID string) (err error) {
	descInfo, _ := ex.Describe(ctx, sourceID, false)
	if descInfo.State == constants.SourceDisableState {
		err = qerror.SourceIsDisable
		return
	}
	return
}

func (ex *SourcemanagerExecutor) Describe(ctx context.Context, sourceID string, checkState bool) (info model.SourceInfo, err error) {
	db := ex.db.WithContext(ctx)

	if checkState == true {
		if err = ex.CheckSourceState(ctx, sourceID); err != nil {
			return
		}
	}

	err = db.Table(SourceTableName).Where("sourceid = ? ", sourceID).Scan(&info).Error
	if err != nil {
		ex.logger.Error().Error("describe source", err).Fire()
		err = qerror.Internal
		return
	}

	return
}

func (ex *SourcemanagerExecutor) Update(ctx context.Context, req *request.UpdateSource) (err error) {
	var info model.SourceInfo

	info.SourceID = req.GetSourceID()
	info.SourceType = req.GetSourceType()
	info.Name = req.GetName()
	info.Comment = req.GetComment()
	info.Url = req.GetUrl()
	info.UpdateTime = time.Now().Unix()

	if err = ex.CheckSourceState(ctx, info.SourceID); err != nil {
		return
	}

	if err = ex.checkSourceInfo(info.Url, info.SourceType); err != nil {
		return
	}

	descInfo, _ := ex.Describe(ctx, info.SourceID, false)

	if err = ex.CheckName(ctx, info.SpaceID, info.Name, SourceTableName); err != nil {
		if err == qerror.InvalidSourceName {
			return
		}

		if err == qerror.ResourceAlreadyExists && descInfo.Name != info.Name {
			return
		}
		// it is self.
		err = nil
	}

	db := ex.db.WithContext(ctx)
	err = db.Table(SourceTableName).Select("sourcetype", "name", "comment", "url", "updatetime").Where("sourceid = ? ", info.SourceID).Updates(info).Error
	if err != nil {
		ex.logger.Error().Error("update source", err).Fire()
		err = qerror.Internal
	}
	return
}

func (ex *SourcemanagerExecutor) ChangeSourceState(ctx context.Context, sourceIDs []string, state string) (err error) {
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
		eqExpr[i] = clause.Eq{Column: "sourceid", Value: sourceIDs[i]}
	}
	err = tx.Table(SourceTableName).Clauses(clause.Where{
		Exprs: []clause.Expression{
			clause.Or(eqExpr...),
		},
	}).Updates(map[string]interface{}{"state": state, "updatetime": time.Now().Unix()}).Error
	if err != nil {
		return
	}

	return
}

func (ex *SourcemanagerExecutor) Delete(ctx context.Context, sourceIDs []string, checkState bool) (err error) {
	for _, id := range sourceIDs {
		var lt request.ListTable

		lt.Limit = MAXRows
		lt.SourceID = id

		if checkState == true {
			if err = ex.CheckSourceState(ctx, id); err != nil {
				return
			}
		}

		resp, _ := ex.ListTable(ctx, &lt)
		if resp.Total > 0 {
			err = qerror.ResourceIsUsing
			return
		}

		db := ex.db.WithContext(ctx)
		err = db.Table(SourceTableName).Where("sourceid = ? ", id).Delete("").Error
		if err != nil {
			ex.logger.Error().Error("delete source", err).Fire()
			err = qerror.Internal
		}
	}
	return
}

func (ex *SourcemanagerExecutor) DeleteAll(ctx context.Context, spaceIDs []string) (err error) {
	if len(spaceIDs) == 0 {
		return
	}

	eqExpr := make([]clause.Expression, len(spaceIDs))
	for i := 0; i < len(spaceIDs); i++ {
		eqExpr[i] = clause.Eq{Column: "spaceid", Value: spaceIDs[i]}
	}

	db := ex.db.WithContext(ctx)
	err = db.Table(TableName).
		Clauses(clause.Where{Exprs: []clause.Expression{clause.Or(eqExpr...)}}).
		Delete("").Error
	if err != nil {
		return
	}

	err = db.Table(SourceTableName).
		Clauses(clause.Where{Exprs: []clause.Expression{clause.Or(eqExpr...)}}).
		Delete("").Error
	if err != nil {
		return
	}

	return
}

func (ex *SourcemanagerExecutor) SourceKind(ctx context.Context) (ret response.SourceKind, err error) {
	var x string

	db := ex.db.WithContext(ctx)
	err = db.Table(SourceUtileTableName).Select("sourcekind").Where("enginetype = ?", "flink").Take(&x).Error
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
	err = db.Table(SourceUtileTableName).Select("dataformat").Where("enginetype = ?", "flink").Take(&x).Error
	if err != nil {
		ex.logger.Error().Error("get dataformat", err).Fire()
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
	err = db.Table(SourceUtileTableName).Select("datatype").Where("enginetype = ?", "flink").Take(&x).Error
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

func (ex *SourcemanagerExecutor) checkSourcetablesUrl(ctx context.Context, url *model.TableUrl, sourceid string) (err error) {
	descInfo, _ := ex.Describe(ctx, sourceid, false)

	sourcetype := descInfo.SourceType
	if sourcetype == constants.SourceTypeMysql {
		if err = url.GetMySQL().Validate(); err != nil {
			ex.logger.Error().Error("check source mysql define url", err).Fire()
			err = qerror.InvalidJSON
			return
		}
	} else if sourcetype == constants.SourceTypePostgreSQL {
		if err = url.GetPostgreSQL().Validate(); err != nil {
			ex.logger.Error().Error("check source postgres define url", err).Fire()
			err = qerror.InvalidJSON
			return
		}
	} else if sourcetype == constants.SourceTypeKafka {
		if err = url.GetKafka().Validate(); err != nil {
			ex.logger.Error().Error("check source kafka define url", err).Fire()
			err = qerror.InvalidJSON
			return err
		}
	} else if sourcetype == constants.SourceTypeS3 {
		if err = url.GetS3().Validate(); err != nil {
			ex.logger.Error().Error("check source s3 define url", err).Fire()
			err = qerror.InvalidJSON
			return err
		}
	} else if sourcetype == constants.SourceTypeClickHouse {
		if err = url.GetClickHouse().Validate(); err != nil {
			ex.logger.Error().Error("check source clickhouse define url", err).Fire()
			err = qerror.InvalidJSON
			return err
		}
	} else if sourcetype == constants.SourceTypeHbase {
		if err = url.GetHbase().Validate(); err != nil {
			ex.logger.Error().Error("check source hbase define url", err).Fire()
			err = qerror.InvalidJSON
			return err
		}
	} else if sourcetype == constants.SourceTypeFtp {
		if err = url.GetFtp().Validate(); err != nil {
			ex.logger.Error().Error("check source ftp define url", err).Fire()
			err = qerror.InvalidJSON
			return err
		}
	} else if sourcetype == constants.SourceTypeHDFS {
		if err = url.GetHDFS().Validate(); err != nil {
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

	info.TableID = req.GetTableID()
	if info.TableID == "" {
		info.TableID, _ = ex.idGeneratorTable.Take()
	}
	info.SourceID = req.GetSourceID()
	info.SpaceID = req.GetSpaceID()
	info.Name = req.GetName()
	info.Comment = req.GetComment()
	info.Url = req.GetUrl()
	info.Direction = req.GetDirection()
	info.CreateTime = time.Now().Unix()
	info.UpdateTime = info.CreateTime

	if err = ex.CheckSourceState(ctx, info.SourceID); err != nil {
		return
	}

	if err = ex.checkSourcetablesUrl(ctx, info.Url, info.SourceID); err != nil {
		return
	}

	if err = ex.CheckName(ctx, info.SpaceID, info.Name, TableName); err != nil {
		return
	}

	db := ex.db.WithContext(ctx)
	err = db.Table(TableName).Create(info).Error
	if err != nil {
		ex.logger.Error().Error("create source table", err).Fire()
		err = qerror.Internal
	}
	return
}

func (ex *SourcemanagerExecutor) DescribeTable(ctx context.Context, tableID string) (info model.TableInfo, err error) {
	db := ex.db.WithContext(ctx)

	err = db.Table(TableName).Where("tableid = ? ", tableID).Scan(&info).Error
	if err != nil {
		ex.logger.Error().Error("describe source table", err).Fire()
		err = qerror.Internal
	}

	//check source disable state.
	_, err = ex.Describe(ctx, info.SourceID, true)
	if err != nil {
		return
	}
	return
}

func (ex *SourcemanagerExecutor) UpdateTable(ctx context.Context, req *request.UpdateTable) (err error) {
	var (
		info     model.TableInfo
		selfInfo model.TableInfo
	)

	info.TableID = req.GetTableID()
	info.Name = req.GetName()
	info.Comment = req.GetComment()
	info.Url = req.GetUrl()
	info.Direction = req.GetDirection()
	info.UpdateTime = time.Now().Unix()

	// DescribeTable will Check source State
	selfInfo, err = ex.DescribeTable(ctx, info.TableID)
	if err != nil {
		return
	}

	if err = ex.checkSourcetablesUrl(ctx, info.Url, selfInfo.SourceID); err != nil {
		return
	}

	if err = ex.CheckName(ctx, selfInfo.SpaceID, info.Name, TableName); err != nil {
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
	err = db.Table(TableName).Select("direction", "name", "comment", "url", "updatetime").Where("tableid = ? ", info.TableID).Updates(info).Error
	if err != nil {
		ex.logger.Error().Error("update source table", err).Fire()
		err = qerror.Internal
	}
	return
}

func (ex *SourcemanagerExecutor) DeleteTable(ctx context.Context, tableIDs []string) (err error) {
	for _, id := range tableIDs {
		//DescribeTable will check source disable state
		_, err = ex.DescribeTable(ctx, id)
		if err != nil {
			return
		}

		db := ex.db.WithContext(ctx)
		err = db.Table(TableName).Where("tableid = ? ", id).Delete("").Error
		if err != nil {
			ex.logger.Error().Error("delete source table", err).Fire()
			err = qerror.Internal
		}
	}
	return
}

var validListSortBy = map[string]bool{
	"sourceid":   true,
	"tableid":    true,
	"name":       true,
	"createtime": true,
	"updatetime": true,
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
		order = "updatetime"
	}
	if input.Reverse {
		order += " DESC"
	} else {
		order += " ASC"
	}

	// Build where exprs.
	exprs := []clause.Expression{clause.Eq{Column: "spaceid", Value: input.SpaceID}}
	if len(input.Search) > 0 {
		exprs = append(exprs, clause.Like{
			Column: "name",
			Value:  "%" + input.Search + "%",
		})
	}

	db := ex.db.WithContext(ctx)
	infos := []model.SourceInfo{}
	err = db.Table(SourceTableName).Select("*").Clauses(clause.Where{Exprs: exprs}).
		Limit(int(input.Limit)).Offset(int(input.Offset)).Order(order).
		Scan(&infos).Error
	if err != nil {
		return
	}

	resp.Infos = make([]*response.DescribeSource, len(infos))
	for index := range infos {
		var (
			oneinfo   response.DescribeSource
			onesource model.SourceInfo
		)

		onesource = infos[index]
		oneinfo.Info = &onesource

		if tmperr := ex.PingSource(ctx, infos[index].SourceType, infos[index].Url); tmperr != nil {
			oneinfo.Connected = constants.SourceConnectedFailed
		} else {
			oneinfo.Connected = constants.SourceConnectedSuccess
		}
		resp.Infos[index] = &oneinfo
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
		order = "updatetime"
	}
	if input.Reverse {
		order += " DESC"
	} else {
		order += " ASC"
	}

	// Build where exprs.
	exprs := []clause.Expression{clause.Eq{Column: "sourceid", Value: input.SourceID}}
	if len(input.Search) > 0 {
		exprs = append(exprs, clause.Like{
			Column: "name",
			Value:  "%" + input.Search + "%",
		})
	}

	db := ex.db.WithContext(ctx)
	err = db.Table(TableName).Select("*").Clauses(clause.Where{Exprs: exprs}).
		Limit(int(input.Limit)).Offset(int(input.Offset)).Order(order).
		Scan(&resp.Infos).Error
	if err != nil {
		return
	}

	err = db.Table(TableName).Select("count(*)").Clauses(clause.Where{Exprs: exprs}).Count(&resp.Total).Error
	if err != nil {
		return
	}
	return
}

func (ex *SourcemanagerExecutor) SourceTables(ctx context.Context, req *request.SourceTables) (resp response.JsonList, err error) {
	sourceInfo, _ := ex.Describe(ctx, req.GetSourceID(), true)
	if err != nil {
		return
	}
	if sourceInfo.SourceType == constants.SourceTypePostgreSQL {
		//resp, err = GetPostgreSQLSourceTables(sourceInfo.Url)
	} else if sourceInfo.SourceType == constants.SourceTypeMysql {
		resp, err = GetMysqlSourceTables(sourceInfo.GetUrl().GetMySQL())
	} else if sourceInfo.SourceType == constants.SourceTypeClickHouse {
		//resp, err = GetClickHouseSourceTables(sourceInfo.Url)
	} else if sourceInfo.SourceType == constants.SourceTypeHbase {
		//resp, err = GetHbaseSourceTables(sourceInfo.Url)
	} else if sourceInfo.SourceType == constants.SourceTypeKafka {
		//resp, err = GetKafkaSourceTables(sourceInfo.Url)
	} else {
		err = qerror.NotSupportSourceType.Format(sourceInfo.SourceType)
	}

	return
}

func (ex *SourcemanagerExecutor) TableColumns(ctx context.Context, req *request.TableColumns) (resp response.TableColumns, err error) {
	sourceInfo, _ := ex.Describe(ctx, req.GetSourceID(), true)
	if err != nil {
		return
	}
	if sourceInfo.SourceType == constants.SourceTypePostgreSQL {
		//resp, err = GetPostgreSQLSourceTables(sourceInfo.Url)
	} else if sourceInfo.SourceType == constants.SourceTypeMysql {
		resp, err = GetMysqlSourceTableColumns(sourceInfo.GetUrl().GetMySQL(), req.GetTableName())
	} else if sourceInfo.SourceType == constants.SourceTypeClickHouse {
		//resp, err = GetClickHouseSourceTables(sourceInfo.Url)
	} else if sourceInfo.SourceType == constants.SourceTypeHbase {
		//resp, err = GetHbaseSourceTables(sourceInfo.Url)
	} else if sourceInfo.SourceType == constants.SourceTypeKafka {
		//resp, err = GetKafkaSourceTables(sourceInfo.Url)
	} else {
		err = qerror.NotSupportSourceType.Format(sourceInfo.SourceType)
	}

	return
}
