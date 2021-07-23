package executor

import (
	"context"
	"encoding/json"
	"fmt"
	"time"

	"strings"

	"github.com/DataWorkbench/common/constants"
	"github.com/DataWorkbench/common/qerror"
	"github.com/DataWorkbench/common/utils/idgenerator"
	"github.com/DataWorkbench/glog"
	"gorm.io/gorm"
)

const (
	SourcemanagerTableName = "sourcemanager"
	SourceTablesName       = "sourcetables"
	EngineMapTableName     = "enginemapsource"
)

type EngineMapInfo struct {
	EngineType string `gorm:"column:enginetype;"`
	SourceType string `gorm:"column:sourcetype;"`
}

type SourcemanagerInfo struct {
	ID         string               `gorm:"column:id;primaryKey"`
	SpaceID    string               `gorm:"column:spaceid;"`
	SourceType string               `gorm:"column:sourcetype;"`
	Name       string               `gorm:"column:name;"`
	Comment    string               `gorm:"column:comment;"`
	Creator    string               `gorm:"column:creator;"`
	Url        constants.JSONString `gorm:"column:url;"`
	CreateTime string               `gorm:"column:createtime;"`
	UpdateTime string               `gorm:"column:updatetime;"`
	EngineType string               `gorm:"column:enginetype;"`
	Direction  string               `gorm:"column:direction;"`
}

type SourceTablesInfo struct {
	ID         string               `gorm:"column:id;primaryKey"`
	SourceID   string               `gorm:"column:sourceid;"`
	Name       string               `gorm:"column:name;"`
	Comment    string               `gorm:"column:comment;"`
	Url        constants.JSONString `gorm:"column:url;"`
	CreateTime string               `gorm:"column:createtime;"`
	UpdateTime string               `gorm:"column:updatetime;"`
}

func (smi SourcemanagerInfo) TableName() string {
	return SourcemanagerTableName
}

func (sti SourceTablesInfo) TableName() string {
	return SourceTablesName
}

type SourcemanagerExecutor struct {
	db             *gorm.DB
	idGenerator    *idgenerator.IDGenerator
	idGeneratorSot *idgenerator.IDGenerator
	logger         *glog.Logger
}

func NewSourceManagerExecutor(db *gorm.DB, l *glog.Logger) *SourcemanagerExecutor {
	ex := &SourcemanagerExecutor{
		db:             db,
		idGenerator:    idgenerator.New(constants.SourceManagerIDPrefix),
		idGeneratorSot: idgenerator.New(constants.SourceTablesIDPrefix),
		logger:         l,
	}
	return ex
}

func (ex *SourcemanagerExecutor) GetSourceDirection(enginetype string, sourcetype string) (direction string, err error) {
	if enginetype == constants.ServerTypeFlink {
		if sourcetype == constants.SourceTypeMysql || sourcetype == constants.SourceTypePostgreSQL || sourcetype == constants.SourceTypeKafka || sourcetype == constants.SourceTypeS3 || sourcetype == constants.SourceTypeHbase {
			direction = constants.DirectionSource + constants.DirectionDestination
		} else if sourcetype == constants.SourceTypeClickHouse {
			direction = constants.DirectionDestination
		} else {
			ex.logger.Error().Error("not support source type", fmt.Errorf("unknow source type %s", sourcetype)).Fire()
			err = qerror.NotSupportSourceType.Format(sourcetype)
			return
		}
	} else {
		ex.logger.Error().Error("not support engine type", fmt.Errorf("unknow engine type %s", enginetype)).Fire()
		err = qerror.NotSupportEngineType.Format(enginetype)
		return
	}
	return
}

func (ex *SourcemanagerExecutor) checkSourcemanagerUrl(url constants.JSONString, enginetype string, sourcetype string) (err error) {
	if enginetype == constants.ServerTypeFlink {
		if sourcetype == constants.SourceTypeMysql {
			var v constants.SourceMysqlParams
			if err = json.Unmarshal([]byte(url), &v); err != nil {
				ex.logger.Error().Error("check source mysql url", err).Fire()
				err = qerror.InvalidJSON
				return
			}
		} else if sourcetype == constants.SourceTypePostgreSQL {
			var v constants.SourcePostgreSQLParams
			if err = json.Unmarshal([]byte(url), &v); err != nil {
				ex.logger.Error().Error("check source postgresql url", err).Fire()
				err = qerror.InvalidJSON
				return
			}
		} else if sourcetype == constants.SourceTypeKafka {
			var v constants.SourceKafkaParams
			if err = json.Unmarshal([]byte(url), &v); err != nil {
				ex.logger.Error().Error("check source kafka url", err).Fire()
				err = qerror.InvalidJSON
				return
			}
		} else if sourcetype == constants.SourceTypeS3 {
			var v constants.SourceS3Params
			if err = json.Unmarshal([]byte(url), &v); err != nil {
				ex.logger.Error().Error("check source s3 url", err).Fire()
				err = qerror.InvalidJSON
				return
			}
		} else if sourcetype == constants.SourceTypeClickHouse {
			var v constants.SourceClickHouseParams
			if err = json.Unmarshal([]byte(url), &v); err != nil {
				ex.logger.Error().Error("check source clickhouse url", err).Fire()
				err = qerror.InvalidJSON
				return
			}
		} else if sourcetype == constants.SourceTypeHbase {
			var v constants.SourceHbaseParams
			if err = json.Unmarshal([]byte(url), &v); err != nil {
				ex.logger.Error().Error("check source hbase url", err).Fire()
				err = qerror.InvalidJSON
				return
			}
		} else {
			ex.logger.Error().Error("not support source type", fmt.Errorf("unknow source type %s", sourcetype)).Fire()
			err = qerror.NotSupportSourceType.Format(sourcetype)
			return
		}
	} else {
		ex.logger.Error().Error("not support engine type", fmt.Errorf("unknow engine type %s", enginetype)).Fire()
		err = qerror.NotSupportEngineType.Format(enginetype)
		return
	}

	return nil
}

func (ex *SourcemanagerExecutor) CheckSourceExists(ctx context.Context, spaceid string, name string) (r bool) {
	var infos []*SourcemanagerInfo

	db := ex.db.WithContext(ctx)

	err := db.Table(SourcemanagerTableName).Where("spaceid = ? and name = ? ", spaceid, name).Scan(&infos).Error
	if err != nil {
		r = false
	} else {
		if len(infos) > 0 {
			r = true
		} else {
			r = false
		}
	}
	return
}

func (ex *SourcemanagerExecutor) CheckSourceTableExists(ctx context.Context, sourceid string, name string) (r bool) {
	var infos []*SourceTablesInfo

	db := ex.db.WithContext(ctx)

	err := db.Table(SourceTablesName).Where("sourceid = ? and name = ? ", sourceid, name).Scan(&infos).Error
	if err != nil {
		r = false
	} else {
		if len(infos) > 0 {
			r = true
		} else {
			r = false
		}
	}
	return
}

// SourceManager
func (ex *SourcemanagerExecutor) Create(ctx context.Context, info SourcemanagerInfo) (err error) {
	if len(strings.Split(info.Name, ".")) != 1 {
		ex.logger.Error().Error("invalid name", fmt.Errorf("can't use '.' in name")).Fire()
		err = qerror.InvalidSourceName
		return
	}

	if err = ex.checkSourcemanagerUrl(info.Url, info.EngineType, info.SourceType); err != nil {
		return
	}

	if ex.CheckSourceExists(ctx, info.SpaceID, info.Name) == true {
		ex.logger.Error().Error("name exist", fmt.Errorf("this name %s is exists", info.Name)).Fire()
		err = qerror.ResourceAlreadyExists
		return
	}

	if info.ID == "" {
		info.ID, _ = ex.idGenerator.Take()
	}
	info.CreateTime = time.Now().Format("2006-01-02 15:04:05")
	info.UpdateTime = info.CreateTime
	info.Direction, err = ex.GetSourceDirection(info.EngineType, info.SourceType)
	if err != nil {
		ex.logger.Error().Error("create source", err).Fire()
		return
	}

	db := ex.db.WithContext(ctx)
	err = db.Create(info).Error
	if err != nil {
		ex.logger.Error().Error("create source", err).Fire()
		err = qerror.Internal
	}
	return
}

func (ex *SourcemanagerExecutor) Update(ctx context.Context, info SourcemanagerInfo) (err error) {
	if len(strings.Split(info.Name, ".")) != 1 {
		ex.logger.Error().Error("invalid name", fmt.Errorf("can't use '.' in name")).Fire()
		err = qerror.InvalidSourceName
		return
	}

	descInfo, _ := ex.Describe(ctx, info.ID)
	if err = ex.checkSourcemanagerUrl(info.Url, descInfo.EngineType, info.SourceType); err != nil {
		return
	}

	if ex.CheckSourceExists(ctx, descInfo.SpaceID, info.Name) == true && descInfo.Name != info.Name {
		ex.logger.Error().Error("name exist", fmt.Errorf("this name %s is exists", info.Name)).Fire()
		err = qerror.ResourceAlreadyExists
		return
	}

	info.UpdateTime = time.Now().Format("2006-01-02 15:04:05")

	db := ex.db.WithContext(ctx)
	err = db.Select("sourcetype", "name", "comment", "url", "updatetime").Where("id = ? ", info.ID).Updates(info).Error
	if err != nil {
		ex.logger.Error().Error("update source", err).Fire()
		err = qerror.Internal
	}
	return
}

func (ex *SourcemanagerExecutor) Delete(ctx context.Context, id string) (err error) {
	tables, _ := ex.SotLists(ctx, id, 10, 1)
	if len(tables) > 0 {
		err = qerror.ResourceIsUsing
		return
	}

	db := ex.db.WithContext(ctx)

	err = db.Where("id = ? ", id).Delete(&SourcemanagerInfo{}).Error
	if err != nil {
		ex.logger.Error().Error("delete source", err).Fire()
		err = qerror.Internal
	}
	return
}

func (ex *SourcemanagerExecutor) Lists(ctx context.Context, limit int32, offset int32, spaceid string) (infos []*SourcemanagerInfo, err error) {
	db := ex.db.WithContext(ctx)

	err = db.Table(SourcemanagerTableName).Where("spaceid = ? ", spaceid).Limit(int(limit)).Offset(int(offset)).Scan(&infos).Error
	if err != nil {
		ex.logger.Error().Error("list source", err).Fire()
		err = qerror.Internal
	}
	return
}

func (ex *SourcemanagerExecutor) Describe(ctx context.Context, id string) (info SourcemanagerInfo, err error) {
	db := ex.db.WithContext(ctx)

	err = db.Table(SourcemanagerTableName).Where("id = ? ", id).Scan(&info).Error
	if err != nil {
		ex.logger.Error().Error("describe source", err).Fire()
		err = qerror.Internal
	}
	return
}

func (ex *SourcemanagerExecutor) checkSourcetablesUrl(url constants.JSONString, enginetype string, sourcetype string) (err error) {
	if enginetype == constants.ServerTypeFlink {
		if sourcetype == constants.SourceTypeMysql {
			var v constants.FlinkTableDefineMysql
			if err = json.Unmarshal([]byte(url), &v); err != nil {
				ex.logger.Error().Error("check source mysql define url", err).Fire()
				err = qerror.InvalidJSON
				return
			}
		} else if sourcetype == constants.SourceTypePostgreSQL {
			var v constants.FlinkTableDefinePostgreSQL
			if err = json.Unmarshal([]byte(url), &v); err != nil {
				ex.logger.Error().Error("check source postgres define url", err).Fire()
				err = qerror.InvalidJSON
				return
			}
		} else if sourcetype == constants.SourceTypeKafka {
			var v constants.FlinkTableDefineKafka
			if err = json.Unmarshal([]byte(url), &v); err != nil {
				ex.logger.Error().Error("check source kafka define url", err).Fire()
				err = qerror.InvalidJSON
				return err
			}
		} else if sourcetype == constants.SourceTypeS3 {
			var v constants.FlinkTableDefineS3
			if err = json.Unmarshal([]byte(url), &v); err != nil {
				ex.logger.Error().Error("check source s3 define url", err).Fire()
				err = qerror.InvalidJSON
				return err
			}
		} else if sourcetype == constants.SourceTypeClickHouse {
			var v constants.FlinkTableDefineClickHouse
			if err = json.Unmarshal([]byte(url), &v); err != nil {
				ex.logger.Error().Error("check source clickhouse define url", err).Fire()
				err = qerror.InvalidJSON
				return err
			}
		} else if sourcetype == constants.SourceTypeHbase {
			var v constants.FlinkTableDefineHbase
			if err = json.Unmarshal([]byte(url), &v); err != nil {
				ex.logger.Error().Error("check source hbase define url", err).Fire()
				err = qerror.InvalidJSON
				return err
			}
		} else {
			ex.logger.Error().Error("not support source type", fmt.Errorf("unknow source type %s", sourcetype)).Fire()
			err = qerror.NotSupportSourceType.Format(sourcetype)
			return
		}
	} else {
		ex.logger.Error().Error("not support engine type", fmt.Errorf("unknow engine type %s", enginetype)).Fire()
		err = qerror.NotSupportEngineType.Format(enginetype)
		return
	}

	return nil
}

// Source Tables
func (ex *SourcemanagerExecutor) SotCreate(ctx context.Context, info SourceTablesInfo) (err error) {
	if len(strings.Split(info.Name, ".")) != 1 {
		ex.logger.Error().Error("invalid name", fmt.Errorf("can't use '.' in name")).Fire()
		err = qerror.InvalidSourceName
		return
	}

	sourceInfo, _ := ex.Describe(ctx, info.SourceID)

	if err = ex.checkSourcetablesUrl(info.Url, sourceInfo.EngineType, sourceInfo.SourceType); err != nil {
		return
	}

	if ex.CheckSourceTableExists(ctx, info.SourceID, info.Name) == true {
		ex.logger.Error().Error("name exist", fmt.Errorf("this name %s is exists", info.Name)).Fire()
		err = qerror.ResourceAlreadyExists
		return
	}

	if info.ID == "" {
		info.ID, _ = ex.idGeneratorSot.Take()
	}
	info.CreateTime = time.Now().Format("2006-01-02 15:04:05")
	info.UpdateTime = info.CreateTime

	db := ex.db.WithContext(ctx)
	err = db.Create(info).Error
	if err != nil {
		ex.logger.Error().Error("create source table", err).Fire()
		err = qerror.Internal
	}
	return
}

func (ex *SourcemanagerExecutor) SotUpdate(ctx context.Context, info SourceTablesInfo) (err error) {
	if len(strings.Split(info.Name, ".")) != 1 {
		ex.logger.Error().Error("invalid name", fmt.Errorf("can't use '.' in name")).Fire()
		err = qerror.InvalidSourceName
		return
	}

	selfInfo, _ := ex.SotDescribe(ctx, info.ID)
	sourceInfo, _ := ex.Describe(ctx, selfInfo.SourceID)

	if err = ex.checkSourcetablesUrl(info.Url, sourceInfo.EngineType, sourceInfo.SourceType); err != nil {
		return
	}

	if ex.CheckSourceTableExists(ctx, selfInfo.SourceID, info.Name) == true && info.Name != selfInfo.Name {
		ex.logger.Error().Error("name exist", fmt.Errorf("this name %s is exists", info.Name)).Fire()
		err = qerror.ResourceAlreadyExists
		return
	}

	info.UpdateTime = time.Now().Format("2006-01-02 15:04:05")

	db := ex.db.WithContext(ctx)
	err = db.Select("name", "comment", "url", "updatetime").Where("id = ? ", info.ID).Updates(info).Error
	if err != nil {
		ex.logger.Error().Error("update source table", err).Fire()
		err = qerror.Internal
	}
	return
}

func (ex *SourcemanagerExecutor) SotDelete(ctx context.Context, id string) (err error) {
	//TODO schedule DeleteAll
	db := ex.db.WithContext(ctx)
	err = db.Where("id = ? ", id).Delete(&SourceTablesInfo{}).Error
	if err != nil {
		ex.logger.Error().Error("delete source table", err).Fire()
		err = qerror.Internal
	}
	return
}

func (ex *SourcemanagerExecutor) SotLists(ctx context.Context, sourceId string, limit int32, offset int32) (infos []*SourceTablesInfo, err error) {
	db := ex.db.WithContext(ctx)

	err = db.Table(SourceTablesName).Where("sourceid = ? ", sourceId).Limit(int(limit)).Offset(int(offset)).Scan(&infos).Error
	if err != nil {
		ex.logger.Error().Error("list source table", err).Fire()
		err = qerror.Internal
	}
	return
}

func (ex *SourcemanagerExecutor) SotDescribe(ctx context.Context, id string) (info SourceTablesInfo, err error) {
	db := ex.db.WithContext(ctx)

	err = db.Table(SourceTablesName).Where("id = ? ", id).Scan(&info).Error
	if err != nil {
		ex.logger.Error().Error("describe source table", err).Fire()
		err = qerror.Internal
	}
	return
}

func (ex *SourcemanagerExecutor) EngineMap(ctx context.Context, engineName string) (info EngineMapInfo, err error) {
	db := ex.db.WithContext(ctx)

	err = db.Table(EngineMapTableName).Where("enginetype = ? ", engineName).Scan(&info).Error
	if err != nil {
		ex.logger.Error().Error("engine map", err).Fire()
		err = qerror.Internal
	}
	return
}

func (ex *SourcemanagerExecutor) DeleteAll(ctx context.Context, SpaceID string) (err error) {
	var (
		managers []*SourcemanagerInfo
		tables   []*SourceTablesInfo
	)

	managers, err = ex.Lists(ctx, 100000, 0, SpaceID)
	if err != nil {
		return
	}
	for _, managerInfo := range managers {
		tables, err = ex.SotLists(ctx, managerInfo.ID, 100000, 0)
		if err != nil {
			return
		}

		for _, tableInfo := range tables {
			err = ex.SotDelete(ctx, tableInfo.ID)
			if err != nil {
				return
			}
		}
		err = ex.Delete(ctx, managerInfo.ID)
		if err != nil {
			return
		}
	}

	return
}
