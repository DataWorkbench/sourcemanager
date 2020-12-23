package executor

import (
	"context"
	"encoding/json"
	"fmt"
	"time"

	"strings"

	"github.com/DataWorkbench/common/utils/idgenerator"
	"gorm.io/gorm"
)

const (
	SourcemanagerIDPrefix = "som-"
	SourceTablesIDPrefix  = "sot-"

	SourcemanagerTableName = "sourcemanager"
	SourceTablesName       = "sourcetables"
	EngineMapTableName     = "enginemapsource"

	SourceTypeMysql      = "MySQL"
	SourceTypePostgreSQL = "PostgreSQL"
	SourceTypeKafka      = "Kafka"

	TableTypeDimension = "d"
	TableTypeComment   = "c"

	FlinkEngineName = "Flink"

	CreatorWorkBench = "workbench" //can't drop by custom,  workbench is auto create when spark/other engine created
	CreatorCustom    = "custom"
)

type EngineMapInfo struct {
	EngineType string `gorm:"column:enginetype;"`
	SourceType string `gorm:"column:sourcetype;"`
}

type SourcemanagerInfo struct {
	ID         string `gorm:"column:id;primaryKey"`
	SpaceID    string `gorm:"column:spaceid;"`
	SourceType string `gorm:"column:sourcetype;"`
	Name       string `gorm:"column:name;"`
	Comment    string `gorm:"column:comment;"`
	Creator    string `gorm:"column:creator;"`
	Url        string `gorm:"column:url;"`
	CreateTime string `gorm:"column:createtime;"`
	UpdateTime string `gorm:"column:updatetime;"`
	EngineType string `gorm:"column:enginetype;"`
}

type SourceTablesInfo struct {
	ID         string `gorm:"column:id;primaryKey"`
	SourceID   string `gorm:"column:sourceid;"`
	Name       string `gorm:"column:name;"`
	Comment    string `gorm:"column:comment;"`
	Url        string `gorm:"column:url;"`
	CreateTime string `gorm:"column:createtime;"`
	UpdateTime string `gorm:"column:updatetime;"`
	TabType    string `gorm:"column:tabtype;"`
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
}

func NewSourceManagerExecutor(db *gorm.DB) *SourcemanagerExecutor {
	ex := &SourcemanagerExecutor{
		db:             db,
		idGenerator:    idgenerator.New(SourcemanagerIDPrefix),
		idGeneratorSot: idgenerator.New(SourceTablesIDPrefix),
	}
	return ex
}

func checkSourcemanagerUrl(url string, enginetype string, sourcetype string) (err error) {
	var (
		ok     bool
		mapUrl map[string]string
	)

	if err = json.Unmarshal([]byte(url), &mapUrl); err != nil {
		return err
	}

	if enginetype == FlinkEngineName {
		if sourcetype == SourceTypeMysql || sourcetype == SourceTypePostgreSQL {
			if _, ok = mapUrl["user"]; ok == false {
				err = fmt.Errorf("can't not find user")
				return
			}
			if _, ok = mapUrl["password"]; ok == false {
				err = fmt.Errorf("can't not find password")
				return
			}
			if _, ok = mapUrl["host"]; ok == false {
				err = fmt.Errorf("can't not find host")
				return
			}
			if _, ok = mapUrl["port"]; ok == false {
				err = fmt.Errorf("can't not find port")
				return
			}
			if _, ok = mapUrl["database"]; ok == false {
				err = fmt.Errorf("can't not find database")
				return
			}
			if _, ok = mapUrl["connector_options"]; ok == false {
				err = fmt.Errorf("can't not find connector_options")
				return
			}
		} else if sourcetype == SourceTypeKafka {
			if _, ok = mapUrl["host"]; ok == false {
				err = fmt.Errorf("can't not find host")
				return
			}
			if _, ok = mapUrl["port"]; ok == false {
				err = fmt.Errorf("can't not find port")
				return
			}
			if _, ok = mapUrl["connector_options"]; ok == false {
				err = fmt.Errorf("can't not find connector_options")
				return
			}
		} else {
			return fmt.Errorf("unknow source type %s", sourcetype)
		}
	} else {
		return fmt.Errorf("unknow engine type %s", enginetype)
	}

	return nil
}

// SourceManager
func (ex *SourcemanagerExecutor) Create(ctx context.Context, info SourcemanagerInfo) (err error) {
	if len(strings.Split(info.Name, ".")) != 1 {
		err = fmt.Errorf("can't use '.' in name")
		return
	}

	if err = checkSourcemanagerUrl(info.Url, info.EngineType, info.SourceType); err != nil {
		return
	}

	if info.ID == "" {
		info.ID, _ = ex.idGenerator.Take()
	}
	info.CreateTime = time.Now().Format("2006-01-02 15:04:05")
	info.UpdateTime = info.CreateTime

	db := ex.db.WithContext(ctx)
	err = db.Create(info).Error
	return
}

func (ex *SourcemanagerExecutor) Update(ctx context.Context, info SourcemanagerInfo) (err error) {
	descInfo, _ := ex.Describe(ctx, info.ID)
	if err = checkSourcemanagerUrl(info.Url, descInfo.EngineType, info.SourceType); err != nil {
		return
	}

	info.UpdateTime = time.Now().Format("2006-01-02 15:04:05")

	db := ex.db.WithContext(ctx)
	err = db.Select("sourcetype", "name", "comment", "url", "updatetime").Where("id = ? ", info.ID).Updates(info).Error
	return
}

func (ex *SourcemanagerExecutor) Delete(ctx context.Context, id string) (err error) {
	db := ex.db.WithContext(ctx)

	err = db.Where("id = ? ", id).Delete(&SourcemanagerInfo{}).Error
	return
}

func (ex *SourcemanagerExecutor) Lists(ctx context.Context, limit int32, offset int32) (infos []*SourcemanagerInfo, err error) {
	db := ex.db.WithContext(ctx)

	err = db.Table(SourcemanagerTableName).Limit(int(limit)).Offset(int(offset)).Scan(&infos).Error
	return
}

func (ex *SourcemanagerExecutor) Describe(ctx context.Context, id string) (info SourcemanagerInfo, err error) {
	db := ex.db.WithContext(ctx)

	err = db.Table(SourcemanagerTableName).Where("id = ? ", id).Scan(&info).Error
	return
}

func checkSourcetablesUrl(url string, enginetype string, sourcetype string) (err error) {
	var (
		ok           bool
		tableUrlJson map[string]string
	)

	if err = json.Unmarshal([]byte(url), &tableUrlJson); err != nil {
		return
	}

	if enginetype == FlinkEngineName {
		if sourcetype == SourceTypeMysql || sourcetype == SourceTypePostgreSQL {
			if _, ok = tableUrlJson["sqlColumn"]; ok == false {
				err = fmt.Errorf("can't not find sqlColumn")
				return
			}
			if _, ok = tableUrlJson["connector_options"]; ok == false {
				err = fmt.Errorf("can't not find connector_options")
				return
			}
		} else if sourcetype == SourceTypeKafka {
			if _, ok = tableUrlJson["sqlColumn"]; ok == false {
				err = fmt.Errorf("can't not find sqlColumn")
				return
			}
			if _, ok = tableUrlJson["topic"]; ok == false {
				err = fmt.Errorf("can't not find topic")
				return
			}
			if _, ok = tableUrlJson["format"]; ok == false {
				err = fmt.Errorf("can't not find format")
				return
			}
			if _, ok = tableUrlJson["connector_options"]; ok == false {
				err = fmt.Errorf("can't not find connector_options")
				return
			}
		} else {
			err = fmt.Errorf("unknow sourcemanager %s", sourcetype)
			return
		}
	} else {
		err = fmt.Errorf("unknow EngineType %s", enginetype)
		return
	}
	return nil
}

// Source Tables
func (ex *SourcemanagerExecutor) SotCreate(ctx context.Context, info SourceTablesInfo) (err error) {
	sourceInfo, _ := ex.Describe(ctx, info.SourceID)
	if info.TabType == TableTypeDimension && sourceInfo.SourceType != SourceTypeMysql && sourceInfo.SourceType != SourceTypePostgreSQL {
		err = fmt.Errorf("can't create dimension in the sourcemanager %s", sourceInfo.SourceType)
		return
	}

	if err = checkSourcetablesUrl(info.Url, sourceInfo.EngineType, sourceInfo.SourceType); err != nil {
		return
	}

	if info.ID == "" {
		info.ID, _ = ex.idGeneratorSot.Take()
	}
	info.CreateTime = time.Now().Format("2006-01-02 15:04:05")
	info.UpdateTime = info.CreateTime

	db := ex.db.WithContext(ctx)
	err = db.Create(info).Error
	return
}

func (ex *SourcemanagerExecutor) SotUpdate(ctx context.Context, info SourceTablesInfo) (err error) {
	selfInfo, _ := ex.SotDescribe(ctx, info.ID)
	managerInfo, _ := ex.Describe(ctx, selfInfo.SourceID)
	if err = checkSourcetablesUrl(info.Url, managerInfo.EngineType, managerInfo.SourceType); err != nil {
		return
	}

	info.UpdateTime = time.Now().Format("2006-01-02 15:04:05")

	db := ex.db.WithContext(ctx)
	err = db.Select("name", "comment", "url", "updatetime").Where("id = ? ", info.ID).Updates(info).Error
	return
}

func (ex *SourcemanagerExecutor) SotDelete(ctx context.Context, id string) (err error) {
	db := ex.db.WithContext(ctx)
	err = db.Where("id = ? ", id).Delete(&SourceTablesInfo{}).Error
	return
}

func (ex *SourcemanagerExecutor) SotLists(ctx context.Context, sourceId string, limit int32, offset int32) (infos []*SourceTablesInfo, err error) {
	db := ex.db.WithContext(ctx)

	err = db.Table(SourceTablesName).Where("sourceid = ? ", sourceId).Limit(int(limit)).Offset(int(offset)).Scan(&infos).Error
	return
}

func (ex *SourcemanagerExecutor) SotDescribe(ctx context.Context, id string) (info SourceTablesInfo, err error) {
	db := ex.db.WithContext(ctx)

	err = db.Table(SourceTablesName).Where("id = ? ", id).Scan(&info).Error
	return
}

func (ex *SourcemanagerExecutor) EngineMap(ctx context.Context, engineName string) (info EngineMapInfo, err error) {
	db := ex.db.WithContext(ctx)

	err = db.Table(EngineMapTableName).Where("enginetype = ? ", engineName).Scan(&info).Error
	return
}

/*
func (ex *SourcemanagerExecutor) SotApplyTable(ctx context.Context, id string) (err error) {
	db := ex.db.WithContext(ctx)

	err = db.Model(&SourceTablesInfo{}).Where("id = ? ", id).Update("refcount", gorm.Expr("refcount + 1")).Error
	return
}
*/
