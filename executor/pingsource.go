package executor

import (
	"context"
	"encoding/json"
	"fmt"

	"github.com/Shopify/sarama"
	"gorm.io/driver/mysql"
	"gorm.io/driver/postgres"

	"gorm.io/gorm"
)

func PingMysql(url string) (err error) {
	var mapUrl map[string]string
	if err = json.Unmarshal([]byte(url), &mapUrl); err != nil {
		return err
	}
	if _, ok := mapUrl["connector_options"]; ok == false {
		err = fmt.Errorf("can't not find connector_options")
		return
	}

	dsn := fmt.Sprintf(
		"%s:%s@tcp(%s:%s)/%s?charset=utf8mb4&parseTime=True&loc=Local",
		mapUrl["user"], mapUrl["password"], mapUrl["host"], mapUrl["port"], mapUrl["database"],
	)

	var db *gorm.DB
	db, err = gorm.Open(mysql.Open(dsn), &gorm.Config{})
	if err != nil {
		return err
	} else {
		sqldb, _ := db.DB()
		sqldb.Close()
	}
	return
}

func PingPostgreSQL(url string) (err error) {
	var mapUrl map[string]string
	if err = json.Unmarshal([]byte(url), &mapUrl); err != nil {
		return err
	}
	if _, ok := mapUrl["connector_options"]; ok == false {
		err = fmt.Errorf("can't not find connector_options")
		return
	}

	dsn := fmt.Sprintf(
		"user=%s password=%s host=%s port=%s  dbname=%s ",
		mapUrl["user"], mapUrl["password"], mapUrl["host"], mapUrl["port"], mapUrl["database"],
	)

	var db *gorm.DB
	db, err = gorm.Open(postgres.Open(dsn), &gorm.Config{})
	if err != nil {
		return err
	} else {
		sqldb, _ := db.DB()
		sqldb.Close()
	}
	return
}

func PingKafka(url string) (err error) {
	var mapUrl map[string]string
	if err = json.Unmarshal([]byte(url), &mapUrl); err != nil {
		return err
	}

	if _, ok := mapUrl["connector_options"]; ok == false {
		err = fmt.Errorf("can't not find connector_options")
		return
	}

	dsn := fmt.Sprintf("%s:%s", mapUrl["host"], mapUrl["port"])

	consumer, terr := sarama.NewConsumer([]string{dsn}, nil)
	if terr != nil {
		err = terr
		return
	}
	consumer.Close()
	return
}

func (ex *SourcemanagerExecutor) PingSource(ctx context.Context, sourcetype string, url string, enginetype string) (err error) {
	if err = checkSourcemanagerUrl(url, enginetype, sourcetype); err != nil {
		return
	}

	if sourcetype == SourceTypeMysql {
		return PingMysql(url)
	} else if sourcetype == SourceTypePostgreSQL {
		return PingPostgreSQL(url)
	} else if sourcetype == SourceTypeKafka {
		return PingKafka(url)
	} else {
		return fmt.Errorf("unknow source type %s", sourcetype)
	}

	return nil
}
