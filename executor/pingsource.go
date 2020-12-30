package executor

import (
	"context"
	"encoding/json"
	"fmt"

	"github.com/DataWorkbench/common/constants"
	"github.com/Shopify/sarama"
	"gorm.io/driver/mysql"
	"gorm.io/driver/postgres"

	"gorm.io/gorm"
)

func PingMysql(url string) (err error) {
	var m constants.SourceMysqlParams

	if err = json.Unmarshal([]byte(url), &m); err != nil {
		return err
	}

	dsn := fmt.Sprintf(
		"%s:%s@tcp(%s:%d)/%s?charset=utf8mb4&parseTime=True&loc=Local",
		m.User, m.Password, m.Host, m.Port, m.Database,
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
	var p constants.SourcePostgreSQLParams

	if err = json.Unmarshal([]byte(url), &p); err != nil {
		return err
	}

	dsn := fmt.Sprintf(
		"user=%s password=%s host=%s port=%d  dbname=%s ",
		p.User, p.Password, p.Host, p.Port, p.Database,
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
	var k constants.SourceKafkaParams

	if err = json.Unmarshal([]byte(url), &k); err != nil {
		return err
	}

	dsn := fmt.Sprintf("%s:%d", k.Host, k.Port)

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

	if sourcetype == constants.SourceTypeMysql {
		return PingMysql(url)
	} else if sourcetype == constants.SourceTypePostgreSQL {
		return PingPostgreSQL(url)
	} else if sourcetype == constants.SourceTypeKafka {
		return PingKafka(url)
	} else {
		return fmt.Errorf("unknow source type %s", sourcetype)
	}

	return nil
}
