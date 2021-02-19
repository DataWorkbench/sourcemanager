package executor

import (
	"context"
	"encoding/json"
	"fmt"
	"io"
	"io/ioutil"
	"net/http"
	"strings"
	"time"

	"github.com/DataWorkbench/common/constants"
	"github.com/DataWorkbench/common/qerror"
	"github.com/Shopify/sarama"
	"github.com/aws/aws-sdk-go/aws"
	"github.com/aws/aws-sdk-go/aws/credentials"
	"github.com/aws/aws-sdk-go/aws/session"
	"github.com/aws/aws-sdk-go/service/s3"
	_ "github.com/aws/aws-sdk-go/service/s3/s3manager"
	"gorm.io/driver/mysql"
	"gorm.io/driver/postgres"
	"gorm.io/gorm"
)

func PingMysql(url string) (err error) {
	var m constants.SourceMysqlParams

	if err = json.Unmarshal([]byte(url), &m); err != nil {
		return
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
		return
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

func PingClickHouse(url string) (err error) {
	var (
		p       constants.SourceClickHouseParams
		client  *http.Client
		req     *http.Request
		rep     *http.Response
		reqBody io.Reader
	)

	if err = json.Unmarshal([]byte(url), &p); err != nil {
		return
	}

	client = &http.Client{Timeout: time.Second * 10}
	reqBody = strings.NewReader("SELECT 1")
	dsn := fmt.Sprintf(
		"http://%s:%d/?user=%s&password=%s&database=%s",
		p.Host, p.Port, p.User, p.Password, p.Database,
	)

	req, err = http.NewRequest(http.MethodGet, dsn, reqBody)
	if err != nil {
		return
	}

	rep, err = client.Do(req)
	if err != nil {
		return
	}

	repBody, _ := ioutil.ReadAll(rep.Body)
	rep.Body.Close()

	if rep.StatusCode != http.StatusOK {
		err = fmt.Errorf("%s request failed, http status code %d, message %s", dsn, rep.StatusCode, string(repBody))
		rep.Body.Close()
		return
	}
	return
}

func PingKafka(url string) (err error) {
	var k constants.SourceKafkaParams

	if err = json.Unmarshal([]byte(url), &k); err != nil {
		return
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

func PingS3(url string) (err error) {
	var s constants.SourceS3Params

	if err = json.Unmarshal([]byte(url), &s); err != nil {
		return
	}

	sess, err1 := session.NewSession(&aws.Config{
		Credentials: credentials.NewStaticCredentials(s.AccessKey, s.SecretKey, ""),
		Endpoint:    aws.String(s.EndPoint),
		Region:      aws.String("us-east-1"),
	})
	if err1 != nil {
		err = err1
		return
	}

	svc := s3.New(sess)
	_, err = svc.ListBuckets(nil)
	if err != nil {
		return
	}

	return
}

func (ex *SourcemanagerExecutor) PingSource(ctx context.Context, sourcetype string, url string, enginetype string) (err error) {
	if err = ex.checkSourcemanagerUrl(url, enginetype, sourcetype); err != nil {
		return
	}

	if sourcetype == constants.SourceTypeMysql {
		err = PingMysql(url)
	} else if sourcetype == constants.SourceTypePostgreSQL {
		err = PingPostgreSQL(url)
	} else if sourcetype == constants.SourceTypeKafka {
		err = PingKafka(url)
	} else if sourcetype == constants.SourceTypeS3 {
		err = PingS3(url)
	} else if sourcetype == constants.SourceTypeClickHouse {
		err = PingClickHouse(url)
	} else {
		ex.logger.Error().String("don't support this source type ", sourcetype).Fire()
		err = qerror.ConnectSourceFailed
	}
	if err != nil {
		ex.logger.Error().Error("connect source failed", err).Fire()
		err = qerror.ConnectSourceFailed
	}

	return
}
