package executor

import (
	"context"
	"fmt"
	"io"
	"io/ioutil"
	"net"
	"net/http"
	"strings"
	"time"

	"github.com/DataWorkbench/common/constants"
	"github.com/DataWorkbench/common/qerror"
	"github.com/DataWorkbench/gproto/pkg/model"
	"github.com/Shopify/sarama"
	"github.com/aws/aws-sdk-go/aws"
	"github.com/aws/aws-sdk-go/aws/credentials"
	"github.com/aws/aws-sdk-go/aws/session"
	"github.com/aws/aws-sdk-go/service/s3"
	_ "github.com/aws/aws-sdk-go/service/s3/s3manager"
	"github.com/dutchcoders/goftp"
	"github.com/samuel/go-zookeeper/zk"
	"gorm.io/driver/mysql"
	"gorm.io/driver/postgres"
	"gorm.io/gorm"
)

func PingMysql(url *model.MySQLUrl) (err error) {
	dsn := fmt.Sprintf(
		"%s:%s@tcp(%s:%d)/%s?charset=utf8mb4&parseTime=True&loc=Local",
		url.User, url.Password, url.Host, url.Port, url.Database,
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

func PingPostgreSQL(url *model.PostgreSQLUrl) (err error) {
	dsn := fmt.Sprintf(
		"user=%s password=%s host=%s port=%d  dbname=%s ",
		url.User, url.Password, url.Host, url.Port, url.Database,
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

func PingClickHouse(url *model.ClickHouseUrl) (err error) {
	var (
		client  *http.Client
		req     *http.Request
		rep     *http.Response
		reqBody io.Reader
	)

	client = &http.Client{Timeout: time.Second * 10}
	reqBody = strings.NewReader("SELECT 1")
	dsn := fmt.Sprintf(
		"http://%s:%d/?user=%s&password=%s&database=%s",
		url.Host, url.Port, url.User, url.Password, url.Database,
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

func PingKafka(url *model.KafkaUrl) (err error) {
	dsn := fmt.Sprintf("%s", url.KafkaBrokers)

	consumer, terr := sarama.NewConsumer([]string{dsn}, nil)
	if terr != nil {
		err = terr
		return
	}
	consumer.Close()
	return
}

func PingS3(url *model.S3Url) (err error) {
	sess, err1 := session.NewSession(&aws.Config{
		Credentials: credentials.NewStaticCredentials(url.AccessKey, url.SecretKey, ""),
		Endpoint:    aws.String(url.EndPoint),
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

func PingHbase(url *model.HbaseUrl) (err error) {
	var (
		conn *zk.Conn
	)

	hosts := []string{url.Zookeeper}
	conn, _, err = zk.Connect(hosts, time.Second*10)
	defer conn.Close()
	return
}

func PingFtp(url *model.FtpUrl) (err error) {
	var (
		conn *goftp.FTP
	)

	if conn, err = goftp.Connect(fmt.Sprintf("%v:%d", url.Host, url.Port)); err != nil {
		return
	}
	defer conn.Close()
	return
}

func PingHDFS(url *model.HDFSUrl) (err error) {
	// https://github.com/colinmarc/hdfs -- install the hadoop client. so don't use it.
	// https://studygolang.com/articles/766 -- use 50070 http port. but user input the IPC port.

	for _, node := range url.GetNodes() {
		conn, err := net.DialTimeout("tcp", fmt.Sprintf("%s:%d", node.GetNameNode, node.GetPort()), 3*time.Second)
		if err == nil && conn != nil {
			conn.Close()
			return nil
		}
		if err == nil && conn == nil {
			err = fmt.Errorf("hdfs can't connected")
		}
	}

	return
}

func (ex *SourcemanagerExecutor) PingSource(ctx context.Context, sourcetype string, url *model.SourceUrl) (err error) {
	if err = ex.checkSourceInfo(url, sourcetype); err != nil {
		return
	}

	if sourcetype == constants.SourceTypeMysql {
		err = PingMysql(url.GetMySQL())
	} else if sourcetype == constants.SourceTypePostgreSQL {
		err = PingPostgreSQL(url.GetPostgreSQL())
	} else if sourcetype == constants.SourceTypeKafka {
		err = PingKafka(url.GetKafka())
	} else if sourcetype == constants.SourceTypeS3 {
		err = PingS3(url.GetS3())
	} else if sourcetype == constants.SourceTypeClickHouse {
		err = PingClickHouse(url.GetClickHouse())
	} else if sourcetype == constants.SourceTypeHbase {
		err = PingHbase(url.GetHbase())
	} else if sourcetype == constants.SourceTypeFtp {
		err = PingFtp(url.GetFtp())
	} else if sourcetype == constants.SourceTypeHDFS {
		err = PingHDFS(url.GetHDFS())
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
