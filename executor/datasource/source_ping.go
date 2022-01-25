package datasource

import (
	"context"
	"fmt"
	"io"
	"io/ioutil"
	"net"
	"net/http"
	"strconv"
	"strings"
	"time"

	"github.com/DataWorkbench/gproto/pkg/datasourcepb"
	"github.com/DataWorkbench/gproto/pkg/model"
	"github.com/Shopify/sarama"
	"github.com/dutchcoders/goftp"
	"github.com/samuel/go-zookeeper/zk"
	"gorm.io/driver/mysql"
	"gorm.io/driver/postgres"
	"gorm.io/gorm"
)

func pingMysql(url *datasourcepb.MySQLURL) (err error) {
	var conn net.Conn
	ip := net.JoinHostPort(url.Host, strconv.Itoa(int(url.Port)))
	conn, err = net.DialTimeout("tcp", ip, time.Second*3)
	if err != nil {
		return
	}
	if conn != nil {
		_ = conn.Close()
	}

	dsn := fmt.Sprintf(
		"%s:%s@tcp(%s:%d)/%s?charset=utf8mb4&parseTime=True&loc=Local",
		url.User, url.Password, url.Host, url.Port, url.Database,
	)
	var db *gorm.DB
	db, err = gorm.Open(mysql.Open(dsn), &gorm.Config{})
	if err != nil {
		return err
	} else {
		if sqlDB, e := db.DB(); e == nil {
			_ = sqlDB.Close()
		}
	}
	return
}

func pingPostgreSQL(url *datasourcepb.PostgreSQLURL) (err error) {
	var conn net.Conn

	ip := net.JoinHostPort(url.Host, strconv.Itoa(int(url.Port)))
	conn, err = net.DialTimeout("tcp", ip, time.Second*3)
	if err != nil {
		return
	}
	if conn != nil {
		_ = conn.Close()
	}

	dsn := fmt.Sprintf(
		"user=%s password=%s host=%s port=%d  dbname=%s ",
		url.User, url.Password, url.Host, url.Port, url.Database,
	)
	var db *gorm.DB
	db, err = gorm.Open(postgres.Open(dsn), &gorm.Config{})
	if err != nil {
		return err
	} else {
		if sqlDB, e := db.DB(); e == nil {
			_ = sqlDB.Close()
		}
	}
	return
}

func pingClickHouse(url *datasourcepb.ClickHouseURL) (err error) {
	var conn net.Conn
	ip := net.JoinHostPort(url.Host, strconv.Itoa(int(url.Port)))
	conn, err = net.DialTimeout("tcp", ip, time.Second*3)
	if err != nil {
		return
	}
	if conn != nil {
		_ = conn.Close()
	}

	var (
		client  *http.Client
		req     *http.Request
		rep     *http.Response
		reqBody io.Reader
	)

	client = &http.Client{Timeout: time.Millisecond * 100}
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
	_ = rep.Body.Close()

	if rep.StatusCode != http.StatusOK {
		err = fmt.Errorf("%s request failed, http status code %d, message %s", dsn, rep.StatusCode, string(repBody))
		_ = rep.Body.Close()
		return
	}
	return
}

func pingKafka(url *datasourcepb.KafkaURL) (err error) {
	dsn := strings.Split(url.KafkaBrokers, ",")

	consumer, terr := sarama.NewConsumer(dsn, nil)
	if terr != nil {
		err = terr
		return
	}
	_ = consumer.Close()
	return
}

func pingHBase(url *datasourcepb.HBaseURL) (err error) {
	var conn *zk.Conn

	hosts := strings.Split(url.ZkHosts, ",")
	conn, _, err = zk.Connect(hosts, time.Millisecond*100)
	if err == nil {
		conn.Close()
	}
	return
}

func pingFtp(url *datasourcepb.FtpURL) (err error) {
	var (
		conn *goftp.FTP
	)

	if conn, err = goftp.Connect(fmt.Sprintf("%v:%d", url.Host, url.Port)); err != nil {
		return
	}

	err = conn.Login(url.User, url.Password)
	_ = conn.Close()
	return
}

func pingHDFS(url *datasourcepb.HDFSURL) (err error) {
	var conn net.Conn
	// https://github.com/colinmarc/hdfs -- install the hadoop client. so don't use it.
	// https://studygolang.com/articles/766 -- use 50070 http port. but user input the IPC port.
	ip := net.JoinHostPort(url.GetNodes().GetNameNode(), strconv.Itoa(int(url.GetNodes().GetPort())))
	conn, err = net.DialTimeout("tcp", ip, time.Millisecond*2000)
	if err != nil {
		return
	}
	if conn != nil {
		_ = conn.Close()
	}
	return
}

func PingDataSourceConnection(ctx context.Context, sourceType model.DataSource_Type, sourceURL *model.DataSource_URL) (connInfo *model.DataSourceConnection, err error) {
	begin := time.Now()
	message := ""
	result := model.DataSourceConnection_Success

	switch sourceType {
	case model.DataSource_MySQL:
		err = pingMysql(sourceURL.Mysql)
	case model.DataSource_PostgreSQL:
		err = pingPostgreSQL(sourceURL.Postgresql)
	case model.DataSource_Kafka:
		err = pingKafka(sourceURL.Kafka)
	case model.DataSource_S3:
	case model.DataSource_ClickHouse:
		err = pingClickHouse(sourceURL.Clickhouse)
	case model.DataSource_HBase:
		err = pingHBase(sourceURL.Hbase)
	case model.DataSource_Ftp:
		err = pingFtp(sourceURL.Ftp)
	case model.DataSource_HDFS:
		err = pingHDFS(sourceURL.Hdfs)
	}

	if err != nil {
		result = model.DataSourceConnection_Failed
		message = err.Error()
		err = nil
	}

	connInfo = &model.DataSourceConnection{
		SpaceId:     "",
		SourceId:    "",
		NetworkId:   "",
		Status:      model.DataSourceConnection_Active,
		Result:      result,
		Message:     message,
		Created:     begin.Unix(),
		Elapse:      time.Now().Sub(begin).Milliseconds(),
		NetworkInfo: nil,
	}
	return
}
