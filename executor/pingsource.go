package executor

import (
	"context"
	"fmt"
	"gorm.io/driver/mysql"
	"gorm.io/driver/postgres"
	"gorm.io/gorm"
	"io"
	"io/ioutil"
	"net"
	"net/http"
	"strconv"
	"strings"
	"time"

	"github.com/DataWorkbench/common/qerror"
	"github.com/DataWorkbench/gproto/pkg/datasourcepb"
	"github.com/DataWorkbench/gproto/pkg/model"
	"github.com/Shopify/sarama"
	_ "github.com/aws/aws-sdk-go/service/s3/s3manager"
	"github.com/dutchcoders/goftp"
	"github.com/samuel/go-zookeeper/zk"
)

func PingMysql(url *datasourcepb.MySQLURL) (err error) {
	ip := net.JoinHostPort(url.Host, strconv.Itoa(int(url.Port)))
	conn, err := net.DialTimeout("tcp", ip, time.Millisecond*2000)
	if err == nil {
		if conn != nil {
			_ = conn.Close()
		}
	} else {
		return qerror.ConnectSourceFailed
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
		sqldb, _ := db.DB()
		sqldb.Close()
	}
	return
}

func PingPostgreSQL(url *datasourcepb.PostgreSQLURL) (err error) {
	ip := net.JoinHostPort(url.Host, strconv.Itoa(int(url.Port)))
	conn, err := net.DialTimeout("tcp", ip, time.Millisecond*2000)
	if err == nil {
		if conn != nil {
			_ = conn.Close()
		}
	} else {
		return qerror.ConnectSourceFailed
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
		sqldb, _ := db.DB()
		_ = sqldb.Close()
	}
	return
}



func PingClickHouse(url *datasourcepb.ClickHouseURL) (err error) {
	ip := net.JoinHostPort(url.Host, strconv.Itoa(int(url.Port)))
	conn, err := net.DialTimeout("tcp", ip, time.Millisecond*2000)
	if err == nil {
		if conn != nil {
			_ = conn.Close()
		}
	} else {
		return qerror.ConnectSourceFailed
	}

	var (
		client  *http.Client
		req     *http.Request
		rep     *http.Response
		reqBody io.Reader
	)

	defer func() {
		if rep!=nil {
			_ = rep.Body.Close()
		}
		if client!=nil {
			client.CloseIdleConnections()
		}
	}()
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
		return
	}
	return
}

func PingKafka(url *datasourcepb.KafkaURL) (err error) {
	dsn := strings.Split(url.KafkaBrokers, ",")

	consumer, terr := sarama.NewConsumer(dsn, nil)
	if terr != nil {
		err = terr
		return
	}
	_ = consumer.Close()
	return
}

/*
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
*/

func PingHBase(url *datasourcepb.HBaseURL) (err error) {
	var (
		conn *zk.Conn
	)

	hosts := strings.Split(url.Zookeeper, ",")
	conn, _, err = zk.Connect(hosts, time.Millisecond*100)
	if err == nil {
		conn.Close()
	}
	return
}

func PingFtp(url *datasourcepb.FtpURL) (err error) {
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

func PingHDFS(url *datasourcepb.HDFSURL) (err error) {
	// https://github.com/colinmarc/hdfs -- install the hadoop client. so don't use it.
	// https://studygolang.com/articles/766 -- use 50070 http port. but user input the IPC port.
	ip := net.JoinHostPort(url.GetNodes().GetNameNode(), strconv.Itoa(int(url.GetNodes().GetPort())))
	conn, err := net.DialTimeout("tcp", ip, time.Millisecond*2000)
	if err == nil {
		if conn != nil {
			_ = conn.Close()
		}
		return nil
	}
	return qerror.ConnectSourceFailed
}

func (ex *SourcemanagerExecutor) PingSource(ctx context.Context, sourcetype model.DataSource_Type, url *datasourcepb.DataSourceURL) (err error) {
	if err = ex.checkSourceInfo(url, sourcetype); err != nil {
		return
	}

	if sourcetype == model.DataSource_MySQL {
		err = PingMysql(url.GetMysql())
	} else if sourcetype == model.DataSource_PostgreSQL {
		err = PingPostgreSQL(url.GetPostgresql())
	} else if sourcetype == model.DataSource_Kafka {
		err = PingKafka(url.GetKafka())
	} else if sourcetype == model.DataSource_S3 {
		//err = PingS3(url.GetS3())
		ex.logger.Error().String("don't support this source type ", sourcetype.String()).Fire()
		err = qerror.ConnectSourceFailed
	} else if sourcetype == model.DataSource_ClickHouse {
		err = PingClickHouse(url.GetClickhouse())
	} else if sourcetype == model.DataSource_HBase {
		err = PingHBase(url.GetHbase())
	} else if sourcetype == model.DataSource_Ftp {
		err = PingFtp(url.GetFtp())
	} else if sourcetype == model.DataSource_HDFS {
		err = PingHDFS(url.GetHdfs())
	} else {
		ex.logger.Error().String("don't support this source type ", sourcetype.String()).Fire()
		err = qerror.NotSupportSourceType.Format(sourcetype)
	}
	if err != nil {
		ex.logger.Error().Error("connect source failed", err).Fire()
		err = qerror.ConnectSourceFailed.Format(err)
	}

	return
}
