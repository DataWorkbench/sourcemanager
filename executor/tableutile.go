package executor

import (
	"fmt"
	"io"
	"io/ioutil"
	"net/http"
	"strings"
	"time"
	"unicode"

	"github.com/DataWorkbench/gproto/pkg/model"
	"github.com/DataWorkbench/gproto/pkg/response"
	_ "github.com/aws/aws-sdk-go/service/s3/s3manager"
	"gorm.io/driver/mysql"
	"gorm.io/gorm"
)

func GetMysqlSourceTables(url *model.MySQLUrl) (resp response.JsonList, err error) {
	dsn := fmt.Sprintf(
		"%s:%s@tcp(%s:%d)/%s?charset=utf8mb4&parseTime=True&loc=Local",
		url.GetUser(), url.GetPassword(), url.GetHost(), url.GetPort(), url.GetDatabase(),
	)

	var db *gorm.DB
	db, err = gorm.Open(mysql.Open(dsn), &gorm.Config{})
	if err != nil {
		return resp, err
	} else {
		sqldb, _ := db.DB()
		err = db.Raw("select table_name as item from information_schema.tables where  table_schema = ?", url.GetDatabase()).Scan(&resp.JsonList).Error
		sqldb.Close()
		if err != nil {
			return resp, err
		}
	}
	return
}

func GetMysqlSourceTableColumns(url *model.MySQLUrl, tableName string) (resp response.TableColumns, err error) {
	dsn := fmt.Sprintf(
		"%s:%s@tcp(%s:%d)/%s?charset=utf8mb4&parseTime=True&loc=Local",
		url.GetUser(), url.GetPassword(), url.GetHost(), url.GetPort(), url.GetDatabase(),
	)

	var db *gorm.DB
	db, err = gorm.Open(mysql.Open(dsn), &gorm.Config{})
	if err != nil {
		return resp, err
	} else {
		sqldb, _ := db.DB()
		err = db.Raw("select column_name as name, substring_index(column_type, '(', 1) as type,  substring_index(substring_index(column_type, '(', -1), ')', 1) as length, case column_key when 'PRI' then 'true' else 'false' end  as isprkey  from information_schema.columns where table_schema = ? and table_name= ?", url.GetDatabase(), tableName).Scan(&resp.Columns).Error
		sqldb.Close()
		if err != nil {
			return resp, err
		}
	}
	for _, column := range resp.Columns {
		tp := strings.ToUpper(column.Type)

		if tp == "TINYINT" {
			column.Type = "TINYINT"
			column.Length = ""
		}
		if tp == "SMALLINT" || tp == "TINYINT UNSIGNED" {
			column.Type = "SMALLINT"
			column.Length = ""
		}
		if tp == "INT" || tp == "MEDIUMINT" || tp == "SMALLINT UNSIGNED" {
			column.Type = "INT"
			column.Length = ""
		}
		if tp == "BIGINT" || tp == "INT UNSIGNED" {
			column.Type = "BIGINT"
			column.Length = ""
		}
		if tp == "BIGINT UNSIGNED" {
			column.Type = "DECIMAL"
			column.Length = "20, 0"
		}
		if tp == "FLOAT" {
			column.Type = "FLOAT"
			column.Length = ""
		}
		if tp == "DOUBLE" || tp == "DOUBLE PRECISION" {
			column.Type = "DOUBLE"
			column.Length = ""
		}
		if tp == "NUMERIC" || tp == "DECIMAL" {
			column.Type = "DECIMAL"
		}
		if tp == "BOOLEAN" || (tp == "TINYINT" && column.Length == "1") {
			column.Type = "BOOLEAN"
			column.Length = ""
		}
		if tp == "DATE" {
			column.Type = "DATE"
			column.Length = ""
		}
		if tp == "BINARY" || tp == "VARBINARY" || tp == "BLOB" {
			column.Type = "BYTES"
			column.Length = ""
		}
		if tp == "CHAR" || tp == "VARCHAR" || tp == "TEXT" {
			column.Type = "STRING"
			column.Length = ""
		}
		if tp == "TIME" || tp == "TIMESTAMP" {
			if column.Length != "" && unicode.IsDigit(rune(column.Length[0])) == false {
				column.Length = ""
			}
		}
		if tp == "DATETIME" {
			column.Type = "TIMESTAMP"
			if column.Length != "" && unicode.IsDigit(rune(column.Length[0])) == false {
				column.Length = ""
			}
		}
	}
	return
}

/*
func GetPostgreSQLSourceTables(url constants.JSONString) (subItem []*SourceTables, err error) {
	var p model.PostgreSQLUrl

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
		return subItem, err
	} else {
		sqldb, _ := db.DB()
		db.Raw("SELECT n.nspname || '.' || c.relname as item FROM pg_catalog.pg_class c LEFT JOIN pg_catalog.pg_namespace n ON n.oid = c.relnamespace WHERE c.relkind IN ('r','p') AND n.nspname <> 'pg_catalog' AND n.nspname <> 'information_schema' AND n.nspname !~ '^pg_toast' AND pg_catalog.pg_table_is_visible(c.oid);").Scan(&subItem)
		sqldb.Close()
	}
	return
}
*/

func GetClickHouseSourceTables(url *model.ClickHouseUrl) (resp response.JsonList, err error) {
	var (
		client  *http.Client
		req     *http.Request
		rep     *http.Response
		reqBody io.Reader
	)

	client = &http.Client{Timeout: time.Second * 10}
	reqBody = strings.NewReader("select name as item from system.tables where database='" + url.GetDatabase() + "'")
	dsn := fmt.Sprintf(
		"http://%s:%d/?user=%s&password=%s&database=%s",
		url.GetHost(), url.GetPort(), url.GetUser(), url.GetPassword(), url.GetDatabase(),
	)

	req, err = http.NewRequest(http.MethodGet, dsn, reqBody)
	if err != nil {
		return
	}

	rep, err = client.Do(req)
	if err != nil {
		return
	}

	//select name as item from system.tables where database='default';

	repBody, _ := ioutil.ReadAll(rep.Body)
	rep.Body.Close()

	if rep.StatusCode != http.StatusOK {
		err = fmt.Errorf("%s request failed, http status code %d, message %s", dsn, rep.StatusCode, string(repBody))
		rep.Body.Close()
		return
	}

	returnItems := strings.Split(string(repBody[0:len(repBody)-1]), "\n")
	for i := 0; i <= len(returnItems); i++ {
		resp.JsonList = append(resp.JsonList, returnItems[i])
	}

	return
}

//func GetClickHouseSourceTables(url constants.JSONString) (subItem []*SourceTables, err error) {
//	var (
//		p       model.ClickHouseUrl
//		client  *http.Client
//		req     *http.Request
//		rep     *http.Response
//		reqBody io.Reader
//	)
//
//	if err = json.Unmarshal([]byte(url), &p); err != nil {
//		return
//	}
//
//	client = &http.Client{Timeout: time.Second * 10}
//	reqBody = strings.NewReader("select distinct table from system.columns where database='" + p.Database + "'")
//	dsn := fmt.Sprintf(
//		"http://%s:%d/?user=%s&password=%s&database=%s",
//		p.Host, p.Port, p.User, p.Password, p.Database,
//	)
//
//	req, err = http.NewRequest(http.MethodGet, dsn, reqBody)
//	if err != nil {
//		return
//	}
//
//	rep, err = client.Do(req)
//	if err != nil {
//		return
//	}
//
//	//select name as item from system.tables where database='default';
//
//	repBody, _ := ioutil.ReadAll(rep.Body)
//	rep.Body.Close()
//
//	if rep.StatusCode != http.StatusOK {
//		err = fmt.Errorf("%s request failed, http status code %d, message %s", dsn, rep.StatusCode, string(repBody))
//		rep.Body.Close()
//		return
//	}
//
//	returnItems := strings.Split(string(repBody[0:len(repBody)-1]), "\n")
//	subItem = make([]*SourceTables, len(returnItems))
//
//	for i := 0; i <= len(returnItems); i++ {
//		subItem[i].Item = returnItems[i]
//	}
//
//	return
//}

/*
func GetKafkaSourceTables(url constants.JSONString) (subItem []*SourceTables, err error) {
	var k model.KafkaUrl

	if err = json.Unmarshal([]byte(url), &k); err != nil {
		return
	}

	dsn := fmt.Sprintf("%s", k.KafkaBrokers)

	consumer, terr := sarama.NewConsumer([]string{dsn}, nil)
	if terr != nil {
		err = terr
		return
	}
	consumer.Close()
	return
}


func GetHbaseSourceTables(url constants.JSONString) (subItem []*SourceTables, err error) {
	var (
		s    model.HbaseUrl
		conn *zk.Conn
	)

	if err = json.Unmarshal([]byte(url), &s); err != nil {
		return
	}

	hosts := []string{s.Zookeeper}
	conn, _, err = zk.Connect(hosts, time.Second*10)
	defer conn.Close()
	return
}
*/
