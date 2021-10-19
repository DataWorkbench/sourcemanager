package executor

import (
	"fmt"
	"io"
	"io/ioutil"
	"net/http"
	"strings"
	"time"

	"github.com/DataWorkbench/gproto/pkg/datasourcepb"
	"github.com/DataWorkbench/gproto/pkg/response"
	_ "github.com/aws/aws-sdk-go/service/s3/s3manager"
	"gorm.io/driver/mysql"
	"gorm.io/driver/postgres"
	"gorm.io/gorm"
)

func GetMysqlSourceTables(url *datasourcepb.MySQLURL) (resp response.JsonList, err error) {
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

func GetMysqlSourceTableColumns(url *datasourcepb.MySQLURL, tableName string) (resp response.TableColumns, err error) {
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
		err = db.Raw("select column_name as name, column_type as type, '' as length, case column_key when 'PRI' then 'true' else 'false' end  as isprkey  from information_schema.columns where table_schema = ? and table_name= ?", url.GetDatabase(), tableName).Scan(&resp.Columns).Error
		sqldb.Close()
		if err != nil {
			return resp, err
		}
	}
	for _, column := range resp.Columns {
		column.Type, column.Length = getTypeLength(column.Type)
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
		}
		if tp == "DATETIME" {
		}
	}
	return
}

func GetPostgreSQLSourceTables(url *datasourcepb.PostgreSQLURL) (resp response.JsonList, err error) {
	dsn := fmt.Sprintf(
		"user=%s password=%s host=%s port=%d  dbname=%s ",
		url.GetUser(), url.GetPassword(), url.GetHost(), url.GetPort(), url.GetDatabase(),
	)

	var db *gorm.DB
	db, err = gorm.Open(postgres.Open(dsn), &gorm.Config{})
	if err != nil {
		return resp, err
	} else {
		sqldb, _ := db.DB()
		err = db.Raw("SELECT n.nspname || '.' || c.relname as item FROM pg_catalog.pg_class c LEFT JOIN pg_catalog.pg_namespace n ON n.oid = c.relnamespace WHERE c.relkind IN ('r','p') AND n.nspname <> 'pg_catalog' AND n.nspname <> 'information_schema' AND n.nspname !~ '^pg_toast' AND pg_catalog.pg_table_is_visible(c.oid);").Scan(&resp.JsonList).Error
		sqldb.Close()
		if err != nil {
			return resp, err
		}
	}
	return
}

func getTypeLength(in string) (tp string, l string) {
	if strings.Index(in, "(") == -1 {
		tp = in
	} else {
		tp = in[0:strings.Index(in, "(")]
		l = in[strings.Index(in, "(")+1 : strings.Index(in, ")")]
	}
	return
}

func GetPostgreSQLSourceTableColumns(url *datasourcepb.PostgreSQLURL, tableName string) (resp response.TableColumns, err error) {
	dsn := fmt.Sprintf(
		"user=%s password=%s host=%s port=%d  dbname=%s ",
		url.GetUser(), url.GetPassword(), url.GetHost(), url.GetPort(), url.GetDatabase(),
	)

	var db *gorm.DB
	db, err = gorm.Open(postgres.Open(dsn), &gorm.Config{})
	if err != nil {
		return resp, err
	} else {
		sqldb, _ := db.DB()
		err = db.Raw("SELECT a.attname as name, pg_catalog.format_type(a.atttypid, a.atttypmod) as type, '' as length, case attnotnull when 't' then 'true' else 'false' end  as isprkey FROM pg_catalog.pg_attribute a WHERE a.attrelid = '" + tableName + "'::regclass::oid AND a.attnum > 0 AND NOT a.attisdropped").Scan(&resp.Columns).Error
		sqldb.Close()
		if err != nil {
			return resp, err
		}
	}

	for _, column := range resp.Columns {
		column.Type, column.Length = getTypeLength(column.Type)

		tp := strings.ToUpper(column.Type)

		if tp == "SMALLINT" || tp == "TINYINT UNSIGNED" || tp == "INT2" || tp == "SMALLSERIAL" || tp == "SERIAL2" {
			column.Type = "SMALLINT"
			column.Length = ""
		}
		if tp == "INTEGER" || tp == "SERIAL" {
			column.Type = "INT"
			column.Length = ""
		}
		if tp == "BIGINT" || tp == "BIGSERIAL" {
			column.Type = "BIGINT"
			column.Length = ""
		}
		if tp == "FLOAT" || tp == "REAL" {
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
		if tp == "BOOLEAN" {
			column.Type = "BOOLEAN"
			column.Length = ""
		}
		if tp == "DATE" {
			column.Type = "DATE"
			column.Length = ""
		}
		if tp == "BYTEA" {
			column.Type = "BYTES"
			column.Length = ""
		}
		if tp == "ARRAY" {
			column.Type = "ARRAY"
			column.Length = ""
		}
		if tp == "CHAR" || tp == "VARCHAR" || tp == "TEXT" || tp == "CHARACTER" || tp == "CHARACTER VARYING" {
			column.Type = "STRING"
			column.Length = ""
		}
		if tp == "TIME" || tp == "TIMESTAMP" {
		}
		if tp == "DATETIME" {
		}
	}
	return
}

func GetClickHouseSourceTables(url *datasourcepb.ClickHouseURL) (resp response.JsonList, err error) {
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

	repBody, _ := ioutil.ReadAll(rep.Body)
	rep.Body.Close()

	if rep.StatusCode != http.StatusOK {
		err = fmt.Errorf("%s request failed, http status code %d, message %s", dsn, rep.StatusCode, string(repBody))
		rep.Body.Close()
		return
	}

	returnItems := strings.Split(string(repBody[0:len(repBody)-1]), "\n")
	for i := 0; i < len(returnItems); i++ {
		resp.JsonList = append(resp.JsonList, returnItems[i])
	}

	return
}

func GetClickHouseSourceTableColumns(url *datasourcepb.ClickHouseURL, tableName string) (resp response.TableColumns, err error) {
	var (
		client  *http.Client
		req     *http.Request
		rep     *http.Response
		reqBody io.Reader
	)

	client = &http.Client{Timeout: time.Second * 10}
	reqBody = strings.NewReader("select name, type, '' length, case is_in_primary_key when 1 then 'true' else 'false' end as isprkey from system.columns where table= '" + tableName + "' and database='" + url.GetDatabase() + "'")
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

	repBody, _ := ioutil.ReadAll(rep.Body)
	rep.Body.Close()

	if rep.StatusCode != http.StatusOK {
		err = fmt.Errorf("%s request failed, http status code %d, message %s", dsn, rep.StatusCode, string(repBody))
		rep.Body.Close()
		return
	}

	returnItems := strings.Split(string(repBody[0:len(repBody)-1]), "\n")
	for i := 0; i < len(returnItems); i++ {
		var column response.TableColumns_Column

		dbColumn := strings.Split(returnItems[i], "	")
		Name := dbColumn[0]
		Type, Length := getTypeLength(dbColumn[1])
		IsPrkey := dbColumn[3]

		if Type == "Int8" {
			Type = "TINYINT"
		} else if Type == "Int16" {
			Type = "SMALLINT"
		} else if Type == "Int32" {
			Type = "INTEGER"
		} else if Type == "Int64" {
			Type = "BIGINT"
		} else if Type == "Float32" {
			Type = "FLOAT"
		} else if Type == "Float64" {
			Type = "DOUBLE"
		} else if strings.Index(Type, "String") != -1 {
			Type = "String"
			Length = ""
		} else if strings.Index(Type, "Enum") != -1 {
			Type = "String"
			Length = ""
		} else if Type == "Date" {
			Type = "DATE"
		} else if Type == "DateTime" {
			Type = "TIMESTAMP"
		}

		column.Name = Name
		column.Type = Type
		column.Length = Length
		column.IsPrimaryKey = IsPrkey

		resp.Columns = append(resp.Columns, &column)
	}

	return
}

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
*/
