package tests

import (
	"context"
	"encoding/json"
	"strings"
	"testing"

	"github.com/DataWorkbench/glog"
	"github.com/stretchr/testify/require"

	"github.com/DataWorkbench/common/constants"
	"github.com/DataWorkbench/common/grpcwrap"
	"github.com/DataWorkbench/common/qerror"
	"github.com/DataWorkbench/common/utils/idgenerator"

	"github.com/DataWorkbench/gproto/pkg/model"
	"github.com/DataWorkbench/gproto/pkg/request"
	"github.com/DataWorkbench/gproto/pkg/response"
	"github.com/DataWorkbench/gproto/pkg/smpb"
)

var MysqlManager request.CreateSource //name mysql
var MysqlSource request.CreateTable
var MysqlDest request.CreateTable

var ClickHouseManager request.CreateSource
var ClickHouseSource request.CreateTable
var ClickHouseDest request.CreateTable

var KafkaManager request.CreateSource
var KafkaSource request.CreateTable

var PGManager request.CreateSource

//var PGSource request.CreateTable
//var PGDest request.CreateTable

var S3Manager request.CreateSource
var HbaseManager request.CreateSource
var FtpManager request.CreateSource
var HDFSManager request.CreateSource

var NewSpaceManager request.CreateSource        // name mysql
var NameExistsManager request.CreateSource      //create failed
var NameErrorManager request.CreateSource       //create failed
var SourceTypeErrorManager request.CreateSource //create failed

var TablePG request.CreateTable
var TableNameExists request.CreateTable
var TableNameError request.CreateTable
var TableJsonError request.CreateTable
var TableManagerError request.CreateTable
var TableMysqlDimensionSource request.CreateTable
var TableMysqlDimensionDest request.CreateTable
var TableMysqlCommonSource request.CreateTable
var TableMysqlCommonDest request.CreateTable
var TableS3Source request.CreateTable
var TableS3Dest request.CreateTable
var TableUDFSource request.CreateTable
var TableUDFDest request.CreateTable
var TableHbaseSource request.CreateTable
var TableHbaseDest request.CreateTable
var TableFtpSource request.CreateTable
var TableFtpDest request.CreateTable

func typeToJsonString(v interface{}) string {
	s, _ := json.Marshal(&v)
	return string(s)
}

var client smpb.SourcemanagerClient
var ctx context.Context
var initDone bool
var spaceid string
var newspaceid string

func mainInit(t *testing.T) {
	if initDone == true {
		return
	}
	initDone = true
	spaceid = "wks-0000000000000001"
	newspaceid = "wks-0000000000000002"

	// Mysql
	// https://segmentfault.com/a/1190000039048901
	MysqlManager = request.CreateSource{SourceID: "som-00000000000mysql", SpaceID: spaceid, SourceType: constants.SourceTypeMysql, Name: "mysql", Comment: "",
		Url: &model.SourceUrl{MySQL: &model.MySQLUrl{User: "root", Password: "password", Host: "127.0.0.1", Database: "data_workbench", Port: 3306}}}
	MysqlSource = request.CreateTable{TableID: "sot-00000mysqlsource", SourceID: MysqlManager.SourceID, SpaceID: spaceid, Name: "ms", Comment: "mysql", TableKind: "source", Define: &model.TableDefine{MySQL: &model.MySQLTableDefine{SqlColumn: []*model.SqlColumnType{&model.SqlColumnType{Column: "id", Type: "bigint", PrimaryKey: "t"}, &model.SqlColumnType{Column: "id1", Type: "bigint", Comment: "xxx", PrimaryKey: "f"}}}}}
	MysqlDest = request.CreateTable{TableID: "sot-0000000mysqldest", SourceID: MysqlManager.SourceID, SpaceID: spaceid, Name: "md", Comment: "mysql dest", TableKind: "destination", Define: &model.TableDefine{MySQL: &model.MySQLTableDefine{SqlColumn: []*model.SqlColumnType{&model.SqlColumnType{Column: "id", Type: "bigint", PrimaryKey: "t"}, &model.SqlColumnType{Column: "id1", Type: "bigint", Comment: "xxx", PrimaryKey: "f"}}}}}

	// ClickHouse
	// create table cks(paycount bigint, paymoney varchar(10)) ENGINE=TinyLog;
	// create table zz(id bigint, id1 bigint, t timestamp, v varchar(10), primary key (id)) engine=MergeTree;
	ClickHouseManager = request.CreateSource{SourceID: "som-000000clickhouse", SpaceID: spaceid, SourceType: constants.SourceTypeClickHouse, Name: "clickhouse", Comment: "clickhouse", Url: &model.SourceUrl{ClickHouse: &model.ClickHouseUrl{User: "default", Password: "", Host: "127.0.0.1", Port: 8123, Database: "default"}}}
	ClickHouseSource = request.CreateTable{TableID: "sot-clickhousesource", SourceID: ClickHouseManager.SourceID, SpaceID: spaceid, Name: "cks", Comment: "cksource", TableKind: "source", Define: &model.TableDefine{ClickHouse: &model.ClickHouseTableDefine{SqlColumn: []*model.SqlColumnType{&model.SqlColumnType{Column: "paycount", Type: "bigint", PrimaryKey: "t"}, &model.SqlColumnType{Column: "paymoney", Type: "varchar", Length: "10", Comment: "xxx", PrimaryKey: "f"}}}}}
	ClickHouseDest = request.CreateTable{TableID: "sot-00clickhousedest", SourceID: ClickHouseManager.SourceID, SpaceID: spaceid, Name: "ckd", Comment: "ckdest", TableKind: "destination", Define: &model.TableDefine{ClickHouse: &model.ClickHouseTableDefine{SqlColumn: []*model.SqlColumnType{&model.SqlColumnType{Column: "paycount", Type: "bigint", PrimaryKey: "t"}, &model.SqlColumnType{Column: "paymoney", Type: "varchar", Length: "10", Comment: "xxx", PrimaryKey: "f"}}}}}

	// PostgreSQL
	PGManager = request.CreateSource{SourceID: "som-000000postgresql", SpaceID: spaceid, SourceType: constants.SourceTypePostgreSQL, Name: "pg", Comment: "",
		Url: &model.SourceUrl{PostgreSQL: &model.PostgreSQLUrl{User: "lzzhang", Password: "lzzhang", Host: "127.0.0.1", Database: "lzzhang", Port: 5432}}}
	//PGSource = request.CreateTable{TableID: "sot-postgresqlsource", SourceID: PGManager.SourceID, SpaceID: spaceid, Name: "pgs", Comment: "pgs", TableKind: "source", Define: &model.TableDefine{MySQL: &model.MySQLTableDefine{SqlColumn: []*model.SqlColumnType{&model.SqlColumnType{Column: "id", Type: "bigint", PrimaryKey: "t"}, &model.SqlColumnType{Column: "id1", Type: "bigint", Comment: "xxx", PrimaryKey: "f"}}}}}
	//PGDest = request.CreateTable{TableID: "sot-00postgresqldest", SourceID: PGManager.SourceID, SpaceID: spaceid, Name: "pgd", Comment: "pgd", TableKind: "destination", Define: &model.TableDefine{MySQL: &model.MySQLTableDefine{SqlColumn: []*model.SqlColumnType{&model.SqlColumnType{Column: "id", Type: "bigint", PrimaryKey: "t"}, &model.SqlColumnType{Column: "id1", Type: "bigint", Comment: "xxx", PrimaryKey: "f"}}}}}

	// kafka {"paycount": 2, "paymoney": "EUR"} {"paycount": 1, "paymoney": "USD"}
	KafkaManager = request.CreateSource{SourceID: "som-00000000000kafka", SpaceID: spaceid, SourceType: constants.SourceTypeKafka, Name: "kafka", Comment: "",
		Url: &model.SourceUrl{Kafka: &model.KafkaUrl{KafkaBrokers: "dataworkbench-kafka-for-test:9092"}}}
	KafkaSource = request.CreateTable{TableID: "sot-00000kafkasource", SourceID: KafkaManager.SourceID, SpaceID: spaceid, Name: "billing", Comment: "", TableKind: "source", Define: &model.TableDefine{Kafka: &model.KafkaTableDefine{SqlColumn: []*model.SqlColumnType{&model.SqlColumnType{Column: "paycount", Type: "bigint", PrimaryKey: "f"}, &model.SqlColumnType{Column: "paymoney", Type: "string", Comment: "", PrimaryKey: "f"}}, TimeColumn: []*model.SqlTimeColumnType{&model.SqlTimeColumnType{Column: "tproctime", Type: "proctime"}}, Topic: "workbench", Format: "json", ConnectorOptions: []*model.ConnectorOption{&model.ConnectorOption{Name: "'json.fail-on-missing-field'", Value: "'false'"}, &model.ConnectorOption{Name: "'json.ignore-parse-errors'", Value: "'true'"}}}}}

	S3Manager = request.CreateSource{SourceID: "som-00000000000000s3", SpaceID: spaceid, SourceType: constants.SourceTypeS3, Name: "s3",
		Url: &model.SourceUrl{S3: &model.S3Url{}}}
	//Url: &model.SourceUrl{S3: &model.S3Url{AccessKey: "RDTHDPNFWWDNWPIHESWK", SecretKey: "sVbVhAUsKGPPdiTOPAgveqCNhFjtvXFNpsPnQ7Hx", EndPoint: "http://s3.gd2.qingstor.com"}}}
	HbaseManager = request.CreateSource{SourceID: "som-00000000000hbase", SpaceID: spaceid, SourceType: constants.SourceTypeHbase, Name: "hbase",
		Url: &model.SourceUrl{Hbase: &model.HbaseUrl{Zookeeper: "hbase:2181", Znode: "/hbase"}}}
	FtpManager = request.CreateSource{SourceID: "som-0000000000000ftp", SpaceID: spaceid, SourceType: constants.SourceTypeFtp, Name: "ftp",
		Url: &model.SourceUrl{Ftp: &model.FtpUrl{Host: "42.193.101.183", Port: 21}}}
	HDFSManager = request.CreateSource{SourceID: "som-000000000000hdfs", SpaceID: spaceid, SourceType: constants.SourceTypeHDFS, Name: "hdfs",
		Url: &model.SourceUrl{HDFS: &model.HDFSUrl{Nodes: []*model.HDFSUrl_HDFSNodeUrl{&model.HDFSUrl_HDFSNodeUrl{NameNode: "127.0.0.1", Port: 8020}}}}}

	NewSpaceManager = request.CreateSource{SourceID: "som-00000000newspace", SpaceID: newspaceid, SourceType: constants.SourceTypeMysql, Name: "mysql", Comment: "newspace",
		Url: &model.SourceUrl{MySQL: &model.MySQLUrl{User: "root", Password: "password", Host: "127.0.0.1", Database: "data_workbench", Port: 3306}}}
	NameExistsManager = request.CreateSource{SourceID: "som-000000nameexists", SpaceID: spaceid, SourceType: constants.SourceTypeMysql, Name: "mysql",
		Url: &model.SourceUrl{MySQL: &model.MySQLUrl{User: "root", Password: "password", Host: "127.0.0.1", Database: "data_workbench", Port: 3306}}}
	NameErrorManager = request.CreateSource{SourceID: "som-000000nameerror", SpaceID: spaceid, SourceType: constants.SourceTypeMysql, Name: "mysql.mysql",
		Url: &model.SourceUrl{MySQL: &model.MySQLUrl{User: "root", Password: "password", Host: "127.0.0.1", Database: "data_workbench", Port: 3306}}}
	SourceTypeErrorManager = request.CreateSource{SourceID: "som-0sourcetypeerror", SpaceID: spaceid, SourceType: "SourceTypeError", Name: "SourceTypeError",
		Url: &model.SourceUrl{MySQL: &model.MySQLUrl{User: "root", Password: "password", Host: "127.0.0.1", Database: "data_workbench", Port: 3306}}}

	//// Source Tables
	//TablePG = request.CreateTable{ID: "sot-0123456789012345", SourceID: PGManager.ID, Name: "pd", Comment: "postgresql", Url: typeToJsonString(constants.FlinkTableDefinePostgreSQL{SqlColumn: []constants.SqlColumnType{constants.SqlColumnType{Name: "id", Type: "bigint", PrimaryKey: "t"}, constants.SqlColumnType{Name: "id1", Type: "bigint", Comment: "xxx", PrimaryKey: "f"}}})}
	//TableNameExists = request.CreateTable{ID: "sot-0123456789012351", SourceID: MysqlManager.ID, Name: "ms", Comment: "to pd", Url: typeToJsonString(constants.FlinkTableDefineMysql{SqlColumn: []constants.SqlColumnType{constants.SqlColumnType{Name: "id", Type: "bigint", PrimaryKey: "t"}, constants.SqlColumnType{Name: "id1", Type: "bigint", Comment: "xxx", PrimaryKey: "f"}}})}
	//TableNameError = request.CreateTable{ID: "sot-0123456789012352", SourceID: MysqlManager.ID, Name: "ms.ms", Comment: "to pd", Url: typeToJsonString(constants.FlinkTableDefineMysql{SqlColumn: []constants.SqlColumnType{constants.SqlColumnType{Name: "id", Type: "bigint", PrimaryKey: "t"}, constants.SqlColumnType{Name: "id1", Type: "bigint", Comment: "xxx", PrimaryKey: "f"}}})}
	//TableJsonError = request.CreateTable{ID: "sot-0123456789012353", SourceID: MysqlManager.ID, Name: "ms1", Comment: "to pd", Url: "sss,xx,xx, xx"}
	//TableManagerError = request.CreateTable{ID: "sot-0123456789012354", SourceID: "sot-xxxxyyyyzzzzxxxx", Name: "ms2", Comment: "to pd", Url: typeToJsonString(constants.FlinkTableDefineMysql{SqlColumn: []constants.SqlColumnType{constants.SqlColumnType{Name: "id", Type: "bigint", PrimaryKey: "t"}, constants.SqlColumnType{Name: "id1", Type: "bigint", Comment: "xxx", PrimaryKey: "f"}}})}
	//TableMysqlDimensionSource = request.CreateTable{ID: "sot-0123456789012355", SourceID: MysqlManager.ID, Name: "mw", Comment: "join dimension table", Url: typeToJsonString(constants.FlinkTableDefineMysql{SqlColumn: []constants.SqlColumnType{constants.SqlColumnType{Name: "rate", Type: "bigint", PrimaryKey: "f"}, constants.SqlColumnType{Name: "dbmoney", Type: "varchar", Length: "8", Comment: "xxx"}}})}
	//TableMysqlDimensionDest = request.CreateTable{ID: "sot-0123456789012356", SourceID: MysqlManager.ID, Name: "mwd", Comment: "join dimension table", Url: typeToJsonString(constants.FlinkTableDefineMysql{SqlColumn: []constants.SqlColumnType{constants.SqlColumnType{Name: "total", Type: "bigint", PrimaryKey: "f"}}})}
	//TableMysqlCommonSource = request.CreateTable{ID: "sot-0123456789012357", SourceID: MysqlManager.ID, Name: "mc", Comment: "join common table", Url: typeToJsonString(constants.FlinkTableDefineMysql{SqlColumn: []constants.SqlColumnType{constants.SqlColumnType{Name: "rate", Type: "bigint", PrimaryKey: "f"}, constants.SqlColumnType{Name: "dbmoney", Type: "varchar", Comment: "xxx", PrimaryKey: "f", Length: "8"}}})}
	//TableMysqlCommonDest = request.CreateTable{ID: "sot-0123456789012358", SourceID: MysqlManager.ID, Name: "mcd", Comment: "join common table", Url: typeToJsonString(constants.FlinkTableDefineMysql{SqlColumn: []constants.SqlColumnType{constants.SqlColumnType{Name: "total", Type: "bigint"}}})} //'connector.write.flush.max-rows' = '1'
	//TableS3Source = request.CreateTable{ID: "sot-0123456789012359", SourceID: S3Manager.ID, Name: "s3s", Comment: "s3 source", Url: typeToJsonString(constants.FlinkTableDefineS3{SqlColumn: []constants.SqlColumnType{constants.SqlColumnType{Name: "id", Type: "bigint", PrimaryKey: "t"}, constants.SqlColumnType{Name: "id1", Type: "bigint", Comment: "xxx", PrimaryKey: "f"}}, Path: "s3a://filesystem/source", Format: "json"})}
	//TableS3Dest = request.CreateTable{ID: "sot-0123456789012360", SourceID: S3Manager.ID, Name: "s3d", Comment: "s3 destination", Url: typeToJsonString(constants.FlinkTableDefineS3{SqlColumn: []constants.SqlColumnType{constants.SqlColumnType{Name: "id", Type: "bigint", PrimaryKey: "t"}, constants.SqlColumnType{Name: "id1", Type: "bigint", Comment: "xxx", PrimaryKey: "f"}}, Path: "s3a://filesystem/destination", Format: "json"})}
	//TableUDFSource = request.CreateTable{ID: "sot-0123456789012362", SourceID: MysqlManager.ID, Name: "udfs", Comment: "udfs", Url: typeToJsonString(constants.FlinkTableDefineMysql{SqlColumn: []constants.SqlColumnType{constants.SqlColumnType{Name: "a", Type: "varchar", PrimaryKey: "f", Length: "10"}}})}
	//TableUDFDest = request.CreateTable{ID: "sot-0123456789012363", SourceID: MysqlManager.ID, Name: "udfd", Comment: "udfd", Url: typeToJsonString(constants.FlinkTableDefineMysql{SqlColumn: []constants.SqlColumnType{constants.SqlColumnType{Name: "a", Type: "varchar", PrimaryKey: "f", Length: "10"}}})}
	//TableHbaseSource = request.CreateTable{ID: "sot-0123456789012364", SourceID: HbaseManager.ID, Name: "testsource", Comment: "hbase source", Url: typeToJsonString(constants.FlinkTableDefineHbase{SqlColumn: []constants.SqlColumnType{constants.SqlColumnType{Name: "rowkey", Type: "STRING", PrimaryKey: "f", Length: ""}, constants.SqlColumnType{Name: "columna", Type: "ROW<a STRING>"}}})}
	//TableHbaseDest = request.CreateTable{ID: "sot-0123456789012365", SourceID: HbaseManager.ID, Name: "testdest", Comment: "hbase dest", Url: typeToJsonString(constants.FlinkTableDefineHbase{SqlColumn: []constants.SqlColumnType{constants.SqlColumnType{Name: "rowkey", Type: "STRING", PrimaryKey: "f", Length: ""}, constants.SqlColumnType{Name: "columna", Type: "ROW<a STRING>"}}})}
	//TableFtpSource = request.CreateTable{ID: "sot-0123456789012366", SourceID: FtpManager.ID, Name: "ftpsource", Comment: "ftp source", Url: typeToJsonString(constants.FlinkTableDefineFtp{Path: "/u/", Format: "csv", SqlColumn: []constants.SqlColumnType{constants.SqlColumnType{Name: "readName", Type: "string", Comment: "xxx"}, constants.SqlColumnType{Name: "cellPhone", Type: "string", Comment: "xxx"}, {Name: "universityName", Type: "string", Comment: "xxx"}, {Name: "city", Type: "string", Comment: "xxx"}, {Name: "street", Type: "string", Comment: "xxx"}, {Name: "ip", Type: "string", Comment: "xxx"}, {Name: "pt", Type: "AS PROCTIME()"}}, ConnectorOptions: []string{"'username' = 'ftptest'", "'password' = '123456'"}})}
	//TableFtpDest = request.CreateTable{ID: "sot-0123456789012367", SourceID: FtpManager.ID, Name: "ftpdest", Comment: "ftp dest", Url: typeToJsonString(constants.FlinkTableDefineFtp{Path: "/sink.csv", Format: "csv", SqlColumn: []constants.SqlColumnType{constants.SqlColumnType{Name: "readName", Type: "string", Comment: "xxx"}, constants.SqlColumnType{Name: "cellPhone", Type: "string", Comment: "xxx"}, {Name: "universityName", Type: "string", Comment: "xxx"}, {Name: "city", Type: "string", Comment: "xxx"}, {Name: "street", Type: "string", Comment: "xxx"}, {Name: "ip", Type: "string", Comment: "xxx"}}, ConnectorOptions: []string{"'username' = 'ftptest'", "'password' = '123456'"}})}

	address := "127.0.0.1:9104"
	lp := glog.NewDefault()
	ctx = glog.WithContext(context.Background(), lp)

	conn, err := grpcwrap.NewConn(ctx, &grpcwrap.ClientConfig{
		Address: address,
	})
	require.Nil(t, err, "%+v", err)
	client = smpb.NewSourcemanagerClient(conn)

	logger := glog.NewDefault()
	worker := idgenerator.New("")
	reqId, _ := worker.Take()

	ln := logger.Clone()
	ln.WithFields().AddString("rid", reqId)

	ctx = grpcwrap.ContextWithRequest(context.Background(), ln, reqId)
}

func errorCode(err error) string {
	//rpc error: code = Unknown desc = InvalidSourceName
	return strings.Split(err.Error(), " ")[7]
}

// Source Manager
func Test_CreateSource(t *testing.T) {
	mainInit(t)
	Clean(t)
	var err error

	_, err = client.Create(ctx, &MysqlManager)
	require.Nil(t, err, "%+v", err)
	_, err = client.Create(ctx, &ClickHouseManager)
	require.Nil(t, err, "%+v", err)
	_, err = client.Create(ctx, &PGManager)
	require.Nil(t, err, "%+v", err)
	_, err = client.Create(ctx, &KafkaManager)
	require.Nil(t, err, "%+v", err)
	_, err = client.Create(ctx, &S3Manager)
	require.Nil(t, err, "%+v", err)
	_, err = client.Create(ctx, &HbaseManager)
	require.Nil(t, err, "%+v", err)
	_, err = client.Create(ctx, &FtpManager)
	require.Nil(t, err, "%+v", err)
	_, err = client.Create(ctx, &HDFSManager)
	require.Nil(t, err, "%+v", err)
	_, err = client.Create(ctx, &NewSpaceManager)
	require.Nil(t, err, "%+v", err)
	_, err = client.Create(ctx, &NameErrorManager)
	require.Equal(t, qerror.InvalidSourceName.Code(), errorCode(err))
	_, err = client.Create(ctx, &NameExistsManager)
	require.Equal(t, qerror.ResourceAlreadyExists.Code(), errorCode(err))
	_, err = client.Create(ctx, &SourceTypeErrorManager)
	require.Equal(t, qerror.NotSupportSourceType.Code(), errorCode(err))
}

func managerDescribe(t *testing.T, id string) *response.DescribeSource {
	var d request.DescribeSource
	var err error
	var rep *response.DescribeSource

	if id == "" {
		d.SourceID = MysqlManager.SourceID
		rep, err = client.Describe(ctx, &d)
		require.Nil(t, err, "%+v", err)
		require.Equal(t, rep.Info.SourceID, d.SourceID)
	} else {
		d.SourceID = id
		rep, err = client.Describe(ctx, &d)
		require.Nil(t, err, "%+v", err)
		require.Equal(t, rep.Info.SourceID, d.SourceID)
		return rep
	}

	return nil
}

func Test_DescribeSource(t *testing.T) {
	mainInit(t)
	managerDescribe(t, "")
}

func Test_UpdateSource(t *testing.T) {
	mainInit(t)
	var i request.UpdateSource
	var err error

	i.Name = MysqlManager.Name
	i.SourceID = MysqlManager.SourceID
	i.Comment = "update ok"
	i.SourceType = MysqlManager.SourceType
	i.Url = MysqlManager.Url

	_, err = client.Update(ctx, &i)
	require.Nil(t, err, "%+v", err)
	require.Equal(t, i.Comment, managerDescribe(t, i.SourceID).Info.Comment)
}

func Test_PingSource(t *testing.T) {
	mainInit(t)

	var p request.PingSource
	var err error

	p.SourceType = MysqlManager.SourceType
	p.Url = MysqlManager.Url
	_, err = client.PingSource(ctx, &p)
	require.Nil(t, err, "%+v", err)

	//p.SourceType = PGManager.SourceType
	//p.Url = PGManager.Url
	//_, err = client.PingSource(ctx, &p)
	//require.NotNil(t, err, "%+v", err)

	//p.SourceType = ClickHouseManager.SourceType
	//p.Url = ClickHouseManager.Url
	//_, err = client.PingSource(ctx, &p)
	//require.Nil(t, err, "%+v", err)

	//p.SourceType = KafkaManager.SourceType
	//p.Url = KafkaManager.Url
	//_, err = client.PingSource(ctx, &p)
	//require.Nil(t, err, "%+v", err)

	p.SourceType = S3Manager.SourceType
	p.Url = S3Manager.Url
	_, err = client.PingSource(ctx, &p)
	require.NotNil(t, err, "%+v", err)

	//p.SourceType = HbaseManager.SourceType
	//p.Url = HbaseManager.Url
	//_, err = client.PingSource(ctx, &p)
	//require.Nil(t, err, "%+v", err)

	//p.SourceType = FtpManager.SourceType
	//p.Url = FtpManager.Url
	//_, err = client.PingSource(ctx, &p)
	//require.Nil(t, err, "%+v", err)

	//p.SourceType = HDFSManager.SourceType
	//p.Url = HDFSManager.Url
	//_, err = client.PingSource(ctx, &p)
	//require.Nil(t, err, "%+v", err)
}

func Test_DisableSource(t *testing.T) {
	mainInit(t)
	var v request.DisableSource
	var err error

	v.SourceIDs = []string{MysqlManager.SourceID, KafkaManager.SourceID}

	_, err = client.Disable(ctx, &v)
	require.Nil(t, err, "%+v", err)

	var i request.UpdateSource

	i.Name = MysqlManager.Name
	i.SourceID = MysqlManager.SourceID
	i.Comment = "update ok"
	i.SourceType = MysqlManager.SourceType
	i.Url = MysqlManager.Url

	_, err = client.Update(ctx, &i)
	require.NotNil(t, err, "%+v", err)
	require.Equal(t, qerror.SourceIsDisable.Code(), errorCode(err))
}

func Test_EnableSource(t *testing.T) {
	mainInit(t)
	var v request.EnableSource
	var err error

	v.SourceIDs = []string{MysqlManager.SourceID, KafkaManager.SourceID}

	_, err = client.Enable(ctx, &v)
	require.Nil(t, err, "%+v", err)
}

func Test_SourceKind(t *testing.T) {
	mainInit(t)

	_, err := client.SourceKind(ctx, &model.EmptyStruct{})
	require.Nil(t, err, "%+v", err)
}

func Test_DataFormat(t *testing.T) {
	mainInit(t)

	_, err := client.DataFormat(ctx, &model.EmptyStruct{})
	require.Nil(t, err, "%+v", err)
}

func Test_DataType(t *testing.T) {
	mainInit(t)

	_, err := client.DataType(ctx, &model.EmptyStruct{})
	require.Nil(t, err, "%+v", err)
}

func Test_CreateTable(t *testing.T) {
	var err error
	mainInit(t)

	_, err = client.CreateTable(ctx, &MysqlSource)
	require.Nil(t, err, "%+v", err)
	_, err = client.CreateTable(ctx, &MysqlDest)
	require.Nil(t, err, "%+v", err)
	_, err = client.CreateTable(ctx, &ClickHouseSource)
	require.Nil(t, err, "%+v", err)
	_, err = client.CreateTable(ctx, &ClickHouseDest)
	require.Nil(t, err, "%+v", err)
	_, err = client.CreateTable(ctx, &KafkaSource)
	require.Nil(t, err, "%+v", err)
}

func tablesDescribe(t *testing.T, id string) *model.TableInfo {
	var i request.DescribeTable
	var err error
	var rep *response.DescribeTable

	if id == "" {
		i.TableID = MysqlSource.TableID
		rep, err = client.DescribeTable(ctx, &i)
		require.Nil(t, err, "%+v", err)
	} else {
		i.TableID = id
		rep, err = client.DescribeTable(ctx, &i)
		require.Nil(t, err, "%+v", err)
		return rep.Info
	}
	return nil
}

func Test_DescribeTable(t *testing.T) {
	mainInit(t)
	tablesDescribe(t, "")
}

func Test_UpdateTable(t *testing.T) {
	var i request.UpdateTable
	var err error
	mainInit(t)

	i.Comment = "Update"
	i.TableID = MysqlSource.TableID
	i.Name = MysqlSource.Name
	i.Define = MysqlSource.Define
	i.TableKind = MysqlSource.TableKind

	_, err = client.UpdateTable(ctx, &i)
	require.Nil(t, err, "%+v", err)
	require.Equal(t, i.Comment, tablesDescribe(t, i.TableID).Comment)
}

func tablesDelete(t *testing.T, id string) {
	var i request.DeleteTable
	var err error

	if id == "" {
		i.TableIDs = []string{MysqlSource.TableID, MysqlDest.TableID, ClickHouseSource.TableID, ClickHouseDest.TableID, KafkaSource.TableID}
		_, err = client.DeleteTable(ctx, &i)
		require.Nil(t, err, "%+v", err)
	} else {
		i.TableIDs = []string{id}
		_, err = client.DeleteTable(ctx, &i)
		require.Nil(t, err, "%+v", err)
	}
}

func managerLists(t *testing.T, SpaceID string) *response.ListSource {
	var i request.ListSource
	var rep *response.ListSource
	var err error

	if SpaceID == "" {
		i.SpaceID = spaceid
		i.Limit = 100
		i.Offset = 0
		rep, err = client.List(ctx, &i)
		require.Nil(t, err, "%+v", err)

		i.SpaceID = newspaceid
		i.Limit = 100
		i.Offset = 0
		i.Search = "my"
		rep, err = client.List(ctx, &i)
		require.Nil(t, err, "%+v", err)
		require.Equal(t, 1, len(rep.Infos))
		require.Equal(t, int64(1), rep.Total)

		return nil
	} else {
		i.SpaceID = SpaceID
		i.Limit = 100
		i.Offset = 0
		rep, err = client.List(ctx, &i)
		require.Nil(t, err, "%+v", err)
		return rep
	}
	return nil
}

func Test_ListSource(t *testing.T) {
	mainInit(t)
	managerLists(t, "")
}

func tablesLists(t *testing.T, SourceID string) *response.ListTable {
	var i request.ListTable
	var err error
	var rep *response.ListTable

	if SourceID == "" {
		i.SpaceID = spaceid
		i.Limit = 100
		i.Offset = 0
		rep, err = client.ListTable(ctx, &i)
		require.Nil(t, err, "%+v", err)

		i.SourceID = MysqlManager.SourceID
		i.Limit = 100
		i.Offset = 0
		i.Search = "m"
		rep, err = client.ListTable(ctx, &i)
		require.Nil(t, err, "%+v", err)
		require.Equal(t, 2, len(rep.Infos))
		require.Equal(t, int64(2), rep.Total)

		i.SpaceID = spaceid
		i.SourceID = MysqlManager.SourceID
		i.TableKind = "source"
		i.Limit = 100
		i.Offset = 0
		i.Search = "m"
		rep, err = client.ListTable(ctx, &i)
		require.Nil(t, err, "%+v", err)
		require.Equal(t, 1, len(rep.Infos))
		require.Equal(t, int64(1), rep.Total)
	} else {
		i.SourceID = SourceID
		i.Limit = 100
		i.Offset = 0
		rep, err = client.ListTable(ctx, &i)
		require.Nil(t, err, "%+v", err)
		return rep
	}
	return nil
}

func Test_ListTable(t *testing.T) {
	mainInit(t)
	tablesLists(t, "")
}

func managerDelete(t *testing.T, id string, iserror bool) {
	var i request.DeleteSource
	var err error

	if id == "" {
		if iserror == false {
			i.SourceIDs = []string{MysqlManager.SourceID}
			_, err = client.Delete(ctx, &i)
			require.Nil(t, err, "%+v", err)
			Clean(t)
		} else {
			i.SourceIDs = []string{MysqlManager.SourceID}
			_, err = client.Delete(ctx, &i)
			require.NotNil(t, err, "%+v", err)
			require.Equal(t, qerror.ResourceIsUsing.Code(), errorCode(err))
		}

	} else {
		i.SourceIDs = []string{id}
		_, err = client.Delete(ctx, &i)
		require.Nil(t, err, "%+v", err)
	}
}

func Clean(t *testing.T) {
	var (
		d request.DeleteWorkspaces
	)

	d.SpaceIds = []string{spaceid, newspaceid}
	_, err := client.DeleteAll(ctx, &d)
	require.Nil(t, err, "%+v", err)
}

func Test_SourceTables(t *testing.T) {
	var v request.SourceTables
	var err error

	mainInit(t)

	v.SourceID = MysqlManager.SourceID
	_, err = client.SourceTables(ctx, &v)
	require.Nil(t, err, "%+v", err)

	//v.SourceID = ClickHouseManager.SourceID
	//_, err = client.SourceTables(ctx, &v)
	//require.Nil(t, err, "%+v", err)

	//v.SourceID = PGManager.SourceID
	//_, err = client.SourceTables(ctx, &v)
	//require.Nil(t, err, "%+v", err)
}

func Test_TableColumns(t *testing.T) {
	var v request.TableColumns
	var err error

	mainInit(t)

	v.SourceID = MysqlManager.SourceID
	v.TableName = "sourcemanager"
	_, err = client.TableColumns(ctx, &v)
	require.Nil(t, err, "%+v", err)

	//v.SourceID = ClickHouseManager.SourceID
	//v.TableName = "zz"
	//_, err = client.TableColumns(ctx, &v)
	//require.Nil(t, err, "%+v", err)

	//v.SourceID = PGManager.SourceID
	//v.TableName = "zz"
	//_, err = client.TableColumns(ctx, &v)
	//require.Nil(t, err, "%+v", err)
}

func Test_DeleteTable(t *testing.T) {
	mainInit(t)
	tablesDelete(t, "")
}

func Test_DeleteSource(t *testing.T) {
	mainInit(t)
	managerDelete(t, "", false)
}

func Test_Clean(t *testing.T) {
	mainInit(t)
	Clean(t)
}

func Test_CreateTest(t *testing.T) {
	mainInit(t)
	Clean(t)

	//	Test_CreateSource(t)
	//	Test_CreateTable(t)
}
