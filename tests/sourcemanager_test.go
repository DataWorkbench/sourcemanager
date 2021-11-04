package tests

import (
	"context"
	"encoding/json"
	"strings"
	"testing"

	"github.com/DataWorkbench/glog"
	"github.com/stretchr/testify/require"

	"github.com/DataWorkbench/common/grpcwrap"
	"github.com/DataWorkbench/common/qerror"
	"github.com/DataWorkbench/common/utils/idgenerator"

	"github.com/DataWorkbench/gproto/pkg/datasourcepb"
	"github.com/DataWorkbench/gproto/pkg/flinkpb"
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
	MysqlManager = request.CreateSource{SourceId: "som-00000000000mysql", SpaceId: spaceid, SourceType: model.DataSource_MySQL, Name: "mysql", Comment: "",
		Url: &datasourcepb.DataSourceURL{Mysql: &datasourcepb.MySQLURL{User: "root", Password: "password", Host: "dataworkbench-db", Database: "data_workbench", Port: 3306}}}
	MysqlSource = request.CreateTable{TableId: "sot-00000mysqlsource", SourceId: MysqlManager.SourceId, SpaceId: spaceid, Name: "ms", Comment: "mysql", TableKind: model.TableInfo_Source, TableSchema: &flinkpb.TableSchema{Mysql: &flinkpb.MySQLTable{SqlColumn: []*flinkpb.SqlColumnType{&flinkpb.SqlColumnType{Column: "id", Type: "bigint", PrimaryKey: "t"}, &flinkpb.SqlColumnType{Column: "id1", Type: "bigint", Comment: "xxx", PrimaryKey: "f"}}}}}
	MysqlDest = request.CreateTable{TableId: "sot-0000000mysqldest", SourceId: MysqlManager.SourceId, SpaceId: spaceid, Name: "md", Comment: "mysql dest", TableKind: model.TableInfo_Destination, TableSchema: &flinkpb.TableSchema{Mysql: &flinkpb.MySQLTable{SqlColumn: []*flinkpb.SqlColumnType{&flinkpb.SqlColumnType{Column: "id", Type: "bigint", PrimaryKey: "t"}, &flinkpb.SqlColumnType{Column: "id1", Type: "bigint", Comment: "xxx", PrimaryKey: "f"}}}}}

	// ClickHouse
	// create table cks(paycount bigint, paymoney varchar(10)) ENGINE=TinyLog;
	// create table zz(id bigint, id1 bigint, t timestamp, v varchar(10), primary key (id)) engine=MergeTree;
	ClickHouseManager = request.CreateSource{SourceId: "som-000000clickhouse", SpaceId: spaceid, SourceType: model.DataSource_ClickHouse, Name: "clickhouse", Comment: "clickhouse", Url: &datasourcepb.DataSourceURL{Clickhouse: &datasourcepb.ClickHouseURL{User: "default", Password: "", Host: "dataworkbench-db", Port: 8123, Database: "default"}}}
	ClickHouseSource = request.CreateTable{TableId: "sot-clickhousesource", SourceId: ClickHouseManager.SourceId, SpaceId: spaceid, Name: "cks", Comment: "cksource", TableKind: model.TableInfo_Source, TableSchema: &flinkpb.TableSchema{Clickhouse: &flinkpb.ClickHouseTable{SqlColumn: []*flinkpb.SqlColumnType{&flinkpb.SqlColumnType{Column: "paycount", Type: "bigint", PrimaryKey: "t"}, &flinkpb.SqlColumnType{Column: "paymoney", Type: "varchar", Length: "10", Comment: "xxx", PrimaryKey: "f"}}}}}
	ClickHouseDest = request.CreateTable{TableId: "sot-00clickhousedest", SourceId: ClickHouseManager.SourceId, SpaceId: spaceid, Name: "ckd", Comment: "ckdest", TableKind: model.TableInfo_Destination, TableSchema: &flinkpb.TableSchema{Clickhouse: &flinkpb.ClickHouseTable{SqlColumn: []*flinkpb.SqlColumnType{&flinkpb.SqlColumnType{Column: "paycount", Type: "bigint", PrimaryKey: "t"}, &flinkpb.SqlColumnType{Column: "paymoney", Type: "varchar", Length: "10", Comment: "xxx", PrimaryKey: "f"}}}}}

	// PostgreSQL
	PGManager = request.CreateSource{SourceId: "som-000000postgresql", SpaceId: spaceid, SourceType: model.DataSource_PostgreSQL, Name: "pg", Comment: "",
		Url: &datasourcepb.DataSourceURL{Postgresql: &datasourcepb.PostgreSQLURL{User: "lzzhang", Password: "lzzhang", Host: "dataworkbench-db", Database: "lzzhang", Port: 5432}}}
	//PGSource = request.CreateTable{TableId: "sot-postgresqlsource", SourceId: PGManager.SourceId, SpaceId: spaceid, Name: "pgs", Comment: "pgs", TableKind: model.TableInfo_Source, TableSchema: &flinkpb.TableSchema{MySQL: &model.MySQLTableDefine{SqlColumn: []*flinkpb.SqlColumnType{&flinkpb.SqlColumnType{Column: "id", Type: "bigint", PrimaryKey: "t"}, &flinkpb.SqlColumnType{Column: "id1", Type: "bigint", Comment: "xxx", PrimaryKey: "f"}}}}}
	//PGDest = request.CreateTable{TableId: "sot-00postgresqldest", SourceId: PGManager.SourceId, SpaceId: spaceid, Name: "pgd", Comment: "pgd", TableKind: model.TableInfo_Destination, TableSchema: &flinkpb.TableSchema{MySQL: &model.MySQLTableDefine{SqlColumn: []*flinkpb.SqlColumnType{&flinkpb.SqlColumnType{Column: "id", Type: "bigint", PrimaryKey: "t"}, &flinkpb.SqlColumnType{Column: "id1", Type: "bigint", Comment: "xxx", PrimaryKey: "f"}}}}}

	// kafka {"paycount": 2, "paymoney": "EUR"} {"paycount": 1, "paymoney": "USD"}
	KafkaManager = request.CreateSource{SourceId: "som-00000000000kafka", SpaceId: spaceid, SourceType: model.DataSource_Kafka, Name: "kafka", Comment: "",
		Url: &datasourcepb.DataSourceURL{Kafka: &datasourcepb.KafkaURL{KafkaBrokers: "dataworkbench-kafka-for-test:9092"}}}
	KafkaSource = request.CreateTable{TableId: "sot-00000kafkasource", SourceId: KafkaManager.SourceId, SpaceId: spaceid, Name: "billing", Comment: "", TableKind: model.TableInfo_Source, TableSchema: &flinkpb.TableSchema{Kafka: &flinkpb.KafkaTable{SqlColumn: []*flinkpb.SqlColumnType{&flinkpb.SqlColumnType{Column: "paycount", Type: "bigint", PrimaryKey: "f"}, &flinkpb.SqlColumnType{Column: "paymoney", Type: "string", Comment: "", PrimaryKey: "f"}}, TimeColumn: []*flinkpb.SqlTimeColumnType{&flinkpb.SqlTimeColumnType{Column: "tproctime", Type: "proctime"}}, Topic: "workbench", Format: "json", ConnectorOptions: []*flinkpb.ConnectorOption{&flinkpb.ConnectorOption{Name: "'json.fail-on-missing-field'", Value: "'false'"}, &flinkpb.ConnectorOption{Name: "'json.ignore-parse-errors'", Value: "'true'"}}}}}

	S3Manager = request.CreateSource{SourceId: "som-00000000000000s3", SpaceId: spaceid, SourceType: model.DataSource_S3, Name: "s3",
		Url: &datasourcepb.DataSourceURL{S3: &datasourcepb.S3URL{}}}
	//Url: &datasourcepb.DataSourceURL{S3: &model.S3Url{AccessKey: "RDTHDPNFWWDNWPIHESWK", SecretKey: "sVbVhAUsKGPPdiTOPAgveqCNhFjtvXFNpsPnQ7Hx", EndPoint: "http://s3.gd2.qingstor.com"}}}
	HbaseManager = request.CreateSource{SourceId: "som-00000000000hbase", SpaceId: spaceid, SourceType: model.DataSource_HBase, Name: "hbase",
		Url: &datasourcepb.DataSourceURL{Hbase: &datasourcepb.HBaseURL{Zookeeper: "hbase:2181", ZNode: "/hbase"}}}
	FtpManager = request.CreateSource{SourceId: "som-0000000000000ftp", SpaceId: spaceid, SourceType: model.DataSource_Ftp, Name: "ftp",
		Url: &datasourcepb.DataSourceURL{Ftp: &datasourcepb.FtpURL{Host: "42.193.101.183", Port: 21}}}
	HDFSManager = request.CreateSource{SourceId: "som-000000000000hdfs", SpaceId: spaceid, SourceType: model.DataSource_HDFS, Name: "hdfs",
		Url: &datasourcepb.DataSourceURL{Hdfs: &datasourcepb.HDFSURL{Nodes: &datasourcepb.HDFSURL_HDFSNodeURL{NameNode: "dataworkbench-db", Port: 8020}}}}

	NewSpaceManager = request.CreateSource{SourceId: "som-00000000newspace", SpaceId: newspaceid, SourceType: model.DataSource_MySQL, Name: "mysql", Comment: "newspace",
		Url: &datasourcepb.DataSourceURL{Mysql: &datasourcepb.MySQLURL{User: "root", Password: "password", Host: "dataworkbench-db", Database: "data_workbench", Port: 3306}}}
	NameExistsManager = request.CreateSource{SourceId: "som-000000nameexists", SpaceId: spaceid, SourceType: model.DataSource_MySQL, Name: "mysql",
		Url: &datasourcepb.DataSourceURL{Mysql: &datasourcepb.MySQLURL{User: "root", Password: "password", Host: "dataworkbench-db", Database: "data_workbench", Port: 3306}}}
	NameErrorManager = request.CreateSource{SourceId: "som-000000nameerror", SpaceId: spaceid, SourceType: model.DataSource_MySQL, Name: "mysql.mysql",
		Url: &datasourcepb.DataSourceURL{Mysql: &datasourcepb.MySQLURL{User: "root", Password: "password", Host: "dataworkbench-db", Database: "data_workbench", Port: 3306}}}
	SourceTypeErrorManager = request.CreateSource{SourceId: "som-0sourcetypeerror", SpaceId: spaceid, SourceType: 10000, Name: "SourceTypeError",
		Url: &datasourcepb.DataSourceURL{Mysql: &datasourcepb.MySQLURL{User: "root", Password: "password", Host: "dataworkbench-db", Database: "data_workbench", Port: 3306}}}

	//// Source Tables
	//TablePG = request.CreateTable{ID: "sot-0123456789012345", SourceId: PGManager.ID, Name: "pd", Comment: "postgresql", Url: typeToJsonString(constants.FlinkTableDefinePostgreSQL{SqlColumn: []constants.SqlColumnType{constants.SqlColumnType{Name: "id", Type: "bigint", PrimaryKey: "t"}, constants.SqlColumnType{Name: "id1", Type: "bigint", Comment: "xxx", PrimaryKey: "f"}}})}
	//TableNameExists = request.CreateTable{ID: "sot-0123456789012351", SourceId: MysqlManager.ID, Name: "ms", Comment: "to pd", Url: typeToJsonString(constants.FlinkTableDefineMysql{SqlColumn: []constants.SqlColumnType{constants.SqlColumnType{Name: "id", Type: "bigint", PrimaryKey: "t"}, constants.SqlColumnType{Name: "id1", Type: "bigint", Comment: "xxx", PrimaryKey: "f"}}})}
	//TableNameError = request.CreateTable{ID: "sot-0123456789012352", SourceId: MysqlManager.ID, Name: "ms.ms", Comment: "to pd", Url: typeToJsonString(constants.FlinkTableDefineMysql{SqlColumn: []constants.SqlColumnType{constants.SqlColumnType{Name: "id", Type: "bigint", PrimaryKey: "t"}, constants.SqlColumnType{Name: "id1", Type: "bigint", Comment: "xxx", PrimaryKey: "f"}}})}
	//TableJsonError = request.CreateTable{ID: "sot-0123456789012353", SourceId: MysqlManager.ID, Name: "ms1", Comment: "to pd", Url: "sss,xx,xx, xx"}
	//TableManagerError = request.CreateTable{ID: "sot-0123456789012354", SourceId: "sot-xxxxyyyyzzzzxxxx", Name: "ms2", Comment: "to pd", Url: typeToJsonString(constants.FlinkTableDefineMysql{SqlColumn: []constants.SqlColumnType{constants.SqlColumnType{Name: "id", Type: "bigint", PrimaryKey: "t"}, constants.SqlColumnType{Name: "id1", Type: "bigint", Comment: "xxx", PrimaryKey: "f"}}})}
	//TableMysqlDimensionSource = request.CreateTable{ID: "sot-0123456789012355", SourceId: MysqlManager.ID, Name: "mw", Comment: "join dimension table", Url: typeToJsonString(constants.FlinkTableDefineMysql{SqlColumn: []constants.SqlColumnType{constants.SqlColumnType{Name: "rate", Type: "bigint", PrimaryKey: "f"}, constants.SqlColumnType{Name: "dbmoney", Type: "varchar", Length: "8", Comment: "xxx"}}})}
	//TableMysqlDimensionDest = request.CreateTable{ID: "sot-0123456789012356", SourceId: MysqlManager.ID, Name: "mwd", Comment: "join dimension table", Url: typeToJsonString(constants.FlinkTableDefineMysql{SqlColumn: []constants.SqlColumnType{constants.SqlColumnType{Name: "total", Type: "bigint", PrimaryKey: "f"}}})}
	//TableMysqlCommonSource = request.CreateTable{ID: "sot-0123456789012357", SourceId: MysqlManager.ID, Name: "mc", Comment: "join common table", Url: typeToJsonString(constants.FlinkTableDefineMysql{SqlColumn: []constants.SqlColumnType{constants.SqlColumnType{Name: "rate", Type: "bigint", PrimaryKey: "f"}, constants.SqlColumnType{Name: "dbmoney", Type: "varchar", Comment: "xxx", PrimaryKey: "f", Length: "8"}}})}
	//TableMysqlCommonDest = request.CreateTable{ID: "sot-0123456789012358", SourceId: MysqlManager.ID, Name: "mcd", Comment: "join common table", Url: typeToJsonString(constants.FlinkTableDefineMysql{SqlColumn: []constants.SqlColumnType{constants.SqlColumnType{Name: "total", Type: "bigint"}}})} //'connector.write.flush.max-rows' = '1'
	//TableS3Source = request.CreateTable{ID: "sot-0123456789012359", SourceId: S3Manager.ID, Name: "s3s", Comment: "s3 source", Url: typeToJsonString(constants.FlinkTableDefineS3{SqlColumn: []constants.SqlColumnType{constants.SqlColumnType{Name: "id", Type: "bigint", PrimaryKey: "t"}, constants.SqlColumnType{Name: "id1", Type: "bigint", Comment: "xxx", PrimaryKey: "f"}}, Path: "s3a://filesystem/source", Format: "json"})}
	//TableS3Dest = request.CreateTable{ID: "sot-0123456789012360", SourceId: S3Manager.ID, Name: "s3d", Comment: "s3 destination", Url: typeToJsonString(constants.FlinkTableDefineS3{SqlColumn: []constants.SqlColumnType{constants.SqlColumnType{Name: "id", Type: "bigint", PrimaryKey: "t"}, constants.SqlColumnType{Name: "id1", Type: "bigint", Comment: "xxx", PrimaryKey: "f"}}, Path: "s3a://filesystem/destination", Format: "json"})}
	//TableUDFSource = request.CreateTable{ID: "sot-0123456789012362", SourceId: MysqlManager.ID, Name: "udfs", Comment: "udfs", Url: typeToJsonString(constants.FlinkTableDefineMysql{SqlColumn: []constants.SqlColumnType{constants.SqlColumnType{Name: "a", Type: "varchar", PrimaryKey: "f", Length: "10"}}})}
	//TableUDFDest = request.CreateTable{ID: "sot-0123456789012363", SourceId: MysqlManager.ID, Name: "udfd", Comment: "udfd", Url: typeToJsonString(constants.FlinkTableDefineMysql{SqlColumn: []constants.SqlColumnType{constants.SqlColumnType{Name: "a", Type: "varchar", PrimaryKey: "f", Length: "10"}}})}
	//TableHbaseSource = request.CreateTable{ID: "sot-0123456789012364", SourceId: HbaseManager.ID, Name: "testsource", Comment: "hbase source", Url: typeToJsonString(constants.FlinkTableDefineHbase{SqlColumn: []constants.SqlColumnType{constants.SqlColumnType{Name: "rowkey", Type: "STRING", PrimaryKey: "f", Length: ""}, constants.SqlColumnType{Name: "columna", Type: "ROW<a STRING>"}}})}
	//TableHbaseDest = request.CreateTable{ID: "sot-0123456789012365", SourceId: HbaseManager.ID, Name: "testdest", Comment: "hbase dest", Url: typeToJsonString(constants.FlinkTableDefineHbase{SqlColumn: []constants.SqlColumnType{constants.SqlColumnType{Name: "rowkey", Type: "STRING", PrimaryKey: "f", Length: ""}, constants.SqlColumnType{Name: "columna", Type: "ROW<a STRING>"}}})}
	//TableFtpSource = request.CreateTable{ID: "sot-0123456789012366", SourceId: FtpManager.ID, Name: "ftpsource", Comment: "ftp source", Url: typeToJsonString(constants.FlinkTableDefineFtp{Path: "/u/", Format: "csv", SqlColumn: []constants.SqlColumnType{constants.SqlColumnType{Name: "readName", Type: "string", Comment: "xxx"}, constants.SqlColumnType{Name: "cellPhone", Type: "string", Comment: "xxx"}, {Name: "universityName", Type: "string", Comment: "xxx"}, {Name: "city", Type: "string", Comment: "xxx"}, {Name: "street", Type: "string", Comment: "xxx"}, {Name: "ip", Type: "string", Comment: "xxx"}, {Name: "pt", Type: "AS PROCTIME()"}}, ConnectorOptions: []string{"'username' = 'ftptest'", "'password' = '123456'"}})}
	//TableFtpDest = request.CreateTable{ID: "sot-0123456789012367", SourceId: FtpManager.ID, Name: "ftpdest", Comment: "ftp dest", Url: typeToJsonString(constants.FlinkTableDefineFtp{Path: "/sink.csv", Format: "csv", SqlColumn: []constants.SqlColumnType{constants.SqlColumnType{Name: "readName", Type: "string", Comment: "xxx"}, constants.SqlColumnType{Name: "cellPhone", Type: "string", Comment: "xxx"}, {Name: "universityName", Type: "string", Comment: "xxx"}, {Name: "city", Type: "string", Comment: "xxx"}, {Name: "street", Type: "string", Comment: "xxx"}, {Name: "ip", Type: "string", Comment: "xxx"}}, ConnectorOptions: []string{"'username' = 'ftptest'", "'password' = '123456'"}})}

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
		d.SourceId = MysqlManager.SourceId
		rep, err = client.Describe(ctx, &d)
		require.Nil(t, err, "%+v", err)
		require.Equal(t, rep.Info.SourceId, d.SourceId)
	} else {
		d.SourceId = id
		rep, err = client.Describe(ctx, &d)
		require.Nil(t, err, "%+v", err)
		require.Equal(t, rep.Info.SourceId, d.SourceId)
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
	i.SourceId = MysqlManager.SourceId
	i.Comment = "update ok"
	i.SourceType = MysqlManager.SourceType
	i.Url = MysqlManager.Url

	_, err = client.Update(ctx, &i)
	require.Nil(t, err, "%+v", err)
	require.Equal(t, i.Comment, managerDescribe(t, i.SourceId).Info.Comment)
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

	v.SourceIds = []string{MysqlManager.SourceId, KafkaManager.SourceId}

	_, err = client.Disable(ctx, &v)
	require.Nil(t, err, "%+v", err)

	var i request.UpdateSource

	i.Name = MysqlManager.Name
	i.SourceId = MysqlManager.SourceId
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

	v.SourceIds = []string{MysqlManager.SourceId, KafkaManager.SourceId}

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
		i.TableId = MysqlSource.TableId
		rep, err = client.DescribeTable(ctx, &i)
		require.Nil(t, err, "%+v", err)
	} else {
		i.TableId = id
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
	i.TableId = MysqlSource.TableId
	i.Name = MysqlSource.Name
	i.TableSchema = MysqlSource.TableSchema
	i.TableKind = MysqlSource.TableKind

	_, err = client.UpdateTable(ctx, &i)
	require.Nil(t, err, "%+v", err)
	require.Equal(t, i.Comment, tablesDescribe(t, i.TableId).Comment)
}

func tablesDelete(t *testing.T, id string) {
	var i request.DeleteTable
	var err error

	if id == "" {
		i.TableIds = []string{MysqlSource.TableId, MysqlDest.TableId, ClickHouseSource.TableId, ClickHouseDest.TableId, KafkaSource.TableId}
		_, err = client.DeleteTable(ctx, &i)
		require.Nil(t, err, "%+v", err)
	} else {
		i.TableIds = []string{id}
		_, err = client.DeleteTable(ctx, &i)
		require.Nil(t, err, "%+v", err)
	}
}

func managerLists(t *testing.T, SpaceId string) *response.ListSource {
	var i request.ListSource
	var rep *response.ListSource
	var err error

	if SpaceId == "" {
		i.SpaceId = spaceid
		i.Limit = 100
		i.Offset = 0
		rep, err = client.List(ctx, &i)
		require.Nil(t, err, "%+v", err)

		i.SpaceId = newspaceid
		i.Limit = 100
		i.Offset = 0
		i.Search = "my"
		rep, err = client.List(ctx, &i)
		require.Nil(t, err, "%+v", err)
		require.Equal(t, 1, len(rep.Infos))
		require.Equal(t, int64(1), rep.Total)

		return nil
	} else {
		i.SpaceId = SpaceId
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

func tablesLists(t *testing.T, SourceId string) *response.ListTable {
	var i request.ListTable
	var err error
	var rep *response.ListTable

	if SourceId == "" {
		i.SpaceId = spaceid
		i.Limit = 100
		i.Offset = 0
		rep, err = client.ListTable(ctx, &i)
		require.Nil(t, err, "%+v", err)

		i.SourceId = MysqlManager.SourceId
		i.Limit = 100
		i.Offset = 0
		i.Search = "m"
		rep, err = client.ListTable(ctx, &i)
		require.Nil(t, err, "%+v", err)
		require.Equal(t, 2, len(rep.Infos))
		require.Equal(t, int64(2), rep.Total)

		i.SpaceId = spaceid
		i.SourceId = MysqlManager.SourceId
		i.TableKind = model.TableInfo_Source
		i.Limit = 100
		i.Offset = 0
		i.Search = "m"
		rep, err = client.ListTable(ctx, &i)
		require.Nil(t, err, "%+v", err)
		require.Equal(t, 1, len(rep.Infos))
		require.Equal(t, int64(1), rep.Total)
	} else {
		i.SourceId = SourceId
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
			i.SourceIds = []string{MysqlManager.SourceId}
			_, err = client.Delete(ctx, &i)
			require.Nil(t, err, "%+v", err)
			Clean(t)
		} else {
			i.SourceIds = []string{MysqlManager.SourceId}
			_, err = client.Delete(ctx, &i)
			require.NotNil(t, err, "%+v", err)
			require.Equal(t, qerror.ResourceIsUsing.Code(), errorCode(err))
		}

	} else {
		i.SourceIds = []string{id}
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

	v.SourceId = MysqlManager.SourceId
	_, err = client.SourceTables(ctx, &v)
	require.Nil(t, err, "%+v", err)

	//v.SourceId = ClickHouseManager.SourceId
	//_, err = client.SourceTables(ctx, &v)
	//require.Nil(t, err, "%+v", err)

	//v.SourceId = PGManager.SourceId
	//_, err = client.SourceTables(ctx, &v)
	//require.Nil(t, err, "%+v", err)
}

func Test_TableColumns(t *testing.T) {
	var v request.TableColumns
	var err error

	mainInit(t)

	v.SourceId = MysqlManager.SourceId
	v.TableName = "sourcemanager"
	_, err = client.TableColumns(ctx, &v)
	require.Nil(t, err, "%+v", err)

	//v.SourceId = ClickHouseManager.SourceId
	//v.TableName = "zz"
	//_, err = client.TableColumns(ctx, &v)
	//require.Nil(t, err, "%+v", err)

	//v.SourceId = PGManager.SourceId
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
