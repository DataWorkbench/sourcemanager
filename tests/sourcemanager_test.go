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

// manager
var MysqlManager request.CreateSource //name mysql
var PGManager request.CreateSource
var KafkaManager request.CreateSource
var NewSpaceManager request.CreateSource        // name mysql
var NameExistsManager request.CreateSource      //create failed
var NameErrorManager request.CreateSource       //create failed
var JsonErrorManager request.CreateSource       //create failed
var EngineTypeErrorManager request.CreateSource //create failed
var SourceTypeErrorManager request.CreateSource //create failed
var S3Manager request.CreateSource
var ClickHouseManager request.CreateSource
var HbaseManager request.CreateSource
var FtpManager request.CreateSource

var TablePG request.CreateTable
var TableKafka request.CreateTable
var TableMysqlSource request.CreateTable
var TableMysqlDest request.CreateTable
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
var TableClickHouseDest request.CreateTable // click support sink only.
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

func mainInit(t *testing.T) {
	if initDone == true {
		return
	}
	initDone = true

	// Source Manager
	// https://segmentfault.com/a/1190000039048901
	MysqlManager = request.CreateSource{SourceID: "som-0123456789012345", SpaceID: "wks-0123456789012345", SourceType: constants.SourceTypeMysql, Name: "mysql", Comment: "",
		Url: &model.SourceUrl{MySQL: &model.MySQLUrl{User: "root", Password: "password", Host: "127.0.0.1", Database: "data_workbench", Port: 3306}}}
	//ypeToJsonString(constants.SourceMysqlParams{User: "root", Password: "password", Host: "dataworkbench-db", Port: 3306, Database: "data_workbench"})}

	//PGManager = request.CreateSource{ID: "som-0123456789012346", SpaceID: "wks-0123456789012345", EngineType: constants.EngineTypeFlink, SourceType: constants.SourceTypePostgreSQL, Name: "pg", Comment: "create ok", Creator: constants.CreatorCustom, Url: typeToJsonString(constants.SourcePostgreSQLParams{User: "lzzhang", Password: "123456", Host: "127.0.0.1", Port: 5432, Database: "lzzhang"})}
	//KafkaManager = request.CreateSource{ID: "som-0123456789012347", SpaceID: "wks-0123456789012345", EngineType: constants.EngineTypeFlink, SourceType: constants.SourceTypeKafka, Name: "kafka", Comment: "create ok", Creator: constants.CreatorCustom, Url: typeToJsonString(constants.SourceKafkaParams{Host: "dataworkbench-kafka-for-test", Port: 9092})}
	//NameExistsManager = request.CreateSource{ID: "som-0123456789012348", SpaceID: "wks-0123456789012345", EngineType: constants.EngineTypeFlink, SourceType: constants.SourceTypeMysql, Name: "mysql", Comment: "create ok", Creator: constants.CreatorWorkBench, Url: typeToJsonString(constants.SourceMysqlParams{User: "root", Password: "123456", Host: "127.0.0.1", Port: 3306, Database: "data_workbench"})}
	//NameErrorManager = request.CreateSource{ID: "som-0123456789012349", SpaceID: "wks-0123456789012345", EngineType: constants.EngineTypeFlink, SourceType: constants.SourceTypeMysql, Name: "hello.world", Comment: "create failed", Creator: constants.CreatorWorkBench, Url: typeToJsonString(constants.SourceMysqlParams{User: "root", Password: "123456", Host: "127.0.0.1", Port: 3306, Database: "data_workbench"})}
	//NewSpaceManager = request.CreateSource{ID: "som-0123456789012350", SpaceID: "wks-0123456789012346", EngineType: constants.EngineTypeFlink, SourceType: constants.SourceTypeMysql, Name: "mysql", Comment: "newspace", Creator: constants.CreatorWorkBench, Url: typeToJsonString(constants.SourceMysqlParams{User: "root", Password: "123456", Host: "127.0.0.1", Port: 3306, Database: "data_workbench"})}
	//JsonErrorManager = request.CreateSource{ID: "som-0123456789012351", SpaceID: "wks-0123456789012346", EngineType: constants.EngineTypeFlink, SourceType: constants.SourceTypeMysql, Name: "mysql", Comment: "create failed", Creator: constants.CreatorWorkBench, Url: "Error.json . error"}
	//EngineTypeErrorManager = request.CreateSource{ID: "som-0123456789012352", SpaceID: "wks-0123456789012346", EngineType: "NotFlink", SourceType: constants.SourceTypeMysql, Name: "mysql", Comment: "create failed", Creator: constants.CreatorWorkBench, Url: typeToJsonString(constants.SourceMysqlParams{User: "root", Password: "123456", Host: "127.0.0.1", Port: 3306, Database: "data_workbench"})}
	//SourceTypeErrorManager = request.CreateSource{ID: "som-0123456789012353", SpaceID: "wks-0123456789012346", EngineType: constants.EngineTypeFlink, SourceType: "ErrorSourceType", Name: "mysql", Comment: "create failed", Creator: constants.CreatorWorkBench, Url: typeToJsonString(constants.SourceMysqlParams{User: "root", Password: "123456", Host: "127.0.0.1", Port: 3306, Database: "data_workbench"})}
	//S3Manager = request.CreateSource{ID: "som-0123456789012354", SpaceID: "wks-0123456789012345", EngineType: constants.EngineTypeFlink, SourceType: constants.SourceTypeS3, Name: "s3", Comment: "qingcloud s3", Creator: constants.CreatorCustom, Url: typeToJsonString(constants.SourceS3Params{AccessKey: "RDTHDPNFWWDNWPIHESWK", SecretKey: "sVbVhAUsKGPPdiTOPAgveqCNhFjtvXFNpsPnQ7Hx", EndPoint: "http://s3.gd2.qingstor.com"})} // s3 not is url bucket
	//ClickHouseManager = request.CreateSource{ID: "som-0123456789012355", SpaceID: "wks-0123456789012345", EngineType: constants.EngineTypeFlink, SourceType: constants.SourceTypeClickHouse, Name: "clickhouse", Comment: "clickhouse", Creator: constants.CreatorCustom, Url: typeToJsonString(constants.SourceClickHouseParams{User: "default", Password: "", Host: "127.0.0.1", Port: 8123, Database: "default"})}
	//HbaseManager = request.CreateSource{ID: "som-0123456789012356", SpaceID: "wks-0123456789012345", EngineType: constants.EngineTypeFlink, SourceType: constants.SourceTypeHbase, Name: "hbase", Comment: "hbase", Creator: constants.CreatorCustom, Url: typeToJsonString(constants.SourceHbaseParams{Zookeeper: "hbase:2181", Znode: "/hbase"})}
	//FtpManager = request.CreateSource{ID: "som-0123456789012357", SpaceID: "wks-0123456789012345", EngineType: constants.EngineTypeFlink, SourceType: constants.SourceTypeFtp, Name: "ftp", Comment: "ftp", Creator: constants.CreatorCustom, Url: typeToJsonString(constants.SourceFtpParams{Host: "42.193.101.183", Port: 21})}

	//// Source Tables
	//TablePG = request.CreateTable{ID: "sot-0123456789012345", SourceID: PGManager.ID, Name: "pd", Comment: "postgresql", Url: typeToJsonString(constants.FlinkTableDefinePostgreSQL{SqlColumn: []constants.SqlColumnType{constants.SqlColumnType{Name: "id", Type: "bigint", PrimaryKey: "t"}, constants.SqlColumnType{Name: "id1", Type: "bigint", Comment: "xxx", PrimaryKey: "f"}}})}
	//TableKafka = request.CreateTable{ID: "sot-0123456789012346", SourceID: KafkaManager.ID, Name: "billing", Comment: "Kafka", Url: typeToJsonString(constants.FlinkTableDefineKafka{SqlColumn: []constants.SqlColumnType{constants.SqlColumnType{Name: "paycount", Type: "bigint", PrimaryKey: "f"}, constants.SqlColumnType{Name: "paymoney", Type: "string", Comment: "xxx", PrimaryKey: "f"}, constants.SqlColumnType{Name: "tproctime", Type: "AS PROCTIME()"}}, Topic: "workbench", Format: "json", ConnectorOptions: []string{"'json.fail-on-missing-field' = 'false'", "'json.ignore-parse-errors' = 'true'"}})} //{"paycount": 2, "paymoney": "EUR"} {"paycount": 1, "paymoney": "USD"}
	TableMysqlSource = request.CreateTable{TableID: "sot-0123456789012347", SourceID: MysqlManager.SourceID, SpaceID: "wks-0123456789012345", Name: "ms", Comment: "mysql", Direction: "source", Url: &model.TableUrl{MySQL: &model.MySQLTableUrl{SqlColumn: []*model.SqlColumnType{&model.SqlColumnType{Column: "id", Type: "bigint", PrimaryKey: "t"}, &model.SqlColumnType{Column: "id1", Type: "bigint", Comment: "xxx", PrimaryKey: "f"}}}}}
	TableMysqlDest = request.CreateTable{TableID: "sot-0123456789012348", SourceID: MysqlManager.SourceID, SpaceID: "wks-0123456789012345", Name: "md", Comment: "mysql dest", Direction: "destination", Url: &model.TableUrl{MySQL: &model.MySQLTableUrl{SqlColumn: []*model.SqlColumnType{&model.SqlColumnType{Column: "id", Type: "bigint", PrimaryKey: "t"}, &model.SqlColumnType{Column: "id1", Type: "bigint", Comment: "xxx", PrimaryKey: "f"}}}}}
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
	//TableClickHouseDest = request.CreateTable{ID: "sot-0123456789012361", SourceID: ClickHouseManager.ID, Name: "ck", Comment: "clickhouse destination", Url: typeToJsonString(constants.FlinkTableDefineClickHouse{SqlColumn: []constants.SqlColumnType{constants.SqlColumnType{Name: "id", Type: "bigint", PrimaryKey: "t"}, constants.SqlColumnType{Name: "id1", Type: "bigint", Comment: "xxx", PrimaryKey: "f"}}})}
	//TableUDFSource = request.CreateTable{ID: "sot-0123456789012362", SourceID: MysqlManager.ID, Name: "udfs", Comment: "udfs", Url: typeToJsonString(constants.FlinkTableDefineMysql{SqlColumn: []constants.SqlColumnType{constants.SqlColumnType{Name: "a", Type: "varchar", PrimaryKey: "f", Length: "10"}}})}
	//TableUDFDest = request.CreateTable{ID: "sot-0123456789012363", SourceID: MysqlManager.ID, Name: "udfd", Comment: "udfd", Url: typeToJsonString(constants.FlinkTableDefineMysql{SqlColumn: []constants.SqlColumnType{constants.SqlColumnType{Name: "a", Type: "varchar", PrimaryKey: "f", Length: "10"}}})}
	//TableHbaseSource = request.CreateTable{ID: "sot-0123456789012364", SourceID: HbaseManager.ID, Name: "testsource", Comment: "hbase source", Url: typeToJsonString(constants.FlinkTableDefineHbase{SqlColumn: []constants.SqlColumnType{constants.SqlColumnType{Name: "rowkey", Type: "STRING", PrimaryKey: "f", Length: ""}, constants.SqlColumnType{Name: "columna", Type: "ROW<a STRING>"}}})}
	//TableHbaseDest = request.CreateTable{ID: "sot-0123456789012365", SourceID: HbaseManager.ID, Name: "testdest", Comment: "hbase dest", Url: typeToJsonString(constants.FlinkTableDefineHbase{SqlColumn: []constants.SqlColumnType{constants.SqlColumnType{Name: "rowkey", Type: "STRING", PrimaryKey: "f", Length: ""}, constants.SqlColumnType{Name: "columna", Type: "ROW<a STRING>"}}})}
	//TableFtpSource = request.CreateTable{ID: "sot-0123456789012366", SourceID: FtpManager.ID, Name: "ftpsource", Comment: "ftp source", Url: typeToJsonString(constants.FlinkTableDefineFtp{Path: "/u/", Format: "csv", SqlColumn: []constants.SqlColumnType{constants.SqlColumnType{Name: "readName", Type: "string", Comment: "xxx"}, constants.SqlColumnType{Name: "cellPhone", Type: "string", Comment: "xxx"}, {Name: "universityName", Type: "string", Comment: "xxx"}, {Name: "city", Type: "string", Comment: "xxx"}, {Name: "street", Type: "string", Comment: "xxx"}, {Name: "ip", Type: "string", Comment: "xxx"}, {Name: "pt", Type: "AS PROCTIME()"}}, ConnectorOptions: []string{"'username' = 'ftptest'", "'password' = '123456'"}})}
	//TableFtpDest = request.CreateTable{ID: "sot-0123456789012367", SourceID: FtpManager.ID, Name: "ftpdest", Comment: "ftp dest", Url: typeToJsonString(constants.FlinkTableDefineFtp{Path: "/sink.csv", Format: "csv", SqlColumn: []constants.SqlColumnType{constants.SqlColumnType{Name: "readName", Type: "string", Comment: "xxx"}, constants.SqlColumnType{Name: "cellPhone", Type: "string", Comment: "xxx"}, {Name: "universityName", Type: "string", Comment: "xxx"}, {Name: "city", Type: "string", Comment: "xxx"}, {Name: "street", Type: "string", Comment: "xxx"}, {Name: "ip", Type: "string", Comment: "xxx"}}, ConnectorOptions: []string{"'username' = 'ftptest'", "'password' = '123456'"}})}

	address := "127.0.0.1:9104"
	//address := "127.0.0.1:50001"
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
	//_, err = client.Create(ctx, &PGManager)
	//require.Nil(t, err, "%+v", err)
	//_, err = client.Create(ctx, &KafkaManager)
	//require.Nil(t, err, "%+v", err)
	//_, err = client.Create(ctx, &NewSpaceManager)
	//require.Nil(t, err, "%+v", err)
	//_, err = client.Create(ctx, &NameErrorManager)
	//require.Equal(t, qerror.InvalidSourceName.Code(), errorCode(err))
	//_, err = client.Create(ctx, &NameExistsManager)
	//require.Equal(t, qerror.ResourceAlreadyExists.Code(), errorCode(err))
	//_, err = client.Create(ctx, &JsonErrorManager)
	//require.Equal(t, qerror.InvalidJSON.Code(), errorCode(err))
	//_, err = client.Create(ctx, &SourceTypeErrorManager)
	//require.Equal(t, qerror.NotSupportSourceType.Code(), errorCode(err))
	//_, err = client.Create(ctx, &EngineTypeErrorManager)
	//require.Equal(t, qerror.NotSupportEngineType.Code(), errorCode(err))
	//_, err = client.Create(ctx, &S3Manager)
	//require.Nil(t, err, "%+v", err)
	//_, err = client.Create(ctx, &ClickHouseManager)
	//require.Nil(t, err, "%+v", err)
	//_, err = client.Create(ctx, &HbaseManager)
	//require.Nil(t, err, "%+v", err)
	//_, err = client.Create(ctx, &FtpManager)
	//require.Nil(t, err, "%+v", err)
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
		//d.ID = PGManager.ID
		//rep, err = client.Describe(ctx, &d)
		//require.Nil(t, err, "%+v", err)
		//require.Equal(t, rep.ID, d.ID)
		//d.ID = KafkaManager.ID
		//rep, err = client.Describe(ctx, &d)
		//require.Nil(t, err, "%+v", err)
		//require.Equal(t, rep.ID, d.ID)
		//d.ID = NewSpaceManager.ID
		//rep, err = client.Describe(ctx, &d)
		//require.Nil(t, err, "%+v", err)
		//require.Equal(t, rep.ID, d.ID)
		//d.ID = S3Manager.ID
		//rep, err = client.Describe(ctx, &d)
		//require.Nil(t, err, "%+v", err)
		//require.Equal(t, rep.ID, d.ID)
		//d.ID = ClickHouseManager.ID
		//rep, err = client.Describe(ctx, &d)
		//require.Nil(t, err, "%+v", err)
		//require.Equal(t, rep.ID, d.ID)
		//d.ID = HbaseManager.ID
		//rep, err = client.Describe(ctx, &d)
		//require.Nil(t, err, "%+v", err)
		//require.Equal(t, rep.ID, d.ID)
		//d.ID = FtpManager.ID
		//rep, err = client.Describe(ctx, &d)
		//require.Nil(t, err, "%+v", err)
		//require.Equal(t, rep.ID, d.ID)
		//return nil
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

	//i.Name = PGManager.Name
	//i.ID = MysqlManager.ID
	//i.SourceType = MysqlManager.SourceType
	//i.Url = MysqlManager.Url
	//_, err = client.Update(ctx, &i)
	//require.NotNil(t, err, "%+v", err)
	//require.Equal(t, qerror.ResourceAlreadyExists.Code(), errorCode(err))
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
	//p.EngineType = PGManager.EngineType
	//_, err = client.PingSource(ctx, &p)
	//require.Equal(t, qerror.ConnectSourceFailed.Code(), errorCode(err))

	//p.SourceType = KafkaManager.SourceType
	//p.Url = KafkaManager.Url
	//p.EngineType = KafkaManager.EngineType
	//_, err = client.PingSource(ctx, &p)
	//require.Nil(t, err, "%+v", err)

	//p.SourceType = S3Manager.SourceType
	//p.Url = S3Manager.Url
	//p.EngineType = S3Manager.EngineType
	//_, err = client.PingSource(ctx, &p)
	//require.Nil(t, err, "%+v", err)
	////require.Equal(t, qerror.ConnectSourceFailed.Code(), errorCode(err))

	//p.SourceType = ClickHouseManager.SourceType
	//p.Url = ClickHouseManager.Url
	//p.EngineType = ClickHouseManager.EngineType
	//_, err = client.PingSource(ctx, &p)
	//require.Equal(t, qerror.ConnectSourceFailed.Code(), errorCode(err))
	////require.Nil(t, err, "%+v", err)

	//p.SourceType = HbaseManager.SourceType
	//p.Url = HbaseManager.Url
	//p.EngineType = HbaseManager.EngineType
	//_, err = client.PingSource(ctx, &p)
	//require.Nil(t, err, "%+v", err)

	//p.SourceType = FtpManager.SourceType
	//p.Url = FtpManager.Url
	//p.EngineType = FtpManager.EngineType
	//_, err = client.PingSource(ctx, &p)
	//require.Nil(t, err, "%+v", err)
}

func Test_DisableSource(t *testing.T) {
	mainInit(t)
	var v request.DisableSource
	var err error

	v.SourceIDs = []string{MysqlManager.SourceID}

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

	//var i1 smpb.SotDescribeRequest

	//i1.ID = TableMysqlSource.ID
	//_, err = client.SotDescribe(ctx, &i1)
	//require.NotNil(t, err, "%+v", err)
	//require.Equal(t, qerror.SourceIsDisable.Code(), errorCode(err))
}

func Test_EnableSource(t *testing.T) {
	mainInit(t)
	var v request.EnableSource
	var err error

	v.SourceIDs = []string{MysqlManager.SourceID}

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
	//_, err = client.SotCreate(ctx, &TablePG)
	//require.Nil(t, err, "%+v", err)
	//_, err = client.SotCreate(ctx, &TableKafka)
	//require.Nil(t, err, "%+v", err)
	_, err = client.CreateTable(ctx, &TableMysqlSource)
	require.Nil(t, err, "%+v", err)
	_, err = client.CreateTable(ctx, &TableMysqlDest)
	require.Nil(t, err, "%+v", err)
	//_, err = client.SotCreate(ctx, &TableNameExists)
	//require.NotNil(t, err, "%+v", err)
	//require.Equal(t, qerror.ResourceAlreadyExists.Code(), errorCode(err))
	//_, err = client.SotCreate(ctx, &TableNameError)
	//require.NotNil(t, err, "%+v", err)
	//require.Equal(t, qerror.InvalidSourceName.Code(), errorCode(err))
	//_, err = client.SotCreate(ctx, &TableJsonError)
	//require.NotNil(t, err, "%+v", err)
	//require.Equal(t, qerror.InvalidJSON.Code(), errorCode(err))
	//_, err = client.SotCreate(ctx, &TableManagerError)
	//require.NotNil(t, err, "%+v", err)
	//_, err = client.SotCreate(ctx, &TableMysqlDimensionSource)
	//require.Nil(t, err, "%+v", err)
	//_, err = client.SotCreate(ctx, &TableMysqlDimensionDest)
	//require.Nil(t, err, "%+v", err)
	//_, err = client.SotCreate(ctx, &TableMysqlCommonSource)
	//require.Nil(t, err, "%+v", err)
	//_, err = client.SotCreate(ctx, &TableMysqlCommonDest)
	//require.Nil(t, err, "%+v", err)
	//_, err = client.SotCreate(ctx, &TableS3Source)
	//require.Nil(t, err, "%+v", err)
	//_, err = client.SotCreate(ctx, &TableS3Dest)
	//require.Nil(t, err, "%+v", err)
	//_, err = client.SotCreate(ctx, &TableClickHouseDest)
	//require.Nil(t, err, "%+v", err)
	//_, err = client.SotCreate(ctx, &TableUDFSource)
	//require.Nil(t, err, "%+v", err)
	//_, err = client.SotCreate(ctx, &TableUDFDest)
	//require.Nil(t, err, "%+v", err)
	//_, err = client.SotCreate(ctx, &TableHbaseSource)
	//require.Nil(t, err, "%+v", err)
	//_, err = client.SotCreate(ctx, &TableHbaseDest)
	//require.Nil(t, err, "%+v", err)
	//_, err = client.SotCreate(ctx, &TableFtpSource)
	//require.Nil(t, err, "%+v", err)
	//_, err = client.SotCreate(ctx, &TableFtpDest)
	//require.Nil(t, err, "%+v", err)
}

func tablesDescribe(t *testing.T, id string) *model.TableInfo {
	var i request.DescribeTable
	var err error
	var rep *response.DescribeTable

	if id == "" {
		//i.ID = TablePG.ID
		//rep, err = client.SotDescribe(ctx, &i)
		//require.Nil(t, err, "%+v", err)
		//i.ID = TableKafka.ID
		//rep, err = client.SotDescribe(ctx, &i)
		//require.Nil(t, err, "%+v", err)
		i.TableID = TableMysqlSource.TableID
		rep, err = client.DescribeTable(ctx, &i)
		require.Nil(t, err, "%+v", err)
		i.TableID = TableMysqlDest.TableID
		rep, err = client.DescribeTable(ctx, &i)
		require.Nil(t, err, "%+v", err)
		//i.ID = TableS3Source.ID
		//rep, err = client.SotDescribe(ctx, &i)
		//require.Nil(t, err, "%+v", err)
		//i.ID = TableS3Dest.ID
		//rep, err = client.SotDescribe(ctx, &i)
		//require.Nil(t, err, "%+v", err)
		//i.ID = TableClickHouseDest.ID
		//rep, err = client.SotDescribe(ctx, &i)
		//require.Nil(t, err, "%+v", err)
		//i.ID = TableUDFSource.ID
		//rep, err = client.SotDescribe(ctx, &i)
		//require.Nil(t, err, "%+v", err)
		//i.ID = TableUDFDest.ID
		//rep, err = client.SotDescribe(ctx, &i)
		//require.Nil(t, err, "%+v", err)
		//i.ID = TableHbaseSource.ID
		//rep, err = client.SotDescribe(ctx, &i)
		//require.Nil(t, err, "%+v", err)
		//i.ID = TableHbaseDest.ID
		//rep, err = client.SotDescribe(ctx, &i)
		//require.Nil(t, err, "%+v", err)
		//i.ID = TableFtpSource.ID
		//rep, err = client.SotDescribe(ctx, &i)
		//require.Nil(t, err, "%+v", err)
		//i.ID = TableFtpDest.ID
		//rep, err = client.SotDescribe(ctx, &i)
		//require.Nil(t, err, "%+v", err)
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
	i.TableID = TableMysqlSource.TableID
	i.Name = TableMysqlSource.Name
	i.Url = TableMysqlSource.Url
	i.Direction = TableMysqlSource.Direction

	_, err = client.UpdateTable(ctx, &i)
	require.Nil(t, err, "%+v", err)
	require.Equal(t, i.Comment, tablesDescribe(t, i.TableID).Comment)

	//i.ID = TableMysqlSource.ID
	//i.Name = "xx.xx"
	//i.Url = TableMysqlSource.Url
	//_, err = client.SotUpdate(ctx, &i)
	//require.NotNil(t, err, "%+v", err)
	//require.Equal(t, qerror.InvalidSourceName.Code(), errorCode(err))

	//i.ID = TableKafka.ID
	//i.Name = TableKafka.Name
	//i.Url = "err, json, url"
	//_, err = client.SotUpdate(ctx, &i)
	//require.NotNil(t, err, "%+v", err)
	//require.Equal(t, qerror.InvalidJSON.Code(), errorCode(err))

	//i.ID = TableMysqlSource.ID
	//i.Name = TableMysqlDest.Name
	//i.Url = TableMysqlSource.Url
	//_, err = client.SotUpdate(ctx, &i)
	//require.NotNil(t, err, "%+v", err)
	//require.Equal(t, qerror.ResourceAlreadyExists.Code(), errorCode(err))
}

func tablesDelete(t *testing.T, id string) {
	var i request.DeleteTable
	var err error

	if id == "" {
		//i.ID = TablePG.ID
		//_, err = client.SotDelete(ctx, &i)
		//require.Nil(t, err, "%+v", err)
		//i.ID = TableKafka.ID
		//_, err = client.SotDelete(ctx, &i)
		//require.Nil(t, err, "%+v", err)
		i.TableIDs = []string{TableMysqlSource.TableID}
		_, err = client.DeleteTable(ctx, &i)
		require.Nil(t, err, "%+v", err)
		i.TableIDs = []string{TableMysqlDest.TableID}
		_, err = client.DeleteTable(ctx, &i)
		//require.Nil(t, err, "%+v", err)
		//i.ID = TableMysqlDimensionSource.ID
		//_, err = client.SotDelete(ctx, &i)
		//require.Nil(t, err, "%+v", err)
		//i.ID = TableMysqlDimensionDest.ID
		//_, err = client.SotDelete(ctx, &i)
		//require.Nil(t, err, "%+v", err)
		//i.ID = TableMysqlCommonSource.ID
		//_, err = client.SotDelete(ctx, &i)
		//require.Nil(t, err, "%+v", err)
		//i.ID = TableMysqlCommonDest.ID
		//_, err = client.SotDelete(ctx, &i)
		//require.Nil(t, err, "%+v", err)
		//i.ID = TableS3Source.ID
		//_, err = client.SotDelete(ctx, &i)
		//require.Nil(t, err, "%+v", err)
		//i.ID = TableS3Dest.ID
		//_, err = client.SotDelete(ctx, &i)
		//require.Nil(t, err, "%+v", err)
		//i.ID = TableClickHouseDest.ID
		//_, err = client.SotDelete(ctx, &i)
		//require.Nil(t, err, "%+v", err)
		//i.ID = TableUDFSource.ID
		//_, err = client.SotDelete(ctx, &i)
		//require.Nil(t, err, "%+v", err)
		//i.ID = TableUDFDest.ID
		//_, err = client.SotDelete(ctx, &i)
		//require.Nil(t, err, "%+v", err)
		//i.ID = TableHbaseSource.ID
		//_, err = client.SotDelete(ctx, &i)
		//require.Nil(t, err, "%+v", err)
		//i.ID = TableHbaseDest.ID
		//_, err = client.SotDelete(ctx, &i)
		//require.Nil(t, err, "%+v", err)
		//i.ID = TableFtpSource.ID
		//_, err = client.SotDelete(ctx, &i)
		//require.Nil(t, err, "%+v", err)
		//i.ID = TableFtpDest.ID
		//_, err = client.SotDelete(ctx, &i)
		//require.Nil(t, err, "%+v", err)
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
		i.SpaceID = MysqlManager.SpaceID
		i.Limit = 100
		i.Offset = 0
		rep, err = client.List(ctx, &i)
		require.Nil(t, err, "%+v", err)

		i.SpaceID = MysqlManager.SpaceID
		i.Limit = 100
		i.Offset = 0
		i.Search = "my"
		rep, err = client.List(ctx, &i)
		require.Nil(t, err, "%+v", err)
		//require.Equal(t, 7, len(rep.Infos))
		//require.Equal(t, int32(7), rep.Total)

		//i.SpaceID = NewSpaceManager.SpaceID
		//i.Limit = 100
		//i.Offset = 0
		//rep, err = client.List(ctx, &i)
		//require.Nil(t, err, "%+v", err)
		//require.Equal(t, 1, len(rep.Infos))
		//require.Equal(t, int32(1), rep.Total)
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
		//i.SourceID = TablePG.SourceID
		//i.Limit = 100
		//i.Offset = 0
		//rep, err = client.SotList(ctx, &i)
		//require.Nil(t, err, "%+v", err)
		//require.Equal(t, 1, len(rep.Infos))
		//require.Equal(t, int32(1), rep.Total)

		i.SourceID = TableMysqlSource.SourceID
		i.Limit = 100
		i.Offset = 0
		rep, err = client.ListTable(ctx, &i)
		require.Nil(t, err, "%+v", err)

		i.SourceID = TableMysqlSource.SourceID
		i.Limit = 100
		i.Offset = 0
		i.Search = "m"
		rep, err = client.ListTable(ctx, &i)
		require.Nil(t, err, "%+v", err)
		//require.Equal(t, 8, len(rep.Infos))
		//require.Equal(t, int32(8), rep.Total)

		//i.SourceID = TableKafka.SourceID
		//i.Limit = 100
		//i.Offset = 0
		//rep, err = client.SotList(ctx, &i)
		//require.Nil(t, err, "%+v", err)
		//require.Equal(t, 1, len(rep.Infos))
		//require.Equal(t, int32(1), rep.Total)

		//i.SourceID = TableS3Source.SourceID
		//i.Limit = 100
		//i.Offset = 0
		//rep, err = client.SotList(ctx, &i)
		//require.Nil(t, err, "%+v", err)
		//require.Equal(t, 2, len(rep.Infos))
		//require.Equal(t, int32(2), rep.Total)

		//i.SourceID = TableClickHouseDest.SourceID
		//i.Limit = 100
		//i.Offset = 0
		//rep, err = client.SotList(ctx, &i)
		//require.Nil(t, err, "%+v", err)
		//require.Equal(t, 1, len(rep.Infos))
		//require.Equal(t, int32(1), rep.Total)

		//i.SourceID = TableHbaseSource.SourceID
		//i.Limit = 100
		//i.Offset = 0
		//rep, err = client.SotList(ctx, &i)
		//require.Nil(t, err, "%+v", err)
		//require.Equal(t, 2, len(rep.Infos))
		//require.Equal(t, int32(2), rep.Total)

		//i.SourceID = TableFtpSource.SourceID
		//i.Limit = 100
		//i.Offset = 0
		//rep, err = client.SotList(ctx, &i)
		//require.Nil(t, err, "%+v", err)
		//require.Equal(t, 2, len(rep.Infos))
		//require.Equal(t, int32(2), rep.Total)
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
			//i.ID = PGManager.ID
			//_, err = client.Delete(ctx, &i)
			//require.Nil(t, err, "%+v", err)
			//i.ID = KafkaManager.ID
			//_, err = client.Delete(ctx, &i)
			//require.Nil(t, err, "%+v", err)
			//i.ID = S3Manager.ID
			//_, err = client.Delete(ctx, &i)
			//require.Nil(t, err, "%+v", err)
			//i.ID = ClickHouseManager.ID
			//_, err = client.Delete(ctx, &i)
			//require.Nil(t, err, "%+v", err)
			//i.ID = HbaseManager.ID
			//_, err = client.Delete(ctx, &i)
			//require.Nil(t, err, "%+v", err)
			//i.ID = NewSpaceManager.ID
			//_, err = client.Delete(ctx, &i)
			//require.Nil(t, err, "%+v", err)
			//i.ID = FtpManager.ID
			//_, err = client.Delete(ctx, &i)
			//require.Nil(t, err, "%+v", err)
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

	d.SpaceIds = []string{MysqlManager.SpaceID}
	_, err := client.DeleteAll(ctx, &d)
	require.Nil(t, err, "%+v", err)
}

func Test_SourceTables(t *testing.T) {
	var v request.SourceTables

	mainInit(t)

	v.SourceID = MysqlManager.SourceID

	_, err := client.SourceTables(ctx, &v)
	require.Nil(t, err, "%+v", err)
}

func Test_TableColumns(t *testing.T) {
	var v request.TableColumns

	mainInit(t)

	v.SourceID = MysqlManager.SourceID
	v.TableName = "sourcemanager"

	_, err := client.TableColumns(ctx, &v)
	require.Nil(t, err, "%+v", err)
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
	var err error

	_, err = client.Create(ctx, &MysqlManager)
	require.Nil(t, err, "%+v", err)
	_, err = client.CreateTable(ctx, &TableMysqlSource)
	require.Nil(t, err, "%+v", err)
	_, err = client.CreateTable(ctx, &TableMysqlDest)
	require.Nil(t, err, "%+v", err)
}
