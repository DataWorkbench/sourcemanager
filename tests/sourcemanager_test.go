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

	"github.com/DataWorkbench/gproto/pkg/smpb"
)

// manager
var MysqlManager smpb.CreateRequest //name mysql
var PGManager smpb.CreateRequest
var KafkaManager smpb.CreateRequest
var NewSpaceManager smpb.CreateRequest        // name mysql
var NameExistsManager smpb.CreateRequest      //create failed
var NameErrorManager smpb.CreateRequest       //create failed
var JsonErrorManager smpb.CreateRequest       //create failed
var EngineTypeErrorManager smpb.CreateRequest //create failed
var SourceTypeErrorManager smpb.CreateRequest //create failed
var S3Manager smpb.CreateRequest
var ClickHouseManager smpb.CreateRequest
var HbaseManager smpb.CreateRequest

var TablePG smpb.SotCreateRequest
var TableKafka smpb.SotCreateRequest
var TableMysqlSource smpb.SotCreateRequest
var TableMysqlDest smpb.SotCreateRequest
var TableNameExists smpb.SotCreateRequest
var TableNameError smpb.SotCreateRequest
var TableJsonError smpb.SotCreateRequest
var TableManagerError smpb.SotCreateRequest
var TableMysqlDimensionSource smpb.SotCreateRequest
var TableMysqlDimensionDest smpb.SotCreateRequest
var TableMysqlCommonSource smpb.SotCreateRequest
var TableMysqlCommonDest smpb.SotCreateRequest
var TableS3Source smpb.SotCreateRequest
var TableS3Dest smpb.SotCreateRequest
var TableClickHouseDest smpb.SotCreateRequest // click support sink only.
var TableUDFSource smpb.SotCreateRequest
var TableUDFDest smpb.SotCreateRequest
var TableHbaseSource smpb.SotCreateRequest
var TableHbaseDest smpb.SotCreateRequest

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
	MysqlManager = smpb.CreateRequest{ID: "som-0123456789012345", SpaceID: "wks-0123456789012345", EngineType: constants.EngineTypeFlink, SourceType: constants.SourceTypeMysql, Name: "mysql", Comment: "create ok", Creator: constants.CreatorWorkBench, Url: typeToJsonString(constants.SourceMysqlParams{User: "root", Password: "password", Host: "dataworkbench-db", Port: 3306, Database: "data_workbench"})}
	PGManager = smpb.CreateRequest{ID: "som-0123456789012346", SpaceID: "wks-0123456789012345", EngineType: constants.EngineTypeFlink, SourceType: constants.SourceTypePostgreSQL, Name: "pg", Comment: "create ok", Creator: constants.CreatorCustom, Url: typeToJsonString(constants.SourcePostgreSQLParams{User: "lzzhang", Password: "123456", Host: "127.0.0.1", Port: 5432, Database: "lzzhang"})}
	KafkaManager = smpb.CreateRequest{ID: "som-0123456789012347", SpaceID: "wks-0123456789012345", EngineType: constants.EngineTypeFlink, SourceType: constants.SourceTypeKafka, Name: "kafka", Comment: "create ok", Creator: constants.CreatorCustom, Url: typeToJsonString(constants.SourceKafkaParams{Host: "dataworkbench-kafka-for-test", Port: 9092})}
	NameExistsManager = smpb.CreateRequest{ID: "som-0123456789012348", SpaceID: "wks-0123456789012345", EngineType: constants.EngineTypeFlink, SourceType: constants.SourceTypeMysql, Name: "mysql", Comment: "create ok", Creator: constants.CreatorWorkBench, Url: typeToJsonString(constants.SourceMysqlParams{User: "root", Password: "123456", Host: "127.0.0.1", Port: 3306, Database: "data_workbench"})}
	NameErrorManager = smpb.CreateRequest{ID: "som-0123456789012349", SpaceID: "wks-0123456789012345", EngineType: constants.EngineTypeFlink, SourceType: constants.SourceTypeMysql, Name: "hello.world", Comment: "create failed", Creator: constants.CreatorWorkBench, Url: typeToJsonString(constants.SourceMysqlParams{User: "root", Password: "123456", Host: "127.0.0.1", Port: 3306, Database: "data_workbench"})}
	NewSpaceManager = smpb.CreateRequest{ID: "som-0123456789012350", SpaceID: "wks-0123456789012346", EngineType: constants.EngineTypeFlink, SourceType: constants.SourceTypeMysql, Name: "mysql", Comment: "newspace", Creator: constants.CreatorWorkBench, Url: typeToJsonString(constants.SourceMysqlParams{User: "root", Password: "123456", Host: "127.0.0.1", Port: 3306, Database: "data_workbench"})}
	JsonErrorManager = smpb.CreateRequest{ID: "som-0123456789012351", SpaceID: "wks-0123456789012346", EngineType: constants.EngineTypeFlink, SourceType: constants.SourceTypeMysql, Name: "mysql", Comment: "create failed", Creator: constants.CreatorWorkBench, Url: "Error.json . error"}
	EngineTypeErrorManager = smpb.CreateRequest{ID: "som-0123456789012352", SpaceID: "wks-0123456789012346", EngineType: "NotFlink", SourceType: constants.SourceTypeMysql, Name: "mysql", Comment: "create failed", Creator: constants.CreatorWorkBench, Url: typeToJsonString(constants.SourceMysqlParams{User: "root", Password: "123456", Host: "127.0.0.1", Port: 3306, Database: "data_workbench"})}
	SourceTypeErrorManager = smpb.CreateRequest{ID: "som-0123456789012353", SpaceID: "wks-0123456789012346", EngineType: constants.EngineTypeFlink, SourceType: "ErrorSourceType", Name: "mysql", Comment: "create failed", Creator: constants.CreatorWorkBench, Url: typeToJsonString(constants.SourceMysqlParams{User: "root", Password: "123456", Host: "127.0.0.1", Port: 3306, Database: "data_workbench"})}
	S3Manager = smpb.CreateRequest{ID: "som-0123456789012354", SpaceID: "wks-0123456789012345", EngineType: constants.EngineTypeFlink, SourceType: constants.SourceTypeS3, Name: "s3", Comment: "qingcloud s3", Creator: constants.CreatorCustom, Url: typeToJsonString(constants.SourceS3Params{AccessKey: "RDTHDPNFWWDNWPIHESWK", SecretKey: "sVbVhAUsKGPPdiTOPAgveqCNhFjtvXFNpsPnQ7Hx", EndPoint: "http://s3.gd2.qingstor.com"})} // s3 not is url bucket
	ClickHouseManager = smpb.CreateRequest{ID: "som-0123456789012355", SpaceID: "wks-0123456789012345", EngineType: constants.EngineTypeFlink, SourceType: constants.SourceTypeClickHouse, Name: "clickhouse", Comment: "clickhouse", Creator: constants.CreatorCustom, Url: typeToJsonString(constants.SourceClickHouseParams{User: "default", Password: "", Host: "127.0.0.1", Port: 8123, Database: "default"})}
	HbaseManager = smpb.CreateRequest{ID: "som-0123456789012356", SpaceID: "wks-0123456789012345", EngineType: constants.EngineTypeFlink, SourceType: constants.SourceTypeHbase, Name: "hbase", Comment: "hbase", Creator: constants.CreatorCustom, Url: typeToJsonString(constants.SourceHbaseParams{Zookeeper: "hbase:2181", Znode: "/hbase"})}

	// Source Tables
	TablePG = smpb.SotCreateRequest{ID: "sot-0123456789012345", SourceID: PGManager.ID, Name: "pd", Comment: "postgresql", Url: typeToJsonString(constants.FlinkTableDefinePostgreSQL{SqlColumn: []constants.SqlColumnType{constants.SqlColumnType{Name: "id", Type: "bigint", PrimaryKey: "t"}, constants.SqlColumnType{Name: "id1", Type: "bigint", Comment: "xxx", PrimaryKey: "f"}}})}
	TableKafka = smpb.SotCreateRequest{ID: "sot-0123456789012346", SourceID: KafkaManager.ID, Name: "billing", Comment: "Kafka", Url: typeToJsonString(constants.FlinkTableDefineKafka{SqlColumn: []constants.SqlColumnType{constants.SqlColumnType{Name: "paycount", Type: "bigint", PrimaryKey: "f"}, constants.SqlColumnType{Name: "paymoney", Type: "string", Comment: "xxx", PrimaryKey: "f"}, constants.SqlColumnType{Name: "tproctime", Type: "AS PROCTIME()"}}, Topic: "workbench", Format: "json", ConnectorOptions: []string{"'json.fail-on-missing-field' = 'false'", "'json.ignore-parse-errors' = 'true'"}})} //{"paycount": 2, "paymoney": "EUR"} {"paycount": 1, "paymoney": "USD"}
	TableMysqlSource = smpb.SotCreateRequest{ID: "sot-0123456789012347", SourceID: MysqlManager.ID, Name: "ms", Comment: "mysql", Url: typeToJsonString(constants.FlinkTableDefineMysql{SqlColumn: []constants.SqlColumnType{constants.SqlColumnType{Name: "id", Type: "bigint", PrimaryKey: "t"}, constants.SqlColumnType{Name: "id1", Type: "bigint", Comment: "xxx", PrimaryKey: "f"}}})}
	TableMysqlDest = smpb.SotCreateRequest{ID: "sot-0123456789012348", SourceID: MysqlManager.ID, Name: "pd", Comment: "mysqldest", Url: typeToJsonString(constants.FlinkTableDefineMysql{SqlColumn: []constants.SqlColumnType{constants.SqlColumnType{Name: "id", Type: "bigint", PrimaryKey: "t"}, constants.SqlColumnType{Name: "id1", Type: "bigint", Comment: "xxx", PrimaryKey: "f"}}})}
	TableNameExists = smpb.SotCreateRequest{ID: "sot-0123456789012351", SourceID: MysqlManager.ID, Name: "ms", Comment: "to pd", Url: typeToJsonString(constants.FlinkTableDefineMysql{SqlColumn: []constants.SqlColumnType{constants.SqlColumnType{Name: "id", Type: "bigint", PrimaryKey: "t"}, constants.SqlColumnType{Name: "id1", Type: "bigint", Comment: "xxx", PrimaryKey: "f"}}})}
	TableNameError = smpb.SotCreateRequest{ID: "sot-0123456789012352", SourceID: MysqlManager.ID, Name: "ms.ms", Comment: "to pd", Url: typeToJsonString(constants.FlinkTableDefineMysql{SqlColumn: []constants.SqlColumnType{constants.SqlColumnType{Name: "id", Type: "bigint", PrimaryKey: "t"}, constants.SqlColumnType{Name: "id1", Type: "bigint", Comment: "xxx", PrimaryKey: "f"}}})}
	TableJsonError = smpb.SotCreateRequest{ID: "sot-0123456789012353", SourceID: MysqlManager.ID, Name: "ms1", Comment: "to pd", Url: "sss,xx,xx, xx"}
	TableManagerError = smpb.SotCreateRequest{ID: "sot-0123456789012354", SourceID: "sot-xxxxyyyyzzzzxxxx", Name: "ms2", Comment: "to pd", Url: typeToJsonString(constants.FlinkTableDefineMysql{SqlColumn: []constants.SqlColumnType{constants.SqlColumnType{Name: "id", Type: "bigint", PrimaryKey: "t"}, constants.SqlColumnType{Name: "id1", Type: "bigint", Comment: "xxx", PrimaryKey: "f"}}})}
	TableMysqlDimensionSource = smpb.SotCreateRequest{ID: "sot-0123456789012355", SourceID: MysqlManager.ID, Name: "mw", Comment: "join dimension table", Url: typeToJsonString(constants.FlinkTableDefineMysql{SqlColumn: []constants.SqlColumnType{constants.SqlColumnType{Name: "rate", Type: "bigint", PrimaryKey: "f"}, constants.SqlColumnType{Name: "dbmoney", Type: "varchar", Length: "8", Comment: "xxx"}}})}
	TableMysqlDimensionDest = smpb.SotCreateRequest{ID: "sot-0123456789012356", SourceID: MysqlManager.ID, Name: "mwd", Comment: "join dimension table", Url: typeToJsonString(constants.FlinkTableDefineMysql{SqlColumn: []constants.SqlColumnType{constants.SqlColumnType{Name: "total", Type: "bigint", PrimaryKey: "f"}}})}
	TableMysqlCommonSource = smpb.SotCreateRequest{ID: "sot-0123456789012357", SourceID: MysqlManager.ID, Name: "mc", Comment: "join common table", Url: typeToJsonString(constants.FlinkTableDefineMysql{SqlColumn: []constants.SqlColumnType{constants.SqlColumnType{Name: "rate", Type: "bigint", PrimaryKey: "f"}, constants.SqlColumnType{Name: "dbmoney", Type: "varchar", Comment: "xxx", PrimaryKey: "f", Length: "8"}}})}
	TableMysqlCommonDest = smpb.SotCreateRequest{ID: "sot-0123456789012358", SourceID: MysqlManager.ID, Name: "mcd", Comment: "join common table", Url: typeToJsonString(constants.FlinkTableDefineMysql{SqlColumn: []constants.SqlColumnType{constants.SqlColumnType{Name: "total", Type: "bigint"}}})} //'connector.write.flush.max-rows' = '1'
	TableS3Source = smpb.SotCreateRequest{ID: "sot-0123456789012359", SourceID: S3Manager.ID, Name: "s3s", Comment: "s3 source", Url: typeToJsonString(constants.FlinkTableDefineS3{SqlColumn: []constants.SqlColumnType{constants.SqlColumnType{Name: "id", Type: "bigint", PrimaryKey: "t"}, constants.SqlColumnType{Name: "id1", Type: "bigint", Comment: "xxx", PrimaryKey: "f"}}, Path: "s3a://filesystem/source", Format: "json"})}
	TableS3Dest = smpb.SotCreateRequest{ID: "sot-0123456789012360", SourceID: S3Manager.ID, Name: "s3d", Comment: "s3 destination", Url: typeToJsonString(constants.FlinkTableDefineS3{SqlColumn: []constants.SqlColumnType{constants.SqlColumnType{Name: "id", Type: "bigint", PrimaryKey: "t"}, constants.SqlColumnType{Name: "id1", Type: "bigint", Comment: "xxx", PrimaryKey: "f"}}, Path: "s3a://filesystem/destination", Format: "json"})}
	TableClickHouseDest = smpb.SotCreateRequest{ID: "sot-0123456789012361", SourceID: ClickHouseManager.ID, Name: "ck", Comment: "clickhouse destination", Url: typeToJsonString(constants.FlinkTableDefineClickHouse{SqlColumn: []constants.SqlColumnType{constants.SqlColumnType{Name: "id", Type: "bigint", PrimaryKey: "t"}, constants.SqlColumnType{Name: "id1", Type: "bigint", Comment: "xxx", PrimaryKey: "f"}}})}
	TableUDFSource = smpb.SotCreateRequest{ID: "sot-0123456789012362", SourceID: MysqlManager.ID, Name: "udfs", Comment: "udfs", Url: typeToJsonString(constants.FlinkTableDefineMysql{SqlColumn: []constants.SqlColumnType{constants.SqlColumnType{Name: "a", Type: "varchar", PrimaryKey: "f", Length: "10"}}})}
	TableUDFDest = smpb.SotCreateRequest{ID: "sot-0123456789012363", SourceID: MysqlManager.ID, Name: "udfd", Comment: "udfd", Url: typeToJsonString(constants.FlinkTableDefineMysql{SqlColumn: []constants.SqlColumnType{constants.SqlColumnType{Name: "a", Type: "varchar", PrimaryKey: "f", Length: "10"}}})}
	TableHbaseSource = smpb.SotCreateRequest{ID: "sot-0123456789012364", SourceID: HbaseManager.ID, Name: "testsource", Comment: "hbase source", Url: typeToJsonString(constants.FlinkTableDefineHbase{SqlColumn: []constants.SqlColumnType{constants.SqlColumnType{Name: "rowkey", Type: "STRING", PrimaryKey: "f", Length: ""}, constants.SqlColumnType{Name: "columna", Type: "ROW<a STRING>"}}})}
	TableHbaseDest = smpb.SotCreateRequest{ID: "sot-0123456789012365", SourceID: HbaseManager.ID, Name: "testdest", Comment: "hbase dest", Url: typeToJsonString(constants.FlinkTableDefineHbase{SqlColumn: []constants.SqlColumnType{constants.SqlColumnType{Name: "rowkey", Type: "STRING", PrimaryKey: "f", Length: ""}, constants.SqlColumnType{Name: "columna", Type: "ROW<a STRING>"}}})}

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
func Test_Create(t *testing.T) {
	mainInit(t)
	Clean(t)
	var err error

	_, err = client.Create(ctx, &MysqlManager)
	require.Nil(t, err, "%+v", err)
	_, err = client.Create(ctx, &PGManager)
	require.Nil(t, err, "%+v", err)
	_, err = client.Create(ctx, &KafkaManager)
	require.Nil(t, err, "%+v", err)
	_, err = client.Create(ctx, &NewSpaceManager)
	require.Nil(t, err, "%+v", err)
	_, err = client.Create(ctx, &NameErrorManager)
	require.Equal(t, qerror.InvalidSourceName.Code(), errorCode(err))
	_, err = client.Create(ctx, &NameExistsManager)
	require.Equal(t, qerror.ResourceAlreadyExists.Code(), errorCode(err))
	_, err = client.Create(ctx, &JsonErrorManager)
	require.Equal(t, qerror.InvalidJSON.Code(), errorCode(err))
	_, err = client.Create(ctx, &SourceTypeErrorManager)
	require.Equal(t, qerror.NotSupportSourceType.Code(), errorCode(err))
	_, err = client.Create(ctx, &EngineTypeErrorManager)
	require.Equal(t, qerror.NotSupportEngineType.Code(), errorCode(err))
	_, err = client.Create(ctx, &S3Manager)
	require.Nil(t, err, "%+v", err)
	_, err = client.Create(ctx, &ClickHouseManager)
	require.Nil(t, err, "%+v", err)
	_, err = client.Create(ctx, &HbaseManager)
	require.Nil(t, err, "%+v", err)
}

func Test_PingSource(t *testing.T) {
	mainInit(t)

	var p smpb.PingSourceRequest
	var err error

	p.SourceType = MysqlManager.SourceType
	p.Url = MysqlManager.Url
	p.EngineType = MysqlManager.EngineType
	_, err = client.PingSource(ctx, &p)
	require.Nil(t, err, "%+v", err)

	p.SourceType = PGManager.SourceType
	p.Url = PGManager.Url
	p.EngineType = PGManager.EngineType
	_, err = client.PingSource(ctx, &p)
	require.Equal(t, qerror.ConnectSourceFailed.Code(), errorCode(err))

	p.SourceType = KafkaManager.SourceType
	p.Url = KafkaManager.Url
	p.EngineType = KafkaManager.EngineType
	_, err = client.PingSource(ctx, &p)
	require.Nil(t, err, "%+v", err)

	p.SourceType = S3Manager.SourceType
	p.Url = S3Manager.Url
	p.EngineType = S3Manager.EngineType
	_, err = client.PingSource(ctx, &p)
	require.Nil(t, err, "%+v", err)
	//require.Equal(t, qerror.ConnectSourceFailed.Code(), errorCode(err))

	p.SourceType = ClickHouseManager.SourceType
	p.Url = ClickHouseManager.Url
	p.EngineType = ClickHouseManager.EngineType
	_, err = client.PingSource(ctx, &p)
	require.Equal(t, qerror.ConnectSourceFailed.Code(), errorCode(err))
	//require.Nil(t, err, "%+v", err)

	p.SourceType = HbaseManager.SourceType
	p.Url = HbaseManager.Url
	p.EngineType = HbaseManager.EngineType
	_, err = client.PingSource(ctx, &p)
	require.Nil(t, err, "%+v", err)
}

func Test_EngineMap(t *testing.T) {
	var i smpb.EngingMapRequest

	mainInit(t)

	i.EngineType = "flink"

	_, err := client.EngineMap(ctx, &i)
	require.Nil(t, err, "%+v", err)
}

func managerDescribe(t *testing.T, id string) *smpb.InfoReply {
	var d smpb.DescribeRequest
	var err error
	var rep *smpb.InfoReply

	if id == "" {
		d.ID = MysqlManager.ID
		rep, err = client.Describe(ctx, &d)
		require.Nil(t, err, "%+v", err)
		require.Equal(t, rep.ID, d.ID)
		d.ID = PGManager.ID
		rep, err = client.Describe(ctx, &d)
		require.Nil(t, err, "%+v", err)
		require.Equal(t, rep.ID, d.ID)
		d.ID = KafkaManager.ID
		rep, err = client.Describe(ctx, &d)
		require.Nil(t, err, "%+v", err)
		require.Equal(t, rep.ID, d.ID)
		d.ID = NewSpaceManager.ID
		rep, err = client.Describe(ctx, &d)
		require.Nil(t, err, "%+v", err)
		require.Equal(t, rep.ID, d.ID)
		d.ID = S3Manager.ID
		rep, err = client.Describe(ctx, &d)
		require.Nil(t, err, "%+v", err)
		require.Equal(t, rep.ID, d.ID)
		d.ID = ClickHouseManager.ID
		rep, err = client.Describe(ctx, &d)
		require.Nil(t, err, "%+v", err)
		require.Equal(t, rep.ID, d.ID)
		d.ID = HbaseManager.ID
		rep, err = client.Describe(ctx, &d)
		require.Nil(t, err, "%+v", err)
		require.Equal(t, rep.ID, d.ID)
		return nil
	} else {
		d.ID = id
		rep, err = client.Describe(ctx, &d)
		require.Nil(t, err, "%+v", err)
		require.Equal(t, rep.ID, d.ID)
		return rep
	}

	return nil
}

func Test_Describe(t *testing.T) {
	mainInit(t)
	managerDescribe(t, "")
}

func Clean(t *testing.T) {
	var (
		err error
		d   smpb.DeleteAllRequest
	)

	d.SpaceID = MysqlManager.SpaceID
	_, err = client.DeleteAll(ctx, &d)
	require.Nil(t, err, "%+v", err)
	//for _, info := range managerLists(t, MysqlManager.SpaceID).Infos {
	//	for _, table := range tablesLists(t, info.ID).Infos {
	//		tablesDelete(t, table.ID)
	//	}
	//	managerDelete(t, info.ID, false)
	//}

	for _, info := range managerLists(t, NewSpaceManager.SpaceID).Infos {
		for _, table := range tablesLists(t, info.ID).Infos {
			tablesDelete(t, table.ID)
		}
		managerDelete(t, info.ID, false)
	}
}

func managerLists(t *testing.T, SpaceID string) *smpb.ListsReply {
	var i smpb.ListsRequest
	var rep *smpb.ListsReply
	var err error

	if SpaceID == "" {
		i.SpaceID = MysqlManager.SpaceID
		i.Limit = 100
		i.Offset = 0
		rep, err = client.List(ctx, &i)
		require.Nil(t, err, "%+v", err)
		require.Equal(t, 6, len(rep.Infos))
		require.Equal(t, int32(6), rep.Total)
		i.SpaceID = NewSpaceManager.SpaceID
		i.Limit = 100
		i.Offset = 0
		rep, err = client.List(ctx, &i)
		require.Nil(t, err, "%+v", err)
		require.Equal(t, 1, len(rep.Infos))
		require.Equal(t, int32(1), rep.Total)
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

func Test_Lists(t *testing.T) {
	mainInit(t)
	managerLists(t, "")
}

func managerDelete(t *testing.T, id string, iserror bool) {
	var i smpb.DeleteRequest
	var err error

	if id == "" {
		if iserror == false {
			i.ID = MysqlManager.ID
			_, err = client.Delete(ctx, &i)
			require.Nil(t, err, "%+v", err)
			i.ID = PGManager.ID
			_, err = client.Delete(ctx, &i)
			require.Nil(t, err, "%+v", err)
			i.ID = KafkaManager.ID
			_, err = client.Delete(ctx, &i)
			require.Nil(t, err, "%+v", err)
			i.ID = S3Manager.ID
			_, err = client.Delete(ctx, &i)
			require.Nil(t, err, "%+v", err)
			i.ID = ClickHouseManager.ID
			_, err = client.Delete(ctx, &i)
			require.Nil(t, err, "%+v", err)
			i.ID = HbaseManager.ID
			_, err = client.Delete(ctx, &i)
			require.Nil(t, err, "%+v", err)
			i.ID = NewSpaceManager.ID
			_, err = client.Delete(ctx, &i)
			require.Nil(t, err, "%+v", err)
		} else {
			i.ID = MysqlManager.ID
			_, err = client.Delete(ctx, &i)
			require.NotNil(t, err, "%+v", err)
			require.Equal(t, qerror.ResourceIsUsing.Code(), errorCode(err))
		}
	} else {
		i.ID = id
		_, err = client.Delete(ctx, &i)
		require.Nil(t, err, "%+v", err)
	}
}

func Test_Update(t *testing.T) {
	var i smpb.UpdateRequest
	var err error

	i.Name = MysqlManager.Name
	i.ID = MysqlManager.ID
	i.Comment = "updateok"
	i.SourceType = MysqlManager.SourceType
	i.Url = MysqlManager.Url
	_, err = client.Update(ctx, &i)
	require.Nil(t, err, "%+v", err)
	require.Equal(t, i.Comment, managerDescribe(t, i.ID).Comment)

	i.Name = PGManager.Name
	i.ID = MysqlManager.ID
	i.SourceType = MysqlManager.SourceType
	i.Url = MysqlManager.Url
	_, err = client.Update(ctx, &i)
	require.NotNil(t, err, "%+v", err)
	require.Equal(t, qerror.ResourceAlreadyExists.Code(), errorCode(err))
}

// Source Tables
func Test_SotCreate(t *testing.T) {
	var err error
	_, err = client.SotCreate(ctx, &TablePG)
	require.Nil(t, err, "%+v", err)
	_, err = client.SotCreate(ctx, &TableKafka)
	require.Nil(t, err, "%+v", err)
	_, err = client.SotCreate(ctx, &TableMysqlSource)
	require.Nil(t, err, "%+v", err)
	_, err = client.SotCreate(ctx, &TableMysqlDest)
	require.Nil(t, err, "%+v", err)
	_, err = client.SotCreate(ctx, &TableNameExists)
	require.NotNil(t, err, "%+v", err)
	require.Equal(t, qerror.ResourceAlreadyExists.Code(), errorCode(err))
	_, err = client.SotCreate(ctx, &TableNameError)
	require.NotNil(t, err, "%+v", err)
	require.Equal(t, qerror.InvalidSourceName.Code(), errorCode(err))
	_, err = client.SotCreate(ctx, &TableJsonError)
	require.NotNil(t, err, "%+v", err)
	require.Equal(t, qerror.InvalidJSON.Code(), errorCode(err))
	_, err = client.SotCreate(ctx, &TableManagerError)
	require.NotNil(t, err, "%+v", err)
	_, err = client.SotCreate(ctx, &TableMysqlDimensionSource)
	require.Nil(t, err, "%+v", err)
	_, err = client.SotCreate(ctx, &TableMysqlDimensionDest)
	require.Nil(t, err, "%+v", err)
	_, err = client.SotCreate(ctx, &TableMysqlCommonSource)
	require.Nil(t, err, "%+v", err)
	_, err = client.SotCreate(ctx, &TableMysqlCommonDest)
	require.Nil(t, err, "%+v", err)
	_, err = client.SotCreate(ctx, &TableS3Source)
	require.Nil(t, err, "%+v", err)
	_, err = client.SotCreate(ctx, &TableS3Dest)
	require.Nil(t, err, "%+v", err)
	_, err = client.SotCreate(ctx, &TableClickHouseDest)
	require.Nil(t, err, "%+v", err)
	_, err = client.SotCreate(ctx, &TableUDFSource)
	require.Nil(t, err, "%+v", err)
	_, err = client.SotCreate(ctx, &TableUDFDest)
	require.Nil(t, err, "%+v", err)
	_, err = client.SotCreate(ctx, &TableHbaseSource)
	require.Nil(t, err, "%+v", err)
	_, err = client.SotCreate(ctx, &TableHbaseDest)
	require.Nil(t, err, "%+v", err)
}

func tablesLists(t *testing.T, SourceID string) *smpb.SotListsReply {
	var i smpb.SotListsRequest
	var err error
	var rep *smpb.SotListsReply

	if SourceID == "" {
		i.SourceID = TablePG.SourceID
		i.Limit = 100
		i.Offset = 0
		rep, err = client.SotList(ctx, &i)
		require.Nil(t, err, "%+v", err)
		require.Equal(t, 1, len(rep.Infos))
		require.Equal(t, int32(1), rep.Total)

		i.SourceID = TableMysqlSource.SourceID
		i.Limit = 100
		i.Offset = 0
		rep, err = client.SotList(ctx, &i)
		require.Nil(t, err, "%+v", err)
		require.Equal(t, 8, len(rep.Infos))
		require.Equal(t, int32(8), rep.Total)

		i.SourceID = TableKafka.SourceID
		i.Limit = 100
		i.Offset = 0
		rep, err = client.SotList(ctx, &i)
		require.Nil(t, err, "%+v", err)
		require.Equal(t, 1, len(rep.Infos))
		require.Equal(t, int32(1), rep.Total)

		i.SourceID = TableS3Source.SourceID
		i.Limit = 100
		i.Offset = 0
		rep, err = client.SotList(ctx, &i)
		require.Nil(t, err, "%+v", err)
		require.Equal(t, 2, len(rep.Infos))
		require.Equal(t, int32(2), rep.Total)

		i.SourceID = TableClickHouseDest.SourceID
		i.Limit = 100
		i.Offset = 0
		rep, err = client.SotList(ctx, &i)
		require.Nil(t, err, "%+v", err)
		require.Equal(t, 1, len(rep.Infos))
		require.Equal(t, int32(1), rep.Total)

		i.SourceID = TableHbaseSource.SourceID
		i.Limit = 100
		i.Offset = 0
		rep, err = client.SotList(ctx, &i)
		require.Nil(t, err, "%+v", err)
		require.Equal(t, 2, len(rep.Infos))
		require.Equal(t, int32(2), rep.Total)
	} else {
		i.SourceID = SourceID
		i.Limit = 100
		i.Offset = 0
		rep, err = client.SotList(ctx, &i)
		require.Nil(t, err, "%+v", err)
		return rep
	}
	return nil
}

func Test_SotLists(t *testing.T) {
	tablesLists(t, "")
}

func tablesDelete(t *testing.T, id string) {
	var i smpb.SotDeleteRequest
	var err error

	if id == "" {
		i.ID = TablePG.ID
		_, err = client.SotDelete(ctx, &i)
		require.Nil(t, err, "%+v", err)
		i.ID = TableKafka.ID
		_, err = client.SotDelete(ctx, &i)
		require.Nil(t, err, "%+v", err)
		i.ID = TableMysqlSource.ID
		_, err = client.SotDelete(ctx, &i)
		require.Nil(t, err, "%+v", err)
		i.ID = TableMysqlDest.ID
		_, err = client.SotDelete(ctx, &i)
		require.Nil(t, err, "%+v", err)
		i.ID = TableMysqlDimensionSource.ID
		_, err = client.SotDelete(ctx, &i)
		require.Nil(t, err, "%+v", err)
		i.ID = TableMysqlDimensionDest.ID
		_, err = client.SotDelete(ctx, &i)
		require.Nil(t, err, "%+v", err)
		i.ID = TableMysqlCommonSource.ID
		_, err = client.SotDelete(ctx, &i)
		require.Nil(t, err, "%+v", err)
		i.ID = TableMysqlCommonDest.ID
		_, err = client.SotDelete(ctx, &i)
		require.Nil(t, err, "%+v", err)
		i.ID = TableS3Source.ID
		_, err = client.SotDelete(ctx, &i)
		require.Nil(t, err, "%+v", err)
		i.ID = TableS3Dest.ID
		_, err = client.SotDelete(ctx, &i)
		require.Nil(t, err, "%+v", err)
		i.ID = TableClickHouseDest.ID
		_, err = client.SotDelete(ctx, &i)
		require.Nil(t, err, "%+v", err)
		i.ID = TableUDFSource.ID
		_, err = client.SotDelete(ctx, &i)
		require.Nil(t, err, "%+v", err)
		i.ID = TableUDFDest.ID
		_, err = client.SotDelete(ctx, &i)
		require.Nil(t, err, "%+v", err)
		i.ID = TableHbaseSource.ID
		_, err = client.SotDelete(ctx, &i)
		require.Nil(t, err, "%+v", err)
		i.ID = TableHbaseDest.ID
		_, err = client.SotDelete(ctx, &i)
		require.Nil(t, err, "%+v", err)
	} else {
		i.ID = id
		_, err = client.SotDelete(ctx, &i)
		require.Nil(t, err, "%+v", err)
	}
}

func tablesDescribe(t *testing.T, id string) *smpb.SotInfoReply {
	var i smpb.SotDescribeRequest
	var err error
	var rep *smpb.SotInfoReply

	if id == "" {
		i.ID = TablePG.ID
		rep, err = client.SotDescribe(ctx, &i)
		require.Nil(t, err, "%+v", err)
		i.ID = TableKafka.ID
		rep, err = client.SotDescribe(ctx, &i)
		require.Nil(t, err, "%+v", err)
		i.ID = TableMysqlSource.ID
		rep, err = client.SotDescribe(ctx, &i)
		require.Nil(t, err, "%+v", err)
		i.ID = TableMysqlDest.ID
		rep, err = client.SotDescribe(ctx, &i)
		require.Nil(t, err, "%+v", err)
		i.ID = TableS3Source.ID
		rep, err = client.SotDescribe(ctx, &i)
		require.Nil(t, err, "%+v", err)
		i.ID = TableS3Dest.ID
		rep, err = client.SotDescribe(ctx, &i)
		require.Nil(t, err, "%+v", err)
		i.ID = TableClickHouseDest.ID
		rep, err = client.SotDescribe(ctx, &i)
		require.Nil(t, err, "%+v", err)
		i.ID = TableUDFSource.ID
		rep, err = client.SotDescribe(ctx, &i)
		require.Nil(t, err, "%+v", err)
		i.ID = TableUDFDest.ID
		rep, err = client.SotDescribe(ctx, &i)
		require.Nil(t, err, "%+v", err)
		i.ID = TableHbaseSource.ID
		rep, err = client.SotDescribe(ctx, &i)
		require.Nil(t, err, "%+v", err)
		i.ID = TableHbaseDest.ID
		rep, err = client.SotDescribe(ctx, &i)
		require.Nil(t, err, "%+v", err)
	} else {
		i.ID = id
		rep, err = client.SotDescribe(ctx, &i)
		require.Nil(t, err, "%+v", err)
		return rep
	}
	return nil
}

func Test_SotDescribe(t *testing.T) {
	tablesDescribe(t, "")
}

func Test_SotUpdate(t *testing.T) {
	var i smpb.SotUpdateRequest
	var err error

	i.Comment = "tablesUpdate"
	i.ID = TableMysqlSource.ID
	i.Name = TableMysqlSource.Name
	i.Url = TableMysqlSource.Url
	_, err = client.SotUpdate(ctx, &i)
	require.Nil(t, err, "%+v", err)
	require.Equal(t, i.Comment, tablesDescribe(t, i.ID).Comment)

	i.ID = TableMysqlSource.ID
	i.Name = "xx.xx"
	i.Url = TableMysqlSource.Url
	_, err = client.SotUpdate(ctx, &i)
	require.NotNil(t, err, "%+v", err)
	require.Equal(t, qerror.InvalidSourceName.Code(), errorCode(err))

	i.ID = TableKafka.ID
	i.Name = TableKafka.Name
	i.Url = "err, json, url"
	_, err = client.SotUpdate(ctx, &i)
	require.NotNil(t, err, "%+v", err)
	require.Equal(t, qerror.InvalidJSON.Code(), errorCode(err))

	i.ID = TableMysqlSource.ID
	i.Name = TableMysqlDest.Name
	i.Url = TableMysqlSource.Url
	_, err = client.SotUpdate(ctx, &i)
	require.NotNil(t, err, "%+v", err)
	require.Equal(t, qerror.ResourceAlreadyExists.Code(), errorCode(err))
}

func Test_PrepareDelete(t *testing.T) {
	managerDelete(t, "", true)
}

func Test_SotDelete(t *testing.T) {
	tablesDelete(t, "")
}

func Test_Delete(t *testing.T) {
	managerDelete(t, "", false)
}

func Test_CreateObjects(t *testing.T) {
	Test_Create(t)
	Test_Update(t)

	Test_SotCreate(t)
	Test_SotUpdate(t)
}

func Test_Disable(t *testing.T) {
	mainInit(t)
	var v smpb.StateRequest
	var err error

	v.ID = MysqlManager.ID

	_, err = client.Disable(ctx, &v)
	require.Nil(t, err, "%+v", err)

	var i smpb.UpdateRequest

	i.Name = MysqlManager.Name
	i.ID = MysqlManager.ID
	i.Comment = "updateok"
	i.SourceType = MysqlManager.SourceType
	i.Url = MysqlManager.Url
	_, err = client.Update(ctx, &i)
	require.NotNil(t, err, "%+v", err)
	require.Equal(t, qerror.SourceIsDisable.Code(), errorCode(err))

	var i1 smpb.SotDescribeRequest

	i1.ID = TableMysqlSource.ID
	_, err = client.SotDescribe(ctx, &i1)
	require.NotNil(t, err, "%+v", err)
	require.Equal(t, qerror.SourceIsDisable.Code(), errorCode(err))
}

func Test_Enable(t *testing.T) {
	mainInit(t)
	var v smpb.StateRequest
	var err error

	v.ID = MysqlManager.ID

	_, err = client.Enable(ctx, &v)
	require.Nil(t, err, "%+v", err)
}

func Test_Clean(t *testing.T) {
	mainInit(t)
	Clean(t)
}

func Test_CreateObjectsRepeat(t *testing.T) {
	Test_Create(t)
	Test_Update(t)

	Test_SotCreate(t)
	Test_SotUpdate(t)
}
