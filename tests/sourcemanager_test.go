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

var mMysql smpb.CreateRequest //name mysql
var mPG smpb.CreateRequest
var mKafka smpb.CreateRequest
var mNewSpace smpb.CreateRequest        // name mysql
var mNameExists smpb.CreateRequest      //create failed
var mNameError smpb.CreateRequest       //create failed
var mJsonError smpb.CreateRequest       //create failed
var mEngintTypeError smpb.CreateRequest //create failed
var mSourceTypeError smpb.CreateRequest //create failed
var mS3 smpb.CreateRequest
var mCK smpb.CreateRequest

var tPG smpb.SotCreateRequest
var tKafka smpb.SotCreateRequest
var tMysql smpb.SotCreateRequest
var tMysqlDest smpb.SotCreateRequest
var tDimensionError smpb.SotCreateRequest
var tNameExists smpb.SotCreateRequest
var tNameError smpb.SotCreateRequest
var tJsonError smpb.SotCreateRequest
var tManagerError smpb.SotCreateRequest
var tmw smpb.SotCreateRequest
var tmwd smpb.SotCreateRequest
var tmc smpb.SotCreateRequest
var tmcd smpb.SotCreateRequest
var ts3s smpb.SotCreateRequest
var ts3d smpb.SotCreateRequest
var tCKd smpb.SotCreateRequest // click support sink only.
var tUdfs smpb.SotCreateRequest
var tUdfd smpb.SotCreateRequest

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
	mMysql = smpb.CreateRequest{ID: "som-0123456789012345", SpaceID: "wks-0123456789012345", EngineType: constants.ServerTypeFlink, SourceType: constants.SourceTypeMysql, Name: "mysql", Comment: "create ok", Creator: constants.CreatorWorkBench, Url: typeToJsonString(constants.SourceMysqlParams{User: "root", Password: "123456", Host: "127.0.0.1", Port: 3306, Database: "data_workbench"})}
	mPG = smpb.CreateRequest{ID: "som-0123456789012346", SpaceID: "wks-0123456789012345", EngineType: constants.ServerTypeFlink, SourceType: constants.SourceTypePostgreSQL, Name: "pg", Comment: "create ok", Creator: constants.CreatorCustom, Url: typeToJsonString(constants.SourcePostgreSQLParams{User: "lzzhang", Password: "123456", Host: "127.0.0.1", Port: 5432, Database: "lzzhang"})}
	mKafka = smpb.CreateRequest{ID: "som-0123456789012347", SpaceID: "wks-0123456789012345", EngineType: constants.ServerTypeFlink, SourceType: constants.SourceTypeKafka, Name: "kafka", Comment: "create ok", Creator: constants.CreatorCustom, Url: typeToJsonString(constants.SourceKafkaParams{Host: "127.0.0.1", Port: 9092})}
	mNameExists = smpb.CreateRequest{ID: "som-0123456789012348", SpaceID: "wks-0123456789012345", EngineType: constants.ServerTypeFlink, SourceType: constants.SourceTypeMysql, Name: "mysql", Comment: "create ok", Creator: constants.CreatorWorkBench, Url: typeToJsonString(constants.SourceMysqlParams{User: "root", Password: "123456", Host: "127.0.0.1", Port: 3306, Database: "data_workbench"})}
	mNameError = smpb.CreateRequest{ID: "som-0123456789012349", SpaceID: "wks-0123456789012345", EngineType: constants.ServerTypeFlink, SourceType: constants.SourceTypeMysql, Name: "hello.world", Comment: "create failed", Creator: constants.CreatorWorkBench, Url: typeToJsonString(constants.SourceMysqlParams{User: "root", Password: "123456", Host: "127.0.0.1", Port: 3306, Database: "data_workbench"})}
	mNewSpace = smpb.CreateRequest{ID: "som-0123456789012350", SpaceID: "wks-0123456789012346", EngineType: constants.ServerTypeFlink, SourceType: constants.SourceTypeMysql, Name: "mysql", Comment: "newspace", Creator: constants.CreatorWorkBench, Url: typeToJsonString(constants.SourceMysqlParams{User: "root", Password: "123456", Host: "127.0.0.1", Port: 3306, Database: "data_workbench"})}
	mJsonError = smpb.CreateRequest{ID: "som-0123456789012351", SpaceID: "wks-0123456789012346", EngineType: constants.ServerTypeFlink, SourceType: constants.SourceTypeMysql, Name: "mysql", Comment: "create failed", Creator: constants.CreatorWorkBench, Url: "Error.json . error"}
	mEngintTypeError = smpb.CreateRequest{ID: "som-0123456789012352", SpaceID: "wks-0123456789012346", EngineType: "NotFlink", SourceType: constants.SourceTypeMysql, Name: "mysql", Comment: "create failed", Creator: constants.CreatorWorkBench, Url: typeToJsonString(constants.SourceMysqlParams{User: "root", Password: "123456", Host: "127.0.0.1", Port: 3306, Database: "data_workbench"})}
	mSourceTypeError = smpb.CreateRequest{ID: "som-0123456789012353", SpaceID: "wks-0123456789012346", EngineType: constants.ServerTypeFlink, SourceType: "ErrorSourceType", Name: "mysql", Comment: "create failed", Creator: constants.CreatorWorkBench, Url: typeToJsonString(constants.SourceMysqlParams{User: "root", Password: "123456", Host: "127.0.0.1", Port: 3306, Database: "data_workbench"})}
	mS3 = smpb.CreateRequest{ID: "som-0123456789012354", SpaceID: "wks-0123456789012345", EngineType: constants.ServerTypeFlink, SourceType: constants.SourceTypeS3, Name: "s3", Comment: "qingcloud s3", Creator: constants.CreatorCustom, Url: typeToJsonString(constants.SourceS3Params{AccessKey: "RDTHDPNFWWDNWPIHESWK", SecretKey: "sVbVhAUsKGPPdiTOPAgveqCNhFjtvXFNpsPnQ7Hx", EndPoint: "http://s3.gd2.qingstor.com"})} // s3 not is url bucket
	mCK = smpb.CreateRequest{ID: "som-0123456789012355", SpaceID: "wks-0123456789012345", EngineType: constants.ServerTypeFlink, SourceType: constants.SourceTypeClickHouse, Name: "clickhouse", Comment: "clickhouse", Creator: constants.CreatorCustom, Url: typeToJsonString(constants.SourceClickHouseParams{User: "default", Password: "", Host: "127.0.0.1", Port: 8123, Database: "default"})}

	// Source Tables
	tPG = smpb.SotCreateRequest{ID: "sot-0123456789012345", SourceID: mPG.ID, Name: "pd", Comment: "postgresql", Url: typeToJsonString(constants.FlinkTableDefinePostgreSQL{SqlColumn: []string{"id bigint", "id1 bigint"}}), TabType: constants.TableTypeDimension}
	tKafka = smpb.SotCreateRequest{ID: "sot-0123456789012346", SourceID: mKafka.ID, Name: "billing", Comment: "Kafka", Url: typeToJsonString(constants.FlinkTableDefineKafka{SqlColumn: []string{"paycount bigint", "paymoney string", "tproctime AS PROCTIME()"}, Topic: "workbench", Format: "json", ConnectorOptions: []string{"'json.fail-on-missing-field' = 'false'", "'json.ignore-parse-errors' = 'true'"}}), TabType: constants.TableTypeCommon} //{"paycount": 2, "paymoney": "EUR"} {"paycount": 1, "paymoney": "USD"}
	tMysql = smpb.SotCreateRequest{ID: "sot-0123456789012347", SourceID: mMysql.ID, Name: "ms", Comment: "mysql", Url: typeToJsonString(constants.FlinkTableDefineMysql{SqlColumn: []string{"id bigint", "id1 bigint"}}), TabType: constants.TableTypeCommon}
	tMysqlDest = smpb.SotCreateRequest{ID: "sot-0123456789012348", SourceID: mMysql.ID, Name: "pd", Comment: "mysqldest", Url: typeToJsonString(constants.FlinkTableDefineMysql{SqlColumn: []string{"id bigint", "id1 bigint"}}), TabType: constants.TableTypeCommon}
	tDimensionError = smpb.SotCreateRequest{ID: "sot-0123456789012350", SourceID: mKafka.ID, Name: "dimensionerror", Comment: "Kafka", Url: typeToJsonString(constants.FlinkTableDefineKafka{SqlColumn: []string{"paycount bigint", "paymoney string", "tproctime AS PROCTIME()"}, Topic: "workbench", Format: "json", ConnectorOptions: []string{"'json.fail-on-missing-field' = 'false'", "'json.ignore-parse-errors' = 'true'"}}), TabType: constants.TableTypeDimension}
	tNameExists = smpb.SotCreateRequest{ID: "sot-0123456789012351", SourceID: mMysql.ID, Name: "ms", Comment: "to pd", Url: typeToJsonString(constants.FlinkTableDefineMysql{SqlColumn: []string{"id bigint", "id1 bigint"}}), TabType: constants.TableTypeCommon}
	tNameError = smpb.SotCreateRequest{ID: "sot-0123456789012352", SourceID: mMysql.ID, Name: "ms.ms", Comment: "to pd", Url: typeToJsonString(constants.FlinkTableDefineMysql{SqlColumn: []string{"id bigint", "id1 bigint"}}), TabType: constants.TableTypeCommon}
	tJsonError = smpb.SotCreateRequest{ID: "sot-0123456789012353", SourceID: mMysql.ID, Name: "ms1", Comment: "to pd", Url: "sss,xx,xx, xx", TabType: constants.TableTypeCommon}
	tManagerError = smpb.SotCreateRequest{ID: "sot-0123456789012354", SourceID: "sot-xxxxyyyyzzzzxxxx", Name: "ms2", Comment: "to pd", Url: typeToJsonString(constants.FlinkTableDefineMysql{SqlColumn: []string{"id bigint", "id1 bigint"}}), TabType: constants.TableTypeCommon}
	tmw = smpb.SotCreateRequest{ID: "sot-0123456789012355", SourceID: mMysql.ID, Name: "mw", Comment: "join dimension table", Url: typeToJsonString(constants.FlinkTableDefineMysql{SqlColumn: []string{"rate bigint", "dbmoney varchar(8)"}}), TabType: constants.TableTypeDimension}
	tmwd = smpb.SotCreateRequest{ID: "sot-0123456789012356", SourceID: mMysql.ID, Name: "mwd", Comment: "join dimension table", Url: typeToJsonString(constants.FlinkTableDefineMysql{SqlColumn: []string{"total bigint"}}), TabType: constants.TableTypeCommon}
	tmc = smpb.SotCreateRequest{ID: "sot-0123456789012357", SourceID: mMysql.ID, Name: "mc", Comment: "join common table", Url: typeToJsonString(constants.FlinkTableDefineMysql{SqlColumn: []string{"rate bigint", "dbmoney varchar(8)"}}), TabType: constants.TableTypeCommon}
	tmcd = smpb.SotCreateRequest{ID: "sot-0123456789012358", SourceID: mMysql.ID, Name: "mcd", Comment: "join common table", Url: typeToJsonString(constants.FlinkTableDefineMysql{SqlColumn: []string{"total bigint"}}), TabType: constants.TableTypeCommon} //'connector.write.flush.max-rows' = '1'
	ts3s = smpb.SotCreateRequest{ID: "sot-0123456789012359", SourceID: mS3.ID, Name: "s3s", Comment: "s3 source", Url: typeToJsonString(constants.FlinkTableDefineS3{SqlColumn: []string{"id bigint", "id1 bigint"}, Path: "s3a://filesystem/source", Format: "json"}), TabType: constants.TableTypeCommon}
	ts3d = smpb.SotCreateRequest{ID: "sot-0123456789012360", SourceID: mS3.ID, Name: "s3d", Comment: "s3 destination", Url: typeToJsonString(constants.FlinkTableDefineS3{SqlColumn: []string{"id bigint", "id1 bigint"}, Path: "s3a://filesystem/destination", Format: "json"}), TabType: constants.TableTypeCommon}
	tCKd = smpb.SotCreateRequest{ID: "sot-0123456789012361", SourceID: mCK.ID, Name: "ck", Comment: "clickhouse destination", Url: typeToJsonString(constants.FlinkTableDefineClickHouse{SqlColumn: []string{"id bigint", "id1 bigint"}}), TabType: constants.TableTypeCommon}
	tUdfs = smpb.SotCreateRequest{ID: "sot-0123456789012362", SourceID: mMysql.ID, Name: "udfs", Comment: "udfs", Url: typeToJsonString(constants.FlinkTableDefineMysql{SqlColumn: []string{"a varchar(10)"}}), TabType: constants.TableTypeCommon}
	tUdfd = smpb.SotCreateRequest{ID: "sot-0123456789012363", SourceID: mMysql.ID, Name: "udfd", Comment: "udfd", Url: typeToJsonString(constants.FlinkTableDefineMysql{SqlColumn: []string{"a varchar(10)"}}), TabType: constants.TableTypeCommon}

	address := "127.0.0.1:50001"
	lp := glog.NewDefault()
	ctx = glog.WithContext(context.Background(), lp)

	conn, err := grpcwrap.NewConn(ctx, &grpcwrap.ClientConfig{
		Address:      address,
		LogLevel:     2,
		LogVerbosity: 99,
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

	_, err = client.Create(ctx, &mMysql)
	require.Nil(t, err, "%+v", err)
	_, err = client.Create(ctx, &mPG)
	require.Nil(t, err, "%+v", err)
	_, err = client.Create(ctx, &mKafka)
	require.Nil(t, err, "%+v", err)
	_, err = client.Create(ctx, &mNewSpace)
	require.Nil(t, err, "%+v", err)
	_, err = client.Create(ctx, &mNameError)
	require.Equal(t, errorCode(err), qerror.InvalidSourceName.Code())
	_, err = client.Create(ctx, &mNameExists)
	require.Equal(t, errorCode(err), qerror.ResourceAlreadyExists.Code())
	_, err = client.Create(ctx, &mJsonError)
	require.Equal(t, errorCode(err), qerror.InvalidJSON.Code())
	_, err = client.Create(ctx, &mSourceTypeError)
	require.Equal(t, errorCode(err), qerror.NotSupportSourceType.Code())
	_, err = client.Create(ctx, &mEngintTypeError)
	require.Equal(t, errorCode(err), qerror.NotSupportEngineType.Code())
	_, err = client.Create(ctx, &mS3)
	require.Nil(t, err, "%+v", err)
	_, err = client.Create(ctx, &mCK)
	require.Nil(t, err, "%+v", err)
}

func Test_PingSource(t *testing.T) {
	mainInit(t)

	var p smpb.PingSourceRequest
	var err error

	p.SourceType = mMysql.SourceType
	p.Url = mMysql.Url
	p.EngineType = mMysql.EngineType
	_, err = client.PingSource(ctx, &p)
	require.Nil(t, err, "%+v", err)

	p.SourceType = mPG.SourceType
	p.Url = mPG.Url
	p.EngineType = mPG.EngineType
	_, err = client.PingSource(ctx, &p)
	require.Equal(t, errorCode(err), qerror.ConnectSourceFailed.Code())

	p.SourceType = mKafka.SourceType
	p.Url = mKafka.Url
	p.EngineType = mKafka.EngineType
	_, err = client.PingSource(ctx, &p)
	require.Equal(t, errorCode(err), qerror.ConnectSourceFailed.Code())

	p.SourceType = mS3.SourceType
	p.Url = mS3.Url
	p.EngineType = mS3.EngineType
	_, err = client.PingSource(ctx, &p)
	require.Nil(t, err, "%+v", err)
	//require.Equal(t, errorCode(err), qerror.ConnectSourceFailed.Code())

	p.SourceType = mCK.SourceType
	p.Url = mCK.Url
	p.EngineType = mCK.EngineType
	_, err = client.PingSource(ctx, &p)
	require.Equal(t, errorCode(err), qerror.ConnectSourceFailed.Code())
	//require.Nil(t, err, "%+v", err)
}

func Test_EngineMap(t *testing.T) {
	var i smpb.EngingMapRequest

	i.EngineType = "flink"

	reply, err := client.EngineMap(ctx, &i)
	require.Nil(t, err, "%+v", err)
	require.Equal(t, strings.Split(reply.SourceType, ",")[0], constants.SourceTypeMysql)
	require.Equal(t, strings.Split(reply.SourceType, ",")[1], constants.SourceTypePostgreSQL)
	require.Equal(t, strings.Split(reply.SourceType, ",")[2], constants.SourceTypeKafka)
	require.Equal(t, strings.Split(reply.SourceType, ",")[3], constants.SourceTypeS3)
	require.Equal(t, strings.Split(reply.SourceType, ",")[4], constants.SourceTypeClickHouse)
	require.Equal(t, len(strings.Split(reply.SourceType, ",")), 5)
}

func managerDescribe(t *testing.T, id string) *smpb.InfoReply {
	var d smpb.DescribeRequest
	var err error
	var rep *smpb.InfoReply

	if id == "" {
		d.ID = mMysql.ID
		rep, err = client.Describe(ctx, &d)
		require.Nil(t, err, "%+v", err)
		require.Equal(t, d.ID, rep.ID)
		d.ID = mPG.ID
		rep, err = client.Describe(ctx, &d)
		require.Nil(t, err, "%+v", err)
		require.Equal(t, d.ID, rep.ID)
		d.ID = mKafka.ID
		rep, err = client.Describe(ctx, &d)
		require.Nil(t, err, "%+v", err)
		require.Equal(t, d.ID, rep.ID)
		d.ID = mNewSpace.ID
		rep, err = client.Describe(ctx, &d)
		require.Nil(t, err, "%+v", err)
		require.Equal(t, d.ID, rep.ID)
		d.ID = mS3.ID
		rep, err = client.Describe(ctx, &d)
		require.Nil(t, err, "%+v", err)
		require.Equal(t, d.ID, rep.ID)
		d.ID = mCK.ID
		rep, err = client.Describe(ctx, &d)
		require.Nil(t, err, "%+v", err)
		require.Equal(t, d.ID, rep.ID)
		return nil
	} else {
		d.ID = id
		rep, err = client.Describe(ctx, &d)
		require.Nil(t, err, "%+v", err)
		require.Equal(t, d.ID, rep.ID)
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

	d.SpaceID = mMysql.SpaceID
	_, err = client.DeleteAll(ctx, &d)
	require.Nil(t, err, "%+v", err)
	//for _, info := range managerLists(t, mMysql.SpaceID).Infos {
	//	for _, table := range tablesLists(t, info.ID).Infos {
	//		tablesDelete(t, table.ID)
	//	}
	//	managerDelete(t, info.ID, false)
	//}

	for _, info := range managerLists(t, mNewSpace.SpaceID).Infos {
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
		i.SpaceID = mMysql.SpaceID
		i.Limit = 100
		i.Offset = 0
		rep, err = client.List(ctx, &i)
		require.Nil(t, err, "%+v", err)
		require.Equal(t, len(rep.Infos), 5)
		i.SpaceID = mNewSpace.SpaceID
		i.Limit = 100
		i.Offset = 0
		rep, err = client.List(ctx, &i)
		require.Nil(t, err, "%+v", err)
		require.Equal(t, len(rep.Infos), 1)
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
			i.ID = mMysql.ID
			_, err = client.Delete(ctx, &i)
			require.Nil(t, err, "%+v", err)
			i.ID = mPG.ID
			_, err = client.Delete(ctx, &i)
			require.Nil(t, err, "%+v", err)
			i.ID = mKafka.ID
			_, err = client.Delete(ctx, &i)
			require.Nil(t, err, "%+v", err)
			i.ID = mS3.ID
			_, err = client.Delete(ctx, &i)
			require.Nil(t, err, "%+v", err)
			i.ID = mCK.ID
			_, err = client.Delete(ctx, &i)
			require.Nil(t, err, "%+v", err)
			i.ID = mNewSpace.ID
			_, err = client.Delete(ctx, &i)
			require.Nil(t, err, "%+v", err)
		} else {
			i.ID = mMysql.ID
			_, err = client.Delete(ctx, &i)
			require.NotNil(t, err, "%+v", err)
			require.Equal(t, errorCode(err), qerror.ResourceIsUsing.Code())
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

	i.Name = mMysql.Name
	i.ID = mMysql.ID
	i.Comment = "updateok"
	i.SourceType = mMysql.SourceType
	i.Url = mMysql.Url
	_, err = client.Update(ctx, &i)
	require.Nil(t, err, "%+v", err)
	require.Equal(t, i.Comment, managerDescribe(t, i.ID).Comment)

	i.Name = mPG.Name
	i.ID = mMysql.ID
	i.SourceType = mMysql.SourceType
	i.Url = mMysql.Url
	_, err = client.Update(ctx, &i)
	require.NotNil(t, err, "%+v", err)
	require.Equal(t, errorCode(err), qerror.ResourceAlreadyExists.Code())
}

// Source Tables
func Test_SotCreate(t *testing.T) {
	var err error
	_, err = client.SotCreate(ctx, &tPG)
	require.Nil(t, err, "%+v", err)
	_, err = client.SotCreate(ctx, &tKafka)
	require.Nil(t, err, "%+v", err)
	_, err = client.SotCreate(ctx, &tMysql)
	require.Nil(t, err, "%+v", err)
	_, err = client.SotCreate(ctx, &tMysqlDest)
	require.Nil(t, err, "%+v", err)
	_, err = client.SotCreate(ctx, &tDimensionError)
	require.NotNil(t, err, "%+v", err)
	require.Equal(t, errorCode(err), qerror.InvalidDimensionSource.Code())
	_, err = client.SotCreate(ctx, &tNameExists)
	require.NotNil(t, err, "%+v", err)
	require.Equal(t, errorCode(err), qerror.ResourceAlreadyExists.Code())
	_, err = client.SotCreate(ctx, &tNameError)
	require.NotNil(t, err, "%+v", err)
	require.Equal(t, errorCode(err), qerror.InvalidSourceName.Code())
	_, err = client.SotCreate(ctx, &tJsonError)
	require.NotNil(t, err, "%+v", err)
	require.Equal(t, errorCode(err), qerror.InvalidJSON.Code())
	_, err = client.SotCreate(ctx, &tManagerError)
	require.NotNil(t, err, "%+v", err)
	_, err = client.SotCreate(ctx, &tmw)
	require.Nil(t, err, "%+v", err)
	_, err = client.SotCreate(ctx, &tmwd)
	require.Nil(t, err, "%+v", err)
	_, err = client.SotCreate(ctx, &tmc)
	require.Nil(t, err, "%+v", err)
	_, err = client.SotCreate(ctx, &tmcd)
	require.Nil(t, err, "%+v", err)
	_, err = client.SotCreate(ctx, &ts3s)
	require.Nil(t, err, "%+v", err)
	_, err = client.SotCreate(ctx, &ts3d)
	require.Nil(t, err, "%+v", err)
	_, err = client.SotCreate(ctx, &tCKd)
	require.Nil(t, err, "%+v", err)
	_, err = client.SotCreate(ctx, &tUdfs)
	require.Nil(t, err, "%+v", err)
	_, err = client.SotCreate(ctx, &tUdfd)
	require.Nil(t, err, "%+v", err)
}

func tablesLists(t *testing.T, SourceID string) *smpb.SotListsReply {
	var i smpb.SotListsRequest
	var err error
	var rep *smpb.SotListsReply

	if SourceID == "" {
		i.SourceID = tPG.SourceID
		i.Limit = 100
		i.Offset = 0
		rep, err = client.SotList(ctx, &i)
		require.Nil(t, err, "%+v", err)
		require.Equal(t, len(rep.Infos), 1)

		i.SourceID = tMysql.SourceID
		i.Limit = 100
		i.Offset = 0
		rep, err = client.SotList(ctx, &i)
		require.Nil(t, err, "%+v", err)
		require.Equal(t, len(rep.Infos), 8)

		i.SourceID = tKafka.SourceID
		i.Limit = 100
		i.Offset = 0
		rep, err = client.SotList(ctx, &i)
		require.Nil(t, err, "%+v", err)
		require.Equal(t, len(rep.Infos), 1)

		i.SourceID = ts3s.SourceID
		i.Limit = 100
		i.Offset = 0
		rep, err = client.SotList(ctx, &i)
		require.Nil(t, err, "%+v", err)
		require.Equal(t, len(rep.Infos), 2)

		i.SourceID = tCKd.SourceID
		i.Limit = 100
		i.Offset = 0
		rep, err = client.SotList(ctx, &i)
		require.Nil(t, err, "%+v", err)
		require.Equal(t, len(rep.Infos), 1)
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
		i.ID = tPG.ID
		_, err = client.SotDelete(ctx, &i)
		require.Nil(t, err, "%+v", err)
		i.ID = tKafka.ID
		_, err = client.SotDelete(ctx, &i)
		require.Nil(t, err, "%+v", err)
		i.ID = tMysql.ID
		_, err = client.SotDelete(ctx, &i)
		require.Nil(t, err, "%+v", err)
		i.ID = tMysqlDest.ID
		_, err = client.SotDelete(ctx, &i)
		require.Nil(t, err, "%+v", err)
		i.ID = tmw.ID
		_, err = client.SotDelete(ctx, &i)
		require.Nil(t, err, "%+v", err)
		i.ID = tmwd.ID
		_, err = client.SotDelete(ctx, &i)
		require.Nil(t, err, "%+v", err)
		i.ID = tmc.ID
		_, err = client.SotDelete(ctx, &i)
		require.Nil(t, err, "%+v", err)
		i.ID = tmcd.ID
		_, err = client.SotDelete(ctx, &i)
		require.Nil(t, err, "%+v", err)
		i.ID = ts3s.ID
		_, err = client.SotDelete(ctx, &i)
		require.Nil(t, err, "%+v", err)
		i.ID = ts3d.ID
		_, err = client.SotDelete(ctx, &i)
		require.Nil(t, err, "%+v", err)
		i.ID = tCKd.ID
		_, err = client.SotDelete(ctx, &i)
		require.Nil(t, err, "%+v", err)
		i.ID = tUdfs.ID
		_, err = client.SotDelete(ctx, &i)
		require.Nil(t, err, "%+v", err)
		i.ID = tUdfd.ID
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
		i.ID = tPG.ID
		rep, err = client.SotDescribe(ctx, &i)
		require.Nil(t, err, "%+v", err)
		i.ID = tKafka.ID
		rep, err = client.SotDescribe(ctx, &i)
		require.Nil(t, err, "%+v", err)
		i.ID = tMysql.ID
		rep, err = client.SotDescribe(ctx, &i)
		require.Nil(t, err, "%+v", err)
		i.ID = tMysqlDest.ID
		rep, err = client.SotDescribe(ctx, &i)
		require.Nil(t, err, "%+v", err)
		i.ID = ts3s.ID
		rep, err = client.SotDescribe(ctx, &i)
		require.Nil(t, err, "%+v", err)
		i.ID = ts3d.ID
		rep, err = client.SotDescribe(ctx, &i)
		require.Nil(t, err, "%+v", err)
		i.ID = tCKd.ID
		rep, err = client.SotDescribe(ctx, &i)
		require.Nil(t, err, "%+v", err)
		i.ID = tUdfs.ID
		rep, err = client.SotDescribe(ctx, &i)
		require.Nil(t, err, "%+v", err)
		i.ID = tUdfd.ID
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
	i.ID = tMysql.ID
	i.Name = tMysql.Name
	i.Url = tMysql.Url
	i.TabType = tMysql.TabType
	_, err = client.SotUpdate(ctx, &i)
	require.Nil(t, err, "%+v", err)
	require.Equal(t, i.Comment, tablesDescribe(t, i.ID).Comment)

	i.ID = tMysql.ID
	i.Name = "xx.xx"
	i.Url = tMysql.Url
	i.TabType = tMysql.TabType
	_, err = client.SotUpdate(ctx, &i)
	require.NotNil(t, err, "%+v", err)
	require.Equal(t, errorCode(err), qerror.InvalidSourceName.Code())

	i.ID = tKafka.ID
	i.Name = tKafka.Name
	i.Url = tKafka.Url
	i.TabType = constants.TableTypeDimension
	_, err = client.SotUpdate(ctx, &i)
	require.NotNil(t, err, "%+v", err)
	require.Equal(t, errorCode(err), qerror.InvalidDimensionSource.Code())

	i.ID = tKafka.ID
	i.Name = tKafka.Name
	i.Url = "err, json, url"
	i.TabType = tKafka.TabType
	_, err = client.SotUpdate(ctx, &i)
	require.NotNil(t, err, "%+v", err)
	require.Equal(t, errorCode(err), qerror.InvalidJSON.Code())

	i.ID = tMysql.ID
	i.Name = tMysqlDest.Name
	i.Url = tMysql.Url
	i.TabType = tMysql.TabType
	_, err = client.SotUpdate(ctx, &i)
	require.NotNil(t, err, "%+v", err)
	require.Equal(t, errorCode(err), qerror.ResourceAlreadyExists.Code())
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
