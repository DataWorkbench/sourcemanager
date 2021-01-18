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

var tPG smpb.SotCreateRequest
var tKafka smpb.SotCreateRequest
var tMysql smpb.SotCreateRequest
var tMysqlDest smpb.SotCreateRequest
var tDimensionError smpb.SotCreateRequest
var tNameExists smpb.SotCreateRequest
var tNameError smpb.SotCreateRequest
var tJsonError smpb.SotCreateRequest
var tManagerError smpb.SotCreateRequest

func typeToJsonString(v interface{}) string {
	s, _ := json.Marshal(&v)
	return string(s)
}

var client smpb.SourcemanagerClient
var ctx context.Context

func mainInit(t *testing.T) {
	// Source Manager
	mMysql = smpb.CreateRequest{ID: "som-0123456789012345", SpaceID: "wks-0123456789012345", EngineType: constants.EngineTypeFlink, SourceType: constants.SourceTypeMysql, Name: "mysql", Comment: "create ok", Creator: constants.CreatorWorkBench, Url: typeToJsonString(constants.SourceMysqlParams{User: "root", Password: "123456", Host: "127.0.0.1", Port: 3306, Database: "data_workbench"})}
	mPG = smpb.CreateRequest{ID: "som-0123456789012346", SpaceID: "wks-0123456789012345", EngineType: constants.EngineTypeFlink, SourceType: constants.SourceTypePostgreSQL, Name: "pg", Comment: "create ok", Creator: constants.CreatorCustom, Url: typeToJsonString(constants.SourcePostgreSQLParams{User: "lzzhang", Password: "123456", Host: "127.0.0.1", Port: 5432, Database: "lzzhang"})}
	mKafka = smpb.CreateRequest{ID: "som-0123456789012347", SpaceID: "wks-0123456789012345", EngineType: constants.EngineTypeFlink, SourceType: constants.SourceTypeKafka, Name: "kafka", Comment: "create ok", Creator: constants.CreatorCustom, Url: typeToJsonString(constants.SourceKafkaParams{Host: "127.0.0.1", Port: 9092})}
	mNameExists = smpb.CreateRequest{ID: "som-0123456789012348", SpaceID: "wks-0123456789012345", EngineType: constants.EngineTypeFlink, SourceType: constants.SourceTypeMysql, Name: "mysql", Comment: "create ok", Creator: constants.CreatorWorkBench, Url: typeToJsonString(constants.SourceMysqlParams{User: "root", Password: "123456", Host: "127.0.0.1", Port: 3306, Database: "data_workbench"})}
	mNameError = smpb.CreateRequest{ID: "som-0123456789012349", SpaceID: "wks-0123456789012345", EngineType: constants.EngineTypeFlink, SourceType: constants.SourceTypeMysql, Name: "hello.world", Comment: "create failed", Creator: constants.CreatorWorkBench, Url: typeToJsonString(constants.SourceMysqlParams{User: "root", Password: "123456", Host: "127.0.0.1", Port: 3306, Database: "data_workbench"})}
	mNewSpace = smpb.CreateRequest{ID: "som-0123456789012350", SpaceID: "wks-0123456789012346", EngineType: constants.EngineTypeFlink, SourceType: constants.SourceTypeMysql, Name: "mysql", Comment: "create failed", Creator: constants.CreatorWorkBench, Url: typeToJsonString(constants.SourceMysqlParams{User: "root", Password: "123456", Host: "127.0.0.1", Port: 3306, Database: "data_workbench"})}
	mJsonError = smpb.CreateRequest{ID: "som-0123456789012350", SpaceID: "wks-0123456789012346", EngineType: constants.EngineTypeFlink, SourceType: constants.SourceTypeMysql, Name: "mysql", Comment: "create failed", Creator: constants.CreatorWorkBench, Url: "Error.json . error"}
	mEngintTypeError = smpb.CreateRequest{ID: "som-0123456789012350", SpaceID: "wks-0123456789012346", EngineType: "NotFlink", SourceType: constants.SourceTypeMysql, Name: "mysql", Comment: "create failed", Creator: constants.CreatorWorkBench, Url: typeToJsonString(constants.SourceMysqlParams{User: "root", Password: "123456", Host: "127.0.0.1", Port: 3306, Database: "data_workbench"})}
	mSourceTypeError = smpb.CreateRequest{ID: "som-0123456789012350", SpaceID: "wks-0123456789012346", EngineType: constants.EngineTypeFlink, SourceType: "ErrorSourceType", Name: "mysql", Comment: "create failed", Creator: constants.CreatorWorkBench, Url: typeToJsonString(constants.SourceMysqlParams{User: "root", Password: "123456", Host: "127.0.0.1", Port: 3306, Database: "data_workbench"})}

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
func TestSourceManagerGRPC_Create(t *testing.T) {
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
}

func TestSourceManagerGRPC_PingSource(t *testing.T) {
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
}

func TestSourceManagerGRPC_EngineMap(t *testing.T) {
	var i smpb.EngingMapRequest

	i.EngineType = "Flink"

	reply, err := client.EngineMap(ctx, &i)
	require.Nil(t, err, "%+v", err)
	require.Equal(t, strings.Split(reply.SourceType, ",")[0], constants.SourceTypeMysql)
	require.Equal(t, strings.Split(reply.SourceType, ",")[1], constants.SourceTypePostgreSQL)
	require.Equal(t, strings.Split(reply.SourceType, ",")[2], constants.SourceTypeKafka)
	require.Equal(t, len(strings.Split(reply.SourceType, ",")), 3)
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

func TestSourceManagerGRPC_Describe(t *testing.T) {
	managerDescribe(t, "")
}

func Clean(t *testing.T) {
	for _, info := range managerLists(t, mMysql.SpaceID).Infos {
		for _, table := range tablesLists(t, info.ID).Infos {
			tablesDelete(t, table.ID)
		}
		managerDelete(t, info.ID, false)
	}
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
		require.Equal(t, len(rep.Infos), 3)
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

func TestSourceManagerGRPC_Lists(t *testing.T) {
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

func TestSourceManagerGRPC_Update(t *testing.T) {
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
func TestSourceManagerGRPC_SotCreate(t *testing.T) {
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
		require.Equal(t, len(rep.Infos), 2)

		i.SourceID = tKafka.SourceID
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

func TestSourceManagerGRPC_SotLists(t *testing.T) {
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
	} else {
		i.ID = id
		rep, err = client.SotDescribe(ctx, &i)
		require.Nil(t, err, "%+v", err)
		return rep
	}
	return nil
}

func TestSourceManagerGRPC_SotDescribe(t *testing.T) {
	tablesDescribe(t, "")
}

func TestSourceManagerGRPC_SotUpdate(t *testing.T) {
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

func TestSourceManagerGRPC_PrepareDelete(t *testing.T) {
	managerDelete(t, "", true)
}

func TestSourceManagerGRPC_SotDelete(t *testing.T) {
	tablesDelete(t, "")
}

func TestSourceManagerGRPC_Delete(t *testing.T) {
	managerDelete(t, "", false)
}
