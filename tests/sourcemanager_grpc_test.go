package tests

import (
	"context"
	"fmt"
	"testing"

	"github.com/DataWorkbench/glog"
	"github.com/stretchr/testify/require"

	"github.com/DataWorkbench/common/grpcwrap"
	"github.com/DataWorkbench/common/utils/idgenerator"

	"github.com/DataWorkbench/gproto/pkg/smpb"
)

var infos []smpb.CreateRequest
var sotInfos []smpb.SotCreateRequest
var client smpb.SourcemanagerClient
var ctx context.Context

func mainInit(t *testing.T) {
	if len(infos) != 0 {
		return
	}

	// MySQL
	infos = append(infos, smpb.CreateRequest{
		ID: "som-0123456789012345", SpaceID: "wsp-0123456789012345", EngineType: "Flink", SourceType: "MySQL", Name: "mysql", Comment: "create ok", Creator: "workbench", Url: `{"user": "root","password": "123456", "host":"127.0.0.1", "port":"3306", "database":"data_workbench", "connector_options": ""}`})
	// PostgreSQL
	infos = append(infos, smpb.CreateRequest{
		ID: "som-0123456789012346", SpaceID: "wsp-0123456789012345", EngineType: "Flink", SourceType: "PostgreSQL", Name: "pg", Comment: "create ok", Creator: "custom", Url: `{"user": "lzzhang","password": "123456", "host":"127.0.0.1", "port":"5432", "database":"lzzhang", "connector_options": ""}`})
	// Kafka
	infos = append(infos, smpb.CreateRequest{
		ID: "som-0123456789012347", SpaceID: "wsp-0123456789012345", EngineType: "Flink", SourceType: "Kafka", Name: "kafka", Comment: "create ok", Creator: "custom", Url: `{"host":"127.0.0.1", "port":"9092", "connector_options": ""}`})

	// update/pingfailed -> comment : updateok
	infos = append(infos, smpb.CreateRequest{
		ID: "som-0123456789012348", SpaceID: "wsp-0123456789012346", EngineType: "Flink", SourceType: "MySQL", Name: "pingfailed", Comment: "create ok", Creator: "workbench", Url: `{"user": "failed","password": "123456", "host":"127.0.0.1", "port":"3306", "database":"data_workbench", "connector_options": ""}`})
	// delete
	infos = append(infos, smpb.CreateRequest{
		ID: "som-0123456789012349", SpaceID: "wsp-0123456789012346", EngineType: "Flink", SourceType: "MySQL", Name: "mysql", Comment: "create ok", Creator: "workbench", Url: `{"user": "root","password": "123456", "host":"127.0.0.1", "port":"3306", "database":"data_workbench", "connector_options": ""}`})
	// create failed, name exist
	infos = append(infos, smpb.CreateRequest{
		ID: "som-0123456789012350", SpaceID: "wsp-0123456789012346", EngineType: "Flink", SourceType: "MySQL", Name: "mysql", Comment: "create failed", Creator: "workbench", Url: `{"user": "root","password": "123456", "host":"127.0.0.1", "port":"3306", "database":"data_workbench", "connector_options": ""}`})
	// create failed, can't use '.'
	infos = append(infos, smpb.CreateRequest{
		ID: "som-0123456789012350", SpaceID: "wsp-0123456789012346", EngineType: "Flink", SourceType: "MySQL", Name: "hello.world", Comment: "create failed", Creator: "workbench", Url: `{"user": "root","password": "123456", "host":"127.0.0.1", "port":"3306", "database":"data_workbench", "connector_options": ""}`})

	// Source Tables
	// pg
	sotInfos = append(sotInfos, smpb.SotCreateRequest{
		ID: "sot-0123456789012346", SourceID: "som-0123456789012346", Name: "pd", Comment: "from ms", Url: `{"sqlColumn": "(id bigint, id1 bigint)","connector_options": ""}`, TabType: "c"})
	// mysql
	sotInfos = append(sotInfos, smpb.SotCreateRequest{
		ID: "sot-0123456789012345", SourceID: "som-0123456789012345", Name: "ms", Comment: "to pg", Url: `{"sqlColumn": "(id bigint, id1 bigint)","connector_options": ""}`, TabType: "c"})
	sotInfos = append(sotInfos, smpb.SotCreateRequest{
		ID: "sot-0123456789012347", SourceID: "som-0123456789012345", Name: "mw", Comment: "join dimension table", Url: `{"sqlColumn": "(rate bigint, dbmoney varchar(8)) ","connector_options": ""}`, TabType: "d"})
	sotInfos = append(sotInfos, smpb.SotCreateRequest{
		ID: "sot-0123456789012348", SourceID: "som-0123456789012345", Name: "mwd", Comment: "join dimension table", Url: `{"sqlColumn": "(total bigint)",  "connector_options": ""}`, TabType: "c"})
	sotInfos = append(sotInfos, smpb.SotCreateRequest{
		ID: "sot-0123456789012349", SourceID: "som-0123456789012345", Name: "mc", Comment: "join common table", Url: `{"sqlColumn": "(rate bigint, dbmoney varchar(8))","connector_options": ""}`, TabType: "c"})
	sotInfos = append(sotInfos, smpb.SotCreateRequest{
		ID: "sot-0123456789012350", SourceID: "som-0123456789012345", Name: "mcd", Comment: "join common table", Url: `{"sqlColumn": "(total bigint)", "connector_options": ""}`, TabType: "c"}) //'connector.write.flush.max-rows' = '1'
	// kafka
	sotInfos = append(sotInfos, smpb.SotCreateRequest{
		ID: "sot-0123456789012351", SourceID: "som-0123456789012347", Name: "billing", Comment: "Kafka", Url: `{"sqlColumn": "(paycount bigint, paymoney string, tproctime AS PROCTIME())", "topic": "workbench","groupid": "workbench", "format":"json", "connector_options": " 'json.fail-on-missing-field' = 'false', 'json.ignore-parse-errors' = 'true' "}`, TabType: "c"}) //{"paycount": 2, "paymoney": "EUR"} {"paycount": 1, "paymoney": "USD"}

	// update -> comment:update
	sotInfos = append(sotInfos, smpb.SotCreateRequest{
		ID: "sot-0123456789012352", SourceID: "som-0123456789012345", Name: "toupd", Comment: "join common table", Url: `{"sqlColumn": "(rate bigint, dbmoney varchar(8))","connector_options": ""}`, TabType: "c"})
	// delete
	sotInfos = append(sotInfos, smpb.SotCreateRequest{
		ID: "sot-0123456789012353", SourceID: "som-0123456789012345", Name: "todel", Comment: "join common table", Url: `{"sqlColumn": "(rate bigint, dbmoney varchar(8))","connector_options": ""}`, TabType: "c"})
	// create failed: name exists
	sotInfos = append(sotInfos, smpb.SotCreateRequest{
		ID: "sot-0123456789012354", SourceID: "som-0123456789012345", Name: "toupd", Comment: "join common table", Url: `{"sqlColumn": "(rate bigint, dbmoney varchar(8))","connector_options": ""}`, TabType: "c"})

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

// Source Manager
func TestSourceManagerGRPC_Create(t *testing.T) {
	mainInit(t)

	for info := range infos {
		_, err := client.Create(ctx, &infos[info])
		if info == 5 || info == 6 {
			require.NotNil(t, err, "%+v", err)
		} else {
			require.Nil(t, err, "%+v", err)
		}
	}
}

func TestSourceManagerGRPC_Update(t *testing.T) {
	var info *smpb.CreateRequest
	var i smpb.UpdateRequest

	mainInit(t)

	info = &infos[3]
	i.Name = info.Name
	i.ID = info.ID
	i.Comment = "updateok"
	i.SourceType = info.SourceType
	i.Url = info.Url

	_, err := client.Update(ctx, &i)
	require.Nil(t, err, "%+v", err)
}

func TestSourceManagerGRPC_Delete(t *testing.T) {
	var info *smpb.CreateRequest
	var i smpb.DeleteRequest

	mainInit(t)

	info = &infos[4]
	i.ID = info.ID

	_, err := client.Delete(ctx, &i)
	require.Nil(t, err, "%+v", err)
}

func TestSourceManagerGRPC_Lists(t *testing.T) {
	var i smpb.ListsRequest

	mainInit(t)
	i.Limit = 100
	i.Offset = 0

	lists, err := client.List(ctx, &i)
	require.Nil(t, err, "%+v", err)
	fmt.Printf("%#v", lists)
}

func TestSourceManagerGRPC_Describe(t *testing.T) {
	var info *smpb.CreateRequest
	var i smpb.DescribeRequest

	mainInit(t)

	info = &infos[0]
	i.ID = info.ID

	rep, err := client.Describe(ctx, &i)
	require.Nil(t, err, "%+v", err)

	fmt.Printf("%#v", rep)
}

func TestSourceManagerGRPC_PingSource(t *testing.T) {
	var info *smpb.CreateRequest
	var i smpb.PingSourceRequest

	mainInit(t)

	// pingok
	info = &infos[0]
	i.SourceType = info.SourceType
	i.Url = info.Url
	i.EngineType = info.EngineType

	_, err := client.PingSource(ctx, &i)
	require.Nil(t, err, "%+v", err)

	// pingfailed
	info = &infos[3]
	i.SourceType = info.SourceType
	i.Url = info.Url
	i.EngineType = info.EngineType

	_, err = client.PingSource(ctx, &i)
	require.NotNil(t, err, "%+v", err)
}

func TestSourceManagerGRPC_EngineMap(t *testing.T) {
	var i smpb.EngingMapRequest

	mainInit(t)
	i.EngineType = "Flink"

	reply, err := client.EngineMap(ctx, &i)
	require.Nil(t, err, "%+v", err)
	fmt.Printf("%#v", reply)
}

// Source Tables
func TestSourceManagerGRPC_SotCreate(t *testing.T) {
	mainInit(t)

	for info := range sotInfos {
		_, err := client.SotCreate(ctx, &sotInfos[info])
		if info == 9 {
			require.NotNil(t, err, "%+v", err)
		} else {
			require.Nil(t, err, "%+v", err)
		}
	}
}

func TestSourceManagerGRPC_SotUpdate(t *testing.T) {
	var info *smpb.SotCreateRequest
	var i smpb.SotUpdateRequest

	mainInit(t)

	info = &sotInfos[7]
	i.Comment = "Update"
	i.ID = info.ID
	i.Name = info.Name
	i.Url = info.Url

	_, err := client.SotUpdate(ctx, &i)
	require.Nil(t, err, "%+v", err)
}

func TestSourceManagerGRPC_SotDelete(t *testing.T) {
	var info *smpb.SotCreateRequest
	var i smpb.SotDeleteRequest

	mainInit(t)

	info = &sotInfos[8]
	i.ID = info.ID

	_, err := client.SotDelete(ctx, &i)
	require.Nil(t, err, "%+v", err)
}

func TestSourceManagerGRPC_SotLists(t *testing.T) {
	var info *smpb.SotCreateRequest
	var i smpb.SotListsRequest

	mainInit(t)
	info = &sotInfos[1]
	i.SourceID = info.SourceID
	i.Limit = 100
	i.Offset = 0

	lists, err := client.SotList(ctx, &i)
	require.Nil(t, err, "%+v", err)
	fmt.Printf("%#v", lists)
}

func TestSourceManagerGRPC_SotDescribe(t *testing.T) {
	var info *smpb.SotCreateRequest
	var i smpb.SotDescribeRequest

	mainInit(t)

	info = &sotInfos[0]
	i.ID = info.ID

	rep, err := client.SotDescribe(ctx, &i)
	require.Nil(t, err, "%+v", err)
	fmt.Printf("%#v", rep)
}
