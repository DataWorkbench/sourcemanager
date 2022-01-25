package datasource

import "github.com/DataWorkbench/gproto/pkg/datasourcepb"

var SupportedSourceKinds = []*datasourcepb.SourceKind{
	{Name: "MySQL"},
	{Name: "PostgreSQL"},
	{Name: "ClickHouse"},
	{Name: "Kafka"},
	{Name: "Hbase"},
	{Name: "Ftp"},
	{Name: "HDFS"},
}
