-- DATABASE
CREATE DATABASE IF NOT EXISTS data_workbench;
USE data_workbench;

create table source_utile(
	engine_type varchar(16),
	data_type varchar(1024),
	data_format varchar(256),
	source_kind longtext
) ENGINE=InnoDB;
alter table source_utile add constraint source_utile_pkey primary key(engine_type);

insert into source_utile(engine_type, data_type, data_format) values('flink', '{"json_list": ["STRING", "BOOLEAN", "BYTES", "DECIMAL", "TINYINT", "SMALLINT", "INTEGER", "BIGINT", "FLOAT", "DOUBLE", "DATE", "TIME", "TIMESTAMP", "TIMESTAMP WITH LOCAL TIME ZONE", "INTERVAL", "ARRAY", "MULTISET", "MAP", "ROW", "RAW"]}', '{"json_list": ["csv", "json", "avro", "debezium-json", "canal-json", "parquet", "orc"]}');
update source_utile set source_kind = '{"kinds": [{"name":"MySQL"}, {"name": "PostgreSQL"}, {"name":"S3"}, {"name":"ClickHouse"} ,{"name":"Hbase"} ,{"name":"Kafka"} ,{"name":"Ftp"}, {"name":"HDFS"}]}' where engine_type='flink';


-- sourceManagerTable
create table source_manager(
	source_id varchar(24),
	space_id varchar(24) not null,
	source_type int,
	name varchar(64) not null,
	comment varchar(256),
	url varchar(8000),
	created BIGINT(20) UNSIGNED NOT NULL,
	updated BIGINT(20) UNSIGNED NOT NULL,
	status int,
	create_by varchar(64),
	connection int
) ENGINE=InnoDB;
alter table source_manager add constraint source_manager_pkey primary key(source_id);
create index sourcemanager_unique ON source_manager (space_id, name, status);
alter table source_manager add CONSTRAINT source_manager_type check(source_type >= 1 and source_type <= 8);
alter table source_manager add constraint source_manager_status check (status >= 1 and status <= 3);
alter table source_manager add constraint source_manager_connection check (connection >= 1 and connection <= 3);

-- tableManagerTable
create table source_tables(
	table_id varchar(24),
	source_id varchar(24) not null,
	space_id varchar(24) not null,
	name varchar(64) not null,
	comment varchar(256),
	table_schema varchar(8000),
	created BIGINT(20) UNSIGNED NOT NULL,
	updated BIGINT(20) UNSIGNED NOT NULL,
	status int,
	create_by varchar(64),
	table_kind int,
	foreign key(source_id) references source_manager(source_id)
) ENGINE=InnoDB;
alter table source_tables add constraint sourcetables_pkey primary key(table_id);
create index source_tables_unique ON source_tables (space_id, name, status);
