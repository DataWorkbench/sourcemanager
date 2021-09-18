-- DATABASE
CREATE DATABASE IF NOT EXISTS data_workbench;
USE data_workbench;

create table sourceutile(
	enginetype varchar(16),
	datatype varchar(1024),
	dataformat varchar(256),
	sourcekind longtext
) ENGINE=InnoDB;
alter table sourceutile add constraint sourceutile_pkey primary key(enginetype);

insert into sourceutile(enginetype, datatype, dataformat) values('flink', '{"jsonlist": ["STRING", "BOOLEAN", "BYTES", "DECIMAL", "TINYINT", "SMALLINT", "INTEGER", "BIGINT", "FLOAT", "DOUBLE", "DATE", "TIME", "TIMESTAMP", "TIMESTAMP WITH LOCAL TIME ZONE", "INTERVAL", "ARRAY", "MULTISET", "MAP", "ROW", "RAW"]}', '{"jsonlist": ["csv", "json", "avro", "debezium-json", "canal-json", "parquet", "orc"]}');
update sourceutile set sourcekind = '{"kinds": [{"name":"MySQL"}, {"name": "PostgreSQL"}, {"name":"S3"}, {"name":"ClickHouse"} ,{"name":"Hbase"} ,{"name":"Kafka"} ,{"name":"Ftp"}, {"name":"HDFS"}]}' where enginetype='flink';


-- sourceManagerTable
create table sourcemanager(
	sourceid varchar(24),
	spaceid varchar(24) not null,
	sourcetype varchar(16),
	name varchar(64) not null,
	comment varchar(256),
	url varchar(8000),
	createtime BIGINT(20) UNSIGNED NOT NULL,
	updatetime BIGINT(20) UNSIGNED NOT NULL,
	state varchar(16)
) ENGINE=InnoDB;
alter table sourcemanager add constraint sourcemanager_pkey primary key(sourceid);
create unique index sourcemanager_unique ON  sourcemanager (spaceid, name);
alter table sourcemanager add CONSTRAINT sourcemanager_chk_type check(sourcetype = 'MySQL' or sourcetype = 'PostgreSQL' or sourcetype = 'Kafka' or sourcetype = 'S3' or sourcetype = 'ClickHouse' or sourcetype = 'Hbase' or sourcetype = 'Ftp' or sourcetype = 'HDFS');
alter table sourcemanager add constraint sourcemanager_chk_state check (state = 'enable' or state = 'disable');

-- tableManagerTable
create table sourcetables(
	tableid varchar(24),
	sourceid varchar(24) not null,
	spaceid varchar(24) not null,
	name varchar(64) not null,
	comment varchar(256),
	url varchar(8000),
	createtime BIGINT(20) UNSIGNED NOT NULL,
	updatetime BIGINT(20) UNSIGNED NOT NULL,
	direction varchar(16),
	foreign key(sourceid) references sourcemanager(sourceid)
) ENGINE=InnoDB;
alter table sourcetables add constraint sourcetables_pkey primary key(tableid);
create unique index sourcetables_unique ON  sourcetables (spaceid, name);
