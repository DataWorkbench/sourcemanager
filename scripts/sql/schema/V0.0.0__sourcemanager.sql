
create table if not exists data_source (
    -- Workspace id it belongs to
    `space_id` CHAR(20) NOT NULL,

    -- DataSource ID
    `id` CHAR(20) NOT NULL,

    -- unique in a workspace.
    `name` varchar(64) NOT NULL,

    -- DataSource description.
    `desc` varchar(256),

    -- Type, 1->MySQL 2->PostgreSQL 3->Kafka 4->S3 5->ClickHouse 6->Hbase 7->Ftp 8->HDFS
    `type` TINYINT(1) UNSIGNED DEFAULT 0 NOT NULL,

    -- URL of data source settings..
    `url` JSON,

    -- Status, 1 => "Delete", 2 => "enabled", 3 => "disabled"
    `status` TINYINT(1) UNSIGNED DEFAULT 0 NOT NULL,

    -- User ID of created this data source.
    `created_by` VARCHAR(65) NOT NULL,

    -- Timestamp of create time
    `created` BIGINT(20) UNSIGNED NOT NULL,

    -- Timestamp of update time, Update when some changed, default value should be same as "created"
    `updated` BIGINT(20) UNSIGNED NOT NULL,

    PRIMARY KEY (`id`),
    INDEX mul_list_record_by_space_id(`space_id`)
) ENGINE=InnoDB COMMENT='The data source info';

create table if not exists data_source_connection (
    -- Only used to query sort by
    `id` BIGINT(20) NOT NULL AUTO_INCREMENT,

    -- Workspace id it belongs to
    `space_id` CHAR(20) NOT NULL,

    -- DataSource ID
    `source_id` CHAR(20) NOT NULL,

    -- Network ID
    `network_id` CHAR(20) NOT NULL,

    -- Status, 1 => "Delete", 2 => "Active"
    `status` TINYINT(1) UNSIGNED DEFAULT 0 NOT NULL,

    -- result, 1-=> success 2 => failed
    `result` TINYINT(1) UNSIGNED DEFAULT 0 NOT NULL,

    -- Message is the reason when connection failure.
    `message` VARCHAR(1024) NOT NULL,

    -- Use time. unit in ms.
    `elapse` INT,

    -- Timestamp of create time
    `created` BIGINT(20) UNSIGNED NOT NULL,

    PRIMARY KEY (`id`),
    INDEX mul_list_record_by_space_id(`space_id`)

) ENGINE=InnoDB COMMENT='The connection info for datasource';

create table source_utile(
	engine_type varchar(16),
	data_type varchar(1024),
	data_format varchar(256),
	source_kind longtext
) ENGINE=InnoDB;
alter table source_utile add constraint source_utile_pkey primary key(engine_type);

insert into source_utile(engine_type, data_type, data_format) values('flink', '{"json_list": ["STRING", "BOOLEAN", "BYTES", "DECIMAL", "TINYINT", "SMALLINT", "INTEGER", "BIGINT", "FLOAT", "DOUBLE", "DATE", "TIME", "TIMESTAMP", "TIMESTAMP WITH LOCAL TIME ZONE", "INTERVAL", "ARRAY", "MULTISET", "MAP", "ROW", "RAW"]}', '{"json_list": ["csv", "json", "avro", "debezium-json", "canal-json", "parquet", "orc"]}');
# update source_utile set source_kind = '{"kinds": [{"name":"MySQL"}, {"name": "PostgreSQL"}, {"name":"S3"}, {"name":"ClickHouse"} ,{"name":"Hbase"} ,{"name":"Kafka"} ,{"name":"Ftp"}, {"name":"HDFS"}]}' where engine_type='flink';


# -- sourceManagerTable
# create table source_manager(
# 	source_id varchar(24),
# 	space_id varchar(24) not null,
# 	source_type int,
# 	name varchar(64) not null,
# 	comment varchar(256),
# 	url varchar(8000),
# 	created BIGINT(20) UNSIGNED NOT NULL,
# 	updated BIGINT(20) UNSIGNED NOT NULL,
# 	status int,
# 	create_by varchar(64),
# 	connection int
# ) ENGINE=InnoDB;
# alter table source_manager add constraint source_manager_pkey primary key(source_id);
# create index sourcemanager_unique ON source_manager (space_id, name, status);
# alter table source_manager add CONSTRAINT source_manager_type check(source_type >= 1 and source_type <= 8);
# alter table source_manager add constraint source_manager_status check (status >= 1 and status <= 3);
# alter table source_manager add constraint source_manager_connection check (connection >= 1 and connection <= 3);

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
	table_kind int
# 	foreign key(source_id) references source_manager(source_id)
) ENGINE=InnoDB;
alter table source_tables add constraint sourcetables_pkey primary key(table_id);
create index source_tables_unique ON source_tables (space_id, name, status);
