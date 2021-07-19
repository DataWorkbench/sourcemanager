-- DATABASE
CREATE DATABASE IF NOT EXISTS data_workbench;
USE data_workbench;

create table enginemapsource(
	enginetype varchar(16),
	sourcetype varchar(512)
) ENGINE=InnoDB;

insert into enginemapsource values('flink', 'MySQL,PostgreSQL,Kafka,S3,ClickHouse,Hbase');
alter table enginemapsource add constraint enginemapsource_pkey primary key(enginetype);

-- sourceManagerTable
create table sourcemanager(
	id varchar(24),
	spaceid varchar(24) not null,
	sourcetype varchar(16),
	name varchar(64) not null,
	comment varchar(256),
	creator varchar(16),
	url varchar(8000),
	createtime timestamp default now(),
	updatetime timestamp,
	enginetype varchar(16),
	direction varchar(16)
) ENGINE=InnoDB;
alter table sourcemanager add constraint sourcemanager_pkey primary key(id);
create unique index sourcemanager_unique ON  sourcemanager (spaceid, name);
alter table sourcemanager add CONSTRAINT sourcemanager_chk_type check(sourcetype = 'MySQL' or sourcetype = 'PostgreSQL' or sourcetype = 'Kafka' or sourcetype = 'S3' or sourcetype = 'ClickHouse' or sourcetype = 'Hbase');
alter table sourcemanager add constraint sourcemanager_chk_crt check (creator = 'workbench' or creator = 'custom');

-- tableManagerTable
create table sourcetables(
	id varchar(24),
	sourceid varchar(24) not null,
	name varchar(64) not null,
	comment varchar(256),
	url varchar(8000),
	createtime timestamp default now(),
	updatetime timestamp,
	foreign key(sourceid) references sourcemanager(id)
) ENGINE=InnoDB;
alter table sourcetables add constraint sourcetables_pkey primary key(id);
create unique index sourcetables_unique ON  sourcetables (sourceid, name);
