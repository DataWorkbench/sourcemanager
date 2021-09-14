#!/bin/bash

MySQL="MySQL"
MySQL_image=`base64 -w 10000000 MySQL.png`
MySQL_desc="MySQL 是一款安全、跨平台、高效的，并与 PHP、Java 等主流编程语言紧密结合的数据库系统。"

PostgreSQL="PostgreSQL"
PostgreSQL_image=`base64 -w 10000000 PostgreSQL.png`
PostgreSQL_desc="PostgreSQL是一个功能强大的开源对象关系型数据库系统，他使用和扩展了SQL语言，并结合了许多安全存储和扩展最复杂数据工作负载的功能。"

S3="S3"
S3_image=`base64 -w 10000000 S3.png`
S3_desc="对象存储是面向海量非结构化数据的通用数据存储平台，提供安全可靠、低成本的云端存储服务。"

ClickHouse="ClickHouse"
ClickHouse_image=`base64 -w 10000000 ClickHouse.png`
ClickHouse_desc="ClickHouse是一个面向联机分析处理(OLAP)的开源的面向列式存储的DBMS，简称CK, 与Hadoop, Spark相比，ClickHouse很轻量级,由俄罗斯第一大搜索引擎Yandex于2016年6月发布"

Hbase="Hbase"
Hbase_image=`base64 -w 10000000 Hbase.png`
Hbase_desc="HBase是一个分布式的、面向列的开源数据库"

Kafka="Kafka"
Kafka_image=`base64 -w 10000000 Kafka.png`
Kafka_desc="Kafka是一个分布式消息队列。"

Ftp="Ftp"
Ftp_image=`base64 -w 10000000 Ftp.png`
Ftp_desc="FTP是一种数据传输协议，将电脑上的数据与服务器数据进行交换"

HDFS="HDFS"
HDFS_image=`base64 -w 10000000 HDFS.png`
HDFS_desc="HDFS是一个高度容错性的系统，适合部署在廉价的机器上。HDFS能提供高吞吐量的数据访问，非常适合大规模数据集上的应用。"

sourcetype="{\"kinds\": [{\"name\":\"$MySQL\", \"image\": \"$MySQL_image\", \"desc\": \"$MySQL_desc\"}, {\"name\": \"$PostgreSQL\", \"image\": \"$PostgreSQL_image\", \"desc\": \"$PostgreSQL_desc\"}, {\"name\":\"$S3\", \"image\": \"$S3_image\", \"desc\": \"$S3_desc\"}, {\"name\":\"$ClickHouse\", \"image\": \"$ClickHouse_image\", \"desc\": \"$ClickHouse_desc\"} ,{\"name\":\"$Hbase\",\"image\": \"$Hbase_image\", \"desc\": \"$Hbase_desc\"} ,{\"name\":\"$Kafka\", \"image\": \"$Kafka_image\", \"desc\": \"$Kafka_desc\"} ,{\"name\":\"$Ftp\", \"image\": \"$Ftp_image\", \"desc\": \"$Ftp_desc\"}, {\"name\":\"$HDFS\", \"image\": \"$HDFS_image\", \"desc\": \"$HDFS_desc\"}]}"

echo "update sourceutile set sourcekind = '$sourcetype' where enginetype='flink';" >> V0.0.0__sourcemanager.sql
