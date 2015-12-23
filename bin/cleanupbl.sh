#!/bin/sh

currentPath=$(cd "$(dirname "$0")"; pwd)

# specify the value of following three arguments
hbaseMasterIpaddress=ip-172-31-12-149.us-west-2.compute.internal

hbaseTableName=test
hfilePath=hdfs://ip-172-31-12-149.us-west-2.compute.internal:8020/user/$hbaseTableName/bltest_hfile


hbaseMalformedTableName=exception_log
recordCountDir=$currentPath/../tmpbl*
hiveExtTableName=$hbaseTableName
hiveTotalTableName=$hiveExtTableName\_total

# clean hive tables
hive -e "drop table $hiveExtTableName"
hive -e "drop table $hiveTotalTableName"


# clean rcord count dir
rm -rf $recordCountDir


# clean hfile output in hdfs
ssh $hbaseMasterIpaddress "sudo -u hdfs hadoop fs -rmr $hfilePath"


# clean hbase tables
ssh $hbaseMasterIpaddress "echo disable \'$hbaseTableName\' | hbase shell"
ssh $hbaseMasterIpaddress "echo drop \'$hbaseTableName\' | hbase shell"

ssh $hbaseMasterIpaddress "echo disable \'$hbaseMalformedTableName\' | hbase shell"
ssh $hbaseMasterIpaddress "echo drop \'$hbaseMalformedTableName\' | hbase shell"
