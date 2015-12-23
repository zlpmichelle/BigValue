#!/bin/sh

TABLE=$1
echo "disable '$TABLE'" | hbase shell
#echo "alter '$TABLE',METHOD=>'table_att','coprocessor' => 'hdfs:///user/unicom/unicomgd.jar|com.cloudera.delete.BatchEndpoint|1001|'" | hbase shell
#echo "alter '$TABLE',METHOD=>'table_att','coprocessor' => 'hdfs:///user/unicom/unicomgd.jar|com.cloudera.dataPrepare.DataPrepareEndpoint|1001|'" | hbase shell
echo "enable '$TABLE'" | hbase shell

