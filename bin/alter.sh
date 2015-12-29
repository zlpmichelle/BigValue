#!/bin/sh

TABLE=$1
echo "disable '$TABLE'" | hbase shell
#echo "alter '$TABLE',METHOD=>'table_att','coprocessor' => 'hdfs:///user/example2/example2.jar|com.cloudera.delete.BatchEndpoint|1001|'" | hbase shell
#echo "alter '$TABLE',METHOD=>'table_att','coprocessor' => 'hdfs:///user/example2/example2.jar|com.cloudera.dataPrepare.DataPrepareEndpoint|1001|'" | hbase shell
echo "enable '$TABLE'" | hbase shell

