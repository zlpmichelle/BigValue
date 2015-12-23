#!/bin/sh

currentpath=$(cd "$(dirname "$0")";pwd)


properties=$currentpath/../properties/asb-hdfs2hbase-sample.properties


$currentpath/inMemoryAndHdfs2HBase.sh $properties
end=$(date +%s.%N)

