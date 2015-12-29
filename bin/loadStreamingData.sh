#!/bin/sh

currentpath=$(cd "$(dirname "$0")";pwd)
. $currentpath/function.sh

properties=$currentpath/../properties/example1-hdfs2hbase-sample.properties
nodelist=(192.168.0.121 192.168.0.122 192.168.0.123)
EXAMPLE1home=/root/zhangliping/EXAMPLE1

start=$(date +%s.%N)
# run streaming load
for i in "${nodelist[@]}"
do
	echo start streaming load on $i ...
	ssh $i "nohup \"$currentpath\"/inMemoryAndHdfs2HBase.sh \"$properties\" >\"$EXAMPLE1home\"/log/streamingdataload.log 2>&1 &"
done

end=$(date +%s.%N)
echo "----------- time of loading data---------"
getTiming $start $end
