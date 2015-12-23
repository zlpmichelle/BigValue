#!/bin/sh

currentpath=$(cd "$(dirname "$0")";pwd)
. $currentpath/function.sh

properties=$currentpath/../properties/asb-hdfs2hbase-sample.properties
nodelist=(192.168.0.121 192.168.0.122 192.168.0.123)
ASBhome=/root/zhangliping/ASB

start=$(date +%s.%N)
# run streaming load
for i in "${nodelist[@]}"
do
	echo start streaming load on $i ...
	ssh $i "nohup \"$currentpath\"/inMemoryAndHdfs2HBase.sh \"$properties\" >\"$ASBhome\"/log/streamingdataload.log 2>&1 &"
done

end=$(date +%s.%N)
echo "----------- time of loading data---------"
getTiming $start $end
