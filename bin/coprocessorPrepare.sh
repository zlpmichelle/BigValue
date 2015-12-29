#!/bin/sh

currentpath=$(cd "$(dirname "$0")";pwd)
jarname=$currentpath/../IndexCoprocessor-1.0.jar
dirname=example1

regionserverlist=(192.168.0.121 192.168.0.122 192.168.0.123)
masterlist=(192.168.0.121 192.168.0.122 192.168.0.123)

# build coprocessor jar
mv $currentpath/../build.xml $currentpath/../build.xml.pro
mv $currentpath/../build.xml.index $currentpath/../build.xml
ant jar

# put coprocessor jar to HDFS
hadoop fs -rmr /user/$dirname
hadoop fs -mkdir /user/$dirname
hadoop fs -put $jarname /user/$dirname/

# update client jar into hbase lib
for i in "${regionserverlist[@]}"
do
	echo copy lib $i
	ssh $i "rm -rf /opt/cloudera/parcels/CDH-5.0.0-1.cdh5.0.0.p0.47/lib/hbase/lib/IndexCoprocessor-1.0.jar"
	scp $currentpath/../IndexCoprocessor-1.0.jar root@$i:/opt/cloudera/parcels/CDH-5.0.0-1.cdh5.0.0.p0.47/lib/hbase/lib/
done

# restart hbase
#sh $currentpath/restartHBase.sh

# recover build.xml
mv $currentpath/../build.xml $currentpath/../build.xml.index
mv $currentpath/../build.xml.pro $currentpath/../build.xml
