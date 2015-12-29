#!/bin/sh

currentpath=$(cd "$(dirname "$0")";pwd)
jarname=$currentpath/../BigValue-1.0.jar
dirname=example1
indexconf=index-conf

regionserverlist=(192.168.0.121 192.168.0.122 192.168.0.123)
masterlist=(192.168.0.121 192.168.0.122 192.168.0.123)


# restart hbase
# stop habse regionserver 
for i in "${regionserverlist[@]}"
do
	echo stop hbase regionserver $i
	ssh $i "service hbase-regionserver stop"
done

# stop hbase master
for i in "${masterlist[@]}"
do
	echo stop hbase master $i
	ssh $i "service hbase-master stop"
	# curl -X POST -u admin:cdhcluster 'http://192.168.0.121:7180/api/v6/clusters/Cluster%201/services/hbase/commands/stop'
done

# start hbase master
for i in "${masterlist[@]}"
do
	echo start hbase master $i
	ssh $i "service hbase-master start"
	# curl -X POST -u admin:cdhcluster 'http://192.168.0.121:7180/api/v6/clusters/Cluster%201/services/hbase/commands/start'
done


# start habse regionserver 
for i in "${regionserverlist[@]}"
do
	echo start hbase regionserver $i
	ssh $i "service hbase-regionserver start"
done
