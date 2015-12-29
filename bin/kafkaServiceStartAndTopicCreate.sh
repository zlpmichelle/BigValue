#!/bin/sh

currentpath=$(cd "$(dirname "$0")";pwd)
. $currentpath/function.sh

# kafka args
nodelist=(192.168.0.121 192.168.0.122 192.168.0.123)
EXAMPLE1home=/root/zhangliping/EXAMPLE1
kafkahome=/root/zhangliping/kafka_2.8.0-0.8.0/
zookeeperlist=gzp1:2181,gzp2:2181,gzp3:2181 
partition=3
topicname=FileMessage

# detele topic 
echo delete old topic ...
#$kafkahome/bin/kafka-run-class.sh kafka.admin.DeleteTopicCommand -topic $topicname -zookeeper $zookeeperlist

# list topic 
echo list topic ...
$kafkahome/bin/kafka-list-topic.sh -zookeeper $zookeeperlist

for i in "${nodelist[@]}"
do
	# start kafka
	echo start kafka service on $i ...
	ssh $i "nohup \"$kafkahome\"/bin/kafka-server-start.sh \"$kafkahome\"/config/server.properties > \"$EXAMPLE1home\"/log/kafkastart.log 2>&1 &"
done

	
# create topic
echo create topic ...
$kafkahome/bin/kafka-create-topic.sh -zookeeper $zookeeperlist -partition 6 -replica 1 -topic $topicname
#$kafkahome/bin/kafka-create-topic.sh -zookeeper $zookeeperlist -partition 6 -replicas 1 -topic $topicname

# list topic
echo list topic ...
$kafkahome/bin/kafka-list-topic.sh -zookeeper $zookeeperlist
