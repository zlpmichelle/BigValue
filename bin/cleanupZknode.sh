#!/bin/sh

currentpath=$(cd "$(dirname "$0")";pwd)

zookeeperlist=(192.168.0.121 192.168.0.122 192.168.0.123)

for i in "${zookeeperlist[@]}"
do
	echo remove /var/zookeeper/version-2/ $i
	ssh $i "rm -rf /var/zookeeper/version-2/*"
done

