#!/bin/sh

currentpath=$(cd "$(dirname "$0")";pwd)
nodelist=(192.168.0.121 192.168.0.122 192.168.0.123)
option=$1

# open debug
for i in "${nodelist[@]}"
do
	echo open debug on $i
	ssh $i "cd /root/zhangliping/ && tar zxf BigValue.tar.gz"
done


# close debug
for i in "${nodelist[@]}"
do
	echo close debug on $i
	ssh $i "cd /root/zhangliping/ && tar zxf BigValue.tar.gz"
done



