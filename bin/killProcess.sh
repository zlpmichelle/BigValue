#!/bin/sh

currentpath=$(cd "$(dirname "$0")";pwd)
dir=kafka

for i in `ps aux |grep $dir | awk '{print $2}'`
do
	kill -9 $i
	echo kill -9 $i
done
