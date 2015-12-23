#!/bin/sh

kafkadir=/root/zhangliping/kafka_2.8.0-0.8.0
coprocessorname=com.cloudera.bigdata.analysis.dataload.index.IndexCoprocessor

#clean ganglia log
service gmetad stop
rm -rf /var/lib/ganglia/rrds/*
service gmetad start

# recover ganglia
#mkdir /var/lib/ganglia/rrds
#chown -R nobody:nobody /var/lib/ganglia/rrds


rm -rf /var/log/hadoop/*
rm -rf /var/log/hbase/*
rm -rf /var/log/zookeeper/*

rm -rf $kafkadir/logs/*


#clean coprocessor tmp jar and crc
rm -rf /tmp/..*.$coprocessorname.*.jar.crc
rm -rf /tmp/..-*.$coprocessorname.*.jar.crc
rm -rf /tmp/.-*.$coprocessorname*.jar
rm -rf /tmp/.*.$coprocessorname.*.jar

#rm -rf /tmp/..*.*.*.jar.crc
#rm -rf /tmp/..-*.*.*.jar.crc
#rm -rf /tmp/.-*.*.*.jar
#rm -rf /tmp/.*.*.*.jar                                                                   