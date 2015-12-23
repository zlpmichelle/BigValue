#!/bin/sh

currentpath=$(cd "$(dirname "$0")";pwd)
. $currentpath/function.sh

# for debug with source codes
CLASSPATH=.:$currentpath/../target/BigDataAnalysis-1.0.jar

#CLASSPATH=.:$currentpath/../BigDataAnalysis-1.0-obfuscator.jar
jars=`ls $currentpath/../lib`
for jar in $jars
do
    CLASSPATH="$CLASSPATH:$currentpath/../lib/$jar"
done

# for CDH 5.0
CLASSPATH=$CLASSPATH:$currentpath/../lib/cdh5.0/*

CLASSPATH=/etc/hadoop/conf:/etc/hbase/conf:$CLASSPATH

CLASSPATH=$currentpath/../conf:$CLASSPATH
echo $CLASSPATH

# run coprocessor query
java -Djava.library.path=/opt/cloudera/parcels/CDH/lib/hadoop/lib/native -cp $CLASSPATH com.cloudera.bigdata.analysis.index.QueryRecord $@