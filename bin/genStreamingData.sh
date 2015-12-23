#!/bin/sh

currentpath=$(cd "$(dirname "$0")";pwd)
. $currentpath/function.sh

# generate args as number
arg=$(echo "--instanceDoc=/root/lizeming/workspace/mobile.xml --totalNum=160000000 --mode=local --outputDir=hdfs://vt-nn:8020/ASB_zhangliping --replicaNum=1 --parallel=1 --minNum=6000000 --maxNum=6000000 --neverStop=true")
#arg=--instanceDoc=/root/lizeming/workspace/mobile.xml --totalNum=6000000   --mode=mapred  --outputDir=hdfs://vt-nn:8020/ASB_zhangliping --replicaNum=1 --parallel=1 --minNum=6000000 --maxNum=6000000 --neverStop=true
#arg=--instanceDoc=/root/lizeming/workspace/mobile.xml --totalNum=6000000 --outputDir=hdfs://vt-nn:8020/user/lizeming --parallel=72 --replicaNum=1 --mode=mapred --minNum=6000000 --maxNum=6000000

# generate args as size
#arg=--instanceDoc=/root/lizeming/workspace/mobile.xml --totalSize=2 --outputDir=hdfs://vt-nn:8020/user/lizeming --parallel=72 --replicaNum=1 --mode=mapred --minSize=2000 --maxSize=2000



# for debug with source codes
CLASSPATH=.:$currentpath/../target/BigDataAnalysis-1.0.jar

#CLASSPATH=.:$currentpath/../BigDataAnalysis-1.0-obfuscator.jar
jars=`ls $currentpath/../lib`
for jar in $jars
do
    CLASSPATH="$CLASSPATH:$currentpath/../lib/$jar"
done

CLASSPATH=$currentpath/../conf:$CLASSPATH
echo $CLASSPATH

echo generating data ...
nohup java -cp $CLASSPATH com.cloudera.bigdata.analysis.datagen.GeneratorDriver $arg > $currentpath/../log/streamingdatagen.log 2>&1 &



