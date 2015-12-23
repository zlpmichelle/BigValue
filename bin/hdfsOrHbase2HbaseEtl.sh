#!/bin/sh

currentpath=$(cd "$(dirname "$0")";pwd)
$currentpath/function.sh

if $currentpath/checklib.sh ; then
    echo "Check lib [PASS]"
else
    continue_ask
fi

# for debug with target dir
#cp $currentpath/../../BigDataAnalysis-1.0.jar $currentpath/../
#rm -rf $currentpath/../BigDataAnalysis-1.0-obfuscator.jar
#CLASSPATH=.:$currentpath/../target/BigDataAnalysis-1.0.jar

# for debug with source codes
#CLASSPATH=.:$currentpath/../target/BigDataAnalysis-1.0.jar

CLASSPATH=.:$currentpath/../BigDataAnalysis-1.0-obfuscator.jar
jars=`ls $currentpath/../lib`
for jar in $jars
do
    CLASSPATH="$CLASSPATH:$currentpath/../lib/$jar"
done

CLASSPATH=$currentpath/../conf:$CLASSPATH
echo $CLASSPATH

hadoop fs -rmr /user/cluster.jar
hadoop fs -put ./lib/cluster.jar /user

java -cp $CLASSPATH com.cloudera.bigdata.analysis.dataload.etl.GeneralETLTool GeneralETLTool $@

