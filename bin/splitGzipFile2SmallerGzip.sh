#!/bin/sh

currentpath=$(cd "$(dirname "$0")";pwd)
. $currentpath/function.sh


# for debug with target dir
#cp $currentpath/../../BigValue-1.0.jar $currentpath/../
#rm -rf $currentpath/../BigValue-1.0-obfuscator.jar
#CLASSPATH=.:$currentpath/../target/BigValue-1.0.jar

# for debug with source codes
CLASSPATH=.:$currentpath/../target/BigValue-1.0.jar

#CLASSPATH=.:$currentpath/../BigValue-1.0-obfuscator.jar
jars=`ls $currentpath/../lib`
for jar in $jars
do
    CLASSPATH="$CLASSPATH:$currentpath/../lib/$jar"
done

CLASSPATH=$currentpath/../conf:$CLASSPATH
echo $CLASSPATH

start=$(date +%s.%N)
# run gzip split
java -cp $CLASSPATH com.cloudera.bigdata.analysis.dataload.util.GZFileUtils $@ 
end=$(date +%s.%N)
echo "----------- time of spliting gzip file---------"
getTiming $start $end


