#!/bin/sh

PROJECT_HOME=../
CLASSPATH=$PROJECT_HOME/bulkload.jar
CONF_FILE=./dataexport.properties


jars=`ls $PROJECT_HOME/lib`
for jar in $jars
do
    CLASSPATH="$CLASSPATH:$PROJECT_HOME/lib/$jar"
done

CLASSPATH=/usr/lib/hadoop/conf:/usr/lib/hbase/conf:$CLASSPATH


java -cp $CLASSPATH  com.cloudera.dataexport.DataExportClient $CONF_FILE

