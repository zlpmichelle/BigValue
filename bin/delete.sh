#!/bin/sh

currentpath=$(cd "$(dirname "$0")";pwd)
PROJECT_HOME=$currentpath/../
CLASSPATH=$PROJECT_HOME/example2.jar
CONF_FILE=$currentpath/delete.properties

jars=`ls $PROJECT_HOME/lib`
for jar in $jars
do
    CLASSPATH="$CLASSPATH:$PROJECT_HOME/lib/$jar"
done

CLASSPATH=/usr/lib/hadoop/conf:/usr/lib/hbase/conf:$CLASSPATH

echo $CLASSPATH

java -cp $CLASSPATH  com.cloudera.delete.BatchClient $CONF_FILE


