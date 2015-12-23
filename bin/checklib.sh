#!/bin/sh

currentpath=$(cd "$(dirname "$0")";pwd)

HADOOP_JAR=`ls $currentpath/../lib | grep hadoop-core- | grep cdh.jar`
HBASE_JAR=`ls $currentpath/../lib | grep hbase- | grep cdh.jar`
ZOOKEEPER_JAR=`ls $currentpath/../lib | grep zookeeper- | grep cdh.jar`

HADOOP_HOME=/usr/lib/hadoop
HBASE_HOME=/usr/lib/hbase
ZOOKEEPER_HOME=/usr/lib/zookeeper

OWN_HADOOP_JAR_MD5SUM=""
OWN_HBASE_JAR_MD5SUM=""
OWN_ZOOKEEPER_JAR_MD5SUM=""

LOCAL_HADOOP_JAR_MD5SUM=""
LOCAL_HBASE_JAR_MD5SUM=""
LOCAL_ZOOKEEPER_JAR_MD5SUM=""

HADOOP_JAR_WARN="WARN: Its HADOOP CORE jar doesn't match with the client!"
HBASE_JAR_WARN="WARN: Its HBASE jar doesn't match with the client!"
ZOOKEEPER_JAR_WARN="WARN: Its ZOOKEEPER jar doesn't match with the client!"

RETVAL=0
if [ "x$HADOOP_JAR" != "x" ]; then
    OWN_HADOOP_JAR_MD5SUM=`md5sum lib/$HADOOP_JAR | awk '{print $1}'`
else
    echo "WARN: No HADOOP CORE jar in your lib!"
    RETVAL=1
fi
if [ "x$HBASE_JAR" != "x" ]; then
    OWN_HBASE_JAR_MD5SUM=`md5sum lib/$HBASE_JAR | awk '{print $1}'`
else
    echo "WARN: No HBASE jar in your lib!"
    RETVAL=1
fi
if [ "x$ZOOKEEPER_JAR" != "x" ]; then
    OWN_ZOOKEEPER_JAR_MD5SUM=`md5sum lib/$ZOOKEEPER_JAR | awk '{print $1}'`
else
    echo "WARN: No ZOOKEEPER jar in your lib!"
    RETVAL=1
fi

if [ "$RETVAL" == "1" ]; then
    exit 1
fi

if [ -f $HADOOP_HOME/$HADOOP_JAR ]; then
    LOCAL_HADOOP_JAR_MD5SUM=`md5sum $HADOOP_HOME/$HADOOP_JAR | awk '{print $1}'`
else
    echo $HADOOP_JAR_WARN
    RETVAL=1
fi
if [ -f $HBASE_HOME/$HBASE_JAR ]; then
    LOCAL_HBASE_JAR_MD5SUM=`md5sum $HBASE_HOME/$HBASE_JAR | awk '{print $1}'`
else
    echo $HBASE_JAR_WARN
    RETVAL=1
fi
if [ -f $ZOOKEEPER_HOME/$ZOOKEEPER_JAR ]; then
    LOCAL_ZOOKEEPER_JAR_MD5SUM=`md5sum $ZOOKEEPER_HOME/$ZOOKEEPER_JAR | awk '{print $1}'`
else
    echo $ZOOKEEPER_JAR_WARN
    RETVAL=1
fi

# If the name of the jar is not matched, exit
if [ "$RETVAL" == "1" ]; then
    exit 1
fi

if [ "$OWN_HADOOP_JAR_MD5SUM" != "$LOCAL_HADOOP_JAR_MD5SUM" ]; then
    echo $HADOOP_JAR_WARN
    RETVAL=1
fi
if [ "$OWN_HBASE_JAR_MD5SUM" != "$LOCAL_HBASE_JAR_MD5SUM" ]; then
    echo $HBASE_JAR_WARN
    RETVAL=1
fi
if [ "$OWN_ZOOKEEPER_JAR_MD5SUM" != "$LOCAL_ZOOKEEPER_JAR_MD5SUM" ]; then
    echo $ZOOKEEPER_JAR_WARN
    RETVAL=1
fi

if [ "$RETVAL" == "1" ]; then
    exit 1
fi
