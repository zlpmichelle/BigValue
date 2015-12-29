#!/bin/sh

currentpath=$(cd "$(dirname "$0")";pwd)

CLASSPATH=".:$currentpath/../BigValue-1.0.jar:$currentpath/../lib/*:$currentpath/../lib/lib_2_5_1/*:$currentpath/../conf/*"

#java -cp $CLASSPATH com.cloudera.bigdata.analysis.dataload.index.IndexEntryBuilderGroup $@

java -cp $CLASSPATH com.cloudera.bigdata.analysis.dataload.index.search.SearchStrategyDecider $@
