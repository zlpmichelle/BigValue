#!/bin/sh
CLASSPATH=".:../../../target/BigValue-1.0.jar:../../../lib/*"

java -cp $CLASSPATH com.cloudera.bigdata.analysis.datagen.GeneratorDriver --instanceDoc=gd_cuc.xml --totalSize=100 --minSize=100 --maxSize=200 --outputDir=hdfs://192.168.0.110:8020/gd_unicom --parallel=72 --replicaNum=1 --mode=mapred

