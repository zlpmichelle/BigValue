#!/usr/bin/env bash
java -cp .:../../../target/BigValue-1.0.jar:../../../lib/*:../../../lib/lib_2_5_1/* com.cloudera.bigdata.analysis.datagen.GeneratorDriver --instanceDoc=example1.xml --totalNum=6000000   --mode=local  --outputDir=hdfs://vt-nn:8020/EXAMPLE1_lipingzhang --replicaNum=1 --parallel=1 --minNum=6000000 --maxNum=6000000 --neverStop=true