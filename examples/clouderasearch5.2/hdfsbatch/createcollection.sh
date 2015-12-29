#!/usr/bin/env bash
rm -rf nohup.out
solrctl instancedir --create example32gb-hdfs-collection /root/solr/example32gb-hdfs-collection
solrctl collection --create example32gb-hdfs-collection -s 3


nohup hadoop --config /etc/hadoop/conf.cloudera.yarn jar \
/opt/cloudera/parcels/CDH/lib/solr/contrib/mr/search-mr-1.0.0-cdh5.2.0-job.jar org.apache.solr.hadoop.MapReduceIndexerTool \
-D 'mapred.child.java.opts=-Xmx4096m' \
--log4j /opt/cloudera/parcels/CDH/share/doc/search-1.0.0+cdh5.2.0+0/examples/solr-nrt/log4j.properties \
--morphline-file /root/solr/example32gb-hdfs-collection/conf/morphline.conf \
--output-dir hdfs://172.31.12.149:8020/user/root/outdir \
--verbose --go-live --zk-host $ZKHOST:2181/solr \
--collection example32gb-hdfs-collection hdfs://172.31.12.149:8020/user/example32gb/bltest --reducers 10
