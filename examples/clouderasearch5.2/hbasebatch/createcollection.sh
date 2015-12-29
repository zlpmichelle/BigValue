#!/usr/bin/env bash
solrctl instancedir --generate /root/solr/example320gb-collection
solrctl instancedir --create example320gb-collection /root/solr/example320gb-collection
solrctl collection --create example320gb-collection -s 3

hadoop --config /etc/hadoop/conf.cloudera.yarn \
jar /opt/cloudera/parcels/CDH/lib/hbase-solr/tools/hbase-indexer-mr-1.5-cdh5.2.0-job.jar \
--conf /etc/hbase/conf.cloudera.hbase/hbase-site.xml \
-D 'mapred.child.java.opts=-Xmx10240m' \
--hbase-indexer-file /root/solr/morphline-hbase-mapper.xml \
--zk-host $ZKHOME/solr --collection example320gb-collection \
--go-live --log4j /etc/hbase/conf.cloudera.hbase/log4j.properties --reducers 240