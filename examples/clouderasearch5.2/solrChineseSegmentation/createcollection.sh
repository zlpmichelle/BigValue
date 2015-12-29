solrctl instancedir --create nci2gb-ch-collection /root/solr/nci2gb-ch-collection
solrctl collection --create nci2gb-ch-collection -s 3

nohup hadoop --config /etc/hadoop/conf.cloudera.yarn \
jar /opt/cloudera/parcels/CDH/lib/hbase-solr/tools/hbase-indexer-mr-1.5-cdh5.2.0-job.jar \
--conf /etc/hbase/conf.cloudera.hbase/hbase-site.xml \
-D 'mapred.child.java.opts=-Xmx500m' \
--hbase-indexer-file /root/solr/morphline-hbase-mapper.xml \
--zk-host $ZKHOME/solr --collection nci2gb-ch-collection \
--go-live --log4j /etc/hbase/conf.cloudera.hbase/log4j.properties --reducers 6
