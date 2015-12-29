rm -rf nohup.out
solrctl instancedir --create nci2gb-hdfs-collection /root/solr/nci2gb-hdfs-collection
solrctl collection --create nci2gb-hdfs-collection -s 3 


nohup hadoop --config /etc/hadoop/conf.cloudera.yarn jar \
/opt/cloudera/parcels/CDH/lib/solr/contrib/mr/search-mr-1.0.0-cdh5.2.0-job.jar org.apache.solr.hadoop.MapReduceIndexerTool \
-D 'mapred.child.java.opts=-Xmx4096m' \
--log4j /opt/cloudera/parcels/CDH/share/doc/search-1.0.0+cdh5.2.0+0/examples/solr-nrt/log4j.properties \
--morphline-file /root/solr/nci2gb-hdfs-collection/conf/morphline.conf \
--output-dir hdfs://172.31.12.149:8020/user/root/outdir \
--verbose --go-live --zk-host $ZKHOST:2181/solr \
--collection nci2gb-hdfs-collection hdfs://172.31.12.149:8020/user/nci2gb/bltest --reducers 10
