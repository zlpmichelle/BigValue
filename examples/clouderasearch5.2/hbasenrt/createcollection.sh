solrctl instancedir --generate /root/solr/nci2gb-collection
solrctl instancedir --create nci2gb-collection /root/solr/nci2gb-collection
solrctl collection --create nci2gb-collection -s 3

hbase-indexer add-indexer --name myIndexer \
--indexer-conf /root/solr/morphline-hbase-mapper.xml \
--connection-param solr.zk=$ZKHOST/solr \
--connection-param solr.collection=hbase-collection1 \
--zookeeper $ZKHOST:2181


hbase-indexer list-indexers