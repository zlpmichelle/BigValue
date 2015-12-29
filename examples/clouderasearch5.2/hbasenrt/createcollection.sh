#!/usr/bin/env bash
solrctl instancedir --generate /root/solr/example32gb-collection
solrctl instancedir --create example32gb-collection /root/solr/example32gb-collection
solrctl collection --create example32gb-collection -s 3

hbase-indexer add-indexer --name myIndexer \
--indexer-conf /root/solr/morphline-hbase-mapper.xml \
--connection-param solr.zk=$ZKHOST/solr \
--connection-param solr.collection=hbase-collection1 \
--zookeeper $ZKHOST:2181


hbase-indexer list-indexers