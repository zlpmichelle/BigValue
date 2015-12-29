#!/usr/bin/env bash
solrctl collection --delete example32gb-ch-collection
solrctl collection --delete example32gb-ch-collection
solrctl instancedir --delete example32gb-ch-collection
sudo -u hdfs hadoop fs -rm -r /solr/example32gb-ch-collection
rm -rf nohup.out
