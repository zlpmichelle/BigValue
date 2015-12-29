#!/usr/bin/env bash
solrctl collection --deletedocs example32gb-hdfs-collection
solrctl collection --delete example32gb-hdfs-collection
solrctl instancedir --delete example32gb-hdfs-collection
