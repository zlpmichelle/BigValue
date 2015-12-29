#!/usr/bin/env bash
solrctl collection --deletedocs example32gb-collection
solrctl collection --delete example32gb-collection
solrctl instancedir --delete example32gb-collection
hbase-indexer delete-indexer --name myIndexer
