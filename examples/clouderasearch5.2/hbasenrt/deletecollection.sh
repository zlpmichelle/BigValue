solrctl collection --deletedocs nci2gb-collection
solrctl collection --delete nci2gb-collection 
solrctl instancedir --delete nci2gb-collection
hbase-indexer delete-indexer --name myIndexer
