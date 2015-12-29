solrctl collection --delete nci2gb-ch-collection
solrctl collection --delete nci2gb-ch-collection
solrctl instancedir --delete nci2gb-ch-collection
sudo -u hdfs hadoop fs -rm -r /solr/nci2gb-ch-collection
rm -rf nohup.out
