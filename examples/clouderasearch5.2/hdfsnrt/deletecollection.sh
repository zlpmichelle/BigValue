solrctl collection --delete nrt-csv-collection
solrctl collection --delete nrt-csv-collection
solrctl instancedir --delete nrt-csv-collection
sudo -u hdfs hadoop fs -rm -r /solr/nrt-csv-collection
rm -rf nohup.out
