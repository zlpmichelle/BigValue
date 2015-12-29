solrctl instancedir --generate /root/solr/nrt-csv-collection
solrctl instancedir --create nrt-csv-collection /root/solr/nrt-csv-collection
solrctl collection --create nrt-csv-collection -s 3