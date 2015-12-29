# define mode
dataload.mode=mapred
dataload.source.streaming.fetch=false
buildIndex=false
# if there is one and only one larger file(larger than HDFS block size), set dataload.only.a.large.file to true. By default it is false.
dataload.only.a.large.file=true

# define data source
dataload.source.dataSourceClass=com.cloudera.bigdata.analysis.dataload.source.HDFSDataSource
#dataload.source.hdfsDirs=hdfs://ip-172-31-12-149.us-west-2.compute.internal:8020/user/nci2gb/bltest
dataload.source.hdfsDirs=hdfs://ip-172-31-12-149.us-west-2.compute.internal:8020/user/test/bltest

# define data format
dataload.source.parserType=csv
#dataload.source.instanceDocPath=/user/hbase/conf/nci2gb_hdfs2hbase.xml
dataload.source.instanceDocPath=/user/hbase/conf/test_hdfs2hbase.xml
#dataload.source.instanceDocPath=/opt/zlp/BigValue/conf/test_hdfs2hbase.xml

# define client for mapper
dataload.client.queueLength=3
dataload.client.fetchParallel=8
dataload.client.threadsPerMapper=4

# define target table
#hbase.target.table.name=nci2gb
hbase.target.table.name=test
indexConfFileName=nci2gb_index-conf.xml
server.search.enableStatistics=false
hbaseCoprocessorLocation=hdfs://gzp1:8020/user/asb/IndexCoprocessor-1.0.jar
hbase.table.useInMemory=

#hbase.table.splitKeyPrefixes=1329999
hbase.table.splitSize=2
hbase.table.writeToWAL=false
hbase.table.writeBufferSize=6
hbase.table.autoFlush=false
hbase.table.columnReplication=1
hbase.table.memStore=64
