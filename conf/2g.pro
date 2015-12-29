# !!!!! Notice: Each properties usage please refer to $BIGVALUE_HOME/docs/bulkload/PROPERTIES_USAGE_BL.txt

# specify info of CDH
cdh.version=5.0
cdh.hbase.master.ip.address=ip-172-31-12-149.us-west-2.compute.internal

# 1. Source Definition, all are required
hdfs.source.file.input.path=hdfs://ip-172-31-12-149.us-west-2.compute.internal:8020/user/example32gb/bltest

#example2_5red_new
hdfs.source.file.encoding=gb2312
hdfs.source.file.record.fields.delimiter=|
hdfs.source.file.record.fields.number=9
hdfs.source.file.record.fields.type.int=0

# 2 .Target HBase Definition
# 1) target hbase table, all are required
hbase.generated.hfiles.output.path=hdfs://ip-172-31-12-149.us-west-2.compute.internal:8020/user/example32gb/bltest_hfile
hbase.target.table.name=example32gb
hbase.target.write.to.wal.flag=false
# 2) ETL for hbase rowkey, column families and column, all are required, if "isExtendedHbaseRowConverter" is false
hbase.target.table.cell.spec=rowkey,f.q1,f.q2,f.q3,f.q4,f.q5
rowkey=concat(trim(f1),trim(f2))
f.q1=trim(f3)
f.q2=trim(f4)
f.q3=trim(f5)
f.q4=trim(f6)
f.q5=concat(trim(f7),trim(f8),trim(f9))

# 3. BulkLoad Stage Definition, all are optional
# 3.1 build index or not
buildIndex=false
regionQuantity=30
indexConfFileName=test_index-conf.xml
hbaseCoprocessorLocation=hdfs://ip-172-31-12-149.us-west-2.compute.internal:8020/user/example1/IndexCoprocessor-1.0.jar

# 3.2 only if not build index(buildIndex=false), following 3.2 properties only set when buildIndex=false in 3.1
onlyGenerateSplitKeySpec=false
preCreateRegions=false
rowkeyPrefix=concat(f1,'|',f2)
recordsNumPerRegion=
#hbase.target.table.split.key.spec=115428575|Customer#115428575,22857146|Customer#022857146,38285719|Customer#038285719,53714290|Customer#053714290,69142863|Customer#069142863,84571435|Customer#084571435

# 3.3 common bulkload process
nativeTaskEnabled=true
inputSplitSize=
extendedHbaseRowConverterClass=
importDate=
validatorClass=com.cloudera.bigdata.analysis.dataload.exception.DefaultRecordValidator
createMalformedTable=false
