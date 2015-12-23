# !!!!! Notice: Each properties usage please refer to $BIGDATAANALYSIS_HOME/docs/bulkload/PROPERTIES_USAGE_BL.txt

# specify info of CDH
cdh.version=5.0
cdh.hbase.master.ip.address=ip-172-31-12-149.us-west-2.compute.internal

# 1. Source Definition, all are required
hdfs.source.file.input.path=hdfs://ip-172-31-12-149.us-west-2.compute.internal:8020/user/test/bltest

#gd_unicom_5red_new
hdfs.source.file.encoding=gb2312
hdfs.source.file.record.fields.delimiter=|
hdfs.source.file.record.fields.number=30
hdfs.source.file.record.fields.type.int=0

# 2 .Target HBase Definition
# 1) target hbase table, all are required
hbase.generated.hfiles.output.path=hdfs://ip-172-31-12-149.us-west-2.compute.internal:8020/user/test/bltest_hfile
hbase.target.table.name=test
hbase.target.write.to.wal.flag=false
# 2) ETL for hbase rowkey, column families and column, all are required, if "isExtendedHbaseRowConverter" is false
hbase.target.table.cell.spec=rowkey,f.q1,f.q2,f.q3,f.q4,f.q5
rowkey=concat(trim(f30),trim(f5))
f.q1=trim(f30)
f.q2=trim(f5)
f.q3=trim(f7)
f.q4=trim(f15)
f.q5=concat(trim(f2),trim(f11),trim(f17),trim(f19),trim(f27))

# 3. BulkLoad Stage Definition, all are optional
# 3.1 build index or not
buildIndex=false
regionQuantity=30
indexConfFileName=test_index-conf.xml
hbaseCoprocessorLocation=hdfs://ip-172-31-12-149.us-west-2.compute.internal:8020/user/asb/IndexCoprocessor-1.0.jar

# 3.2 only if not build index(buildIndex=false), following 3.2 properties only set when buildIndex=false in 3.1
onlyGenerateSplitKeySpec=false
preCreateRegions=false
rowkeyPrefix=concat(f30,'|',f5)
recordsNumPerRegion=1
#hbase.target.table.split.key.spec=0278,0556,0834,1112,1390,1668,1946,2224,2502,2780,3058,3336,3614,3892,4170,4448,4726,5004,5282,5560,5838,6116,6394,6672,6950,7228,7506,7784,8061,8338,8615,8892,9169,9446,9723

# 3.3 common bulkload process
nativeTaskEnabled=true
inputSplitSize=
extendedHbaseRowConverterClass=
importDate=
validatorClass=com.cloudera.bigdata.analysis.dataload.exception.DefaultRecordValidator
createMalformedTable=false
