package com.cloudera.bigdata.analysis.dataload.mapreduce;

import java.io.IOException;
import java.util.Map;

import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.hbase.client.Result;
import org.apache.hadoop.hbase.client.Scan;
import org.apache.hadoop.hbase.io.ImmutableBytesWritable;
import org.apache.hadoop.hbase.mapreduce.TableMapReduceUtil;
import org.apache.hadoop.mapreduce.Job;
import org.apache.hadoop.mapreduce.lib.output.NullOutputFormat;
import org.apache.log4j.Logger;

import com.cloudera.bigdata.analysis.dataload.Constants;
import com.cloudera.bigdata.analysis.dataload.client.BaseWorker;
import com.cloudera.bigdata.analysis.dataload.client.Worker;
import com.cloudera.bigdata.analysis.dataload.extract.HBaseSourceFieldSpec;
import com.cloudera.bigdata.analysis.dataload.extract.HBaseTargetFieldSpec;
import com.cloudera.bigdata.analysis.dataload.transform.SourceTableSpec;
import com.cloudera.bigdata.analysis.dataload.transform.TargetTableSpec;
import com.cloudera.bigdata.analysis.dataload.util.CommonUtils;

public class HBase2HBaseWorker extends BaseWorker implements Worker {
  private static final Logger LOGGER = Logger
      .getLogger(HBase2HBaseWorker.class);
  final static String NAME = "HBase2HBaseWorker";
  private HBaseSourceFieldSpec hbaseSourceFieldSpec;;
  private HBaseTargetFieldSpec hbaseTargetFieldSpec;

  public HBase2HBaseWorker(String configFile) {
    super(configFile);
    this.hbaseSourceFieldSpec = new HBaseSourceFieldSpec(
        configReader.getConfMap());
    this.hbaseTargetFieldSpec = new HBaseTargetFieldSpec(
        configReader.getConfMap());
  }

  private Job createSubmittableJob(Configuration conf, SourceTableSpec sts)
      throws IOException {
    Job job = new Job(conf, NAME);
    TableMapReduceUtil.addDependencyJars(job);
    job.setJarByClass(HBase2HBaseWorker.class);
    Scan scan = sts.buildScan();
    TableMapReduceUtil.initTableMapperJob(sts.getTableName(), scan,
        HBase2HBaseRowMapper.class, ImmutableBytesWritable.class, Result.class,
        job);
    job.setOutputFormatClass(NullOutputFormat.class);
    job.setNumReduceTasks(0);

    return job;
  }

  @Override
  public void execute() throws Exception {
    doWork();
  }

  @Override
  protected void doWork() throws Exception {
    LOGGER.info("Parse HBase source field spec..." + "\n"
        + hbaseSourceFieldSpec.toString());
    LOGGER.info("Parse HBase target field spec..." + "\n"
        + hbaseTargetFieldSpec.toString());

    String hbaseSourceTableName = hbaseSourceFieldSpec
        .getHbaseSourceTableName();
    String toBeCleanedRowkeyRangeSpec = hbaseSourceFieldSpec
        .getToBeCleanedRowkeyRangeSpec();
    String toBeCleanedCellSpec = hbaseSourceFieldSpec.getToBeCleanedCellSpec();
    SourceTableSpec sts = new SourceTableSpec(hbaseSourceTableName,
        toBeCleanedCellSpec, toBeCleanedRowkeyRangeSpec);

    String hbaseTargetTableName = hbaseTargetFieldSpec
        .getHbaseTargetTableName();
    String hbaseTargetTableSplitKeySpec = hbaseTargetFieldSpec
        .getHbaseTargetTableSplitKeySpec() == null ? "" : hbaseTargetFieldSpec
        .getHbaseTargetTableSplitKeySpec();
    boolean hbaseTargetWriteToWAL = hbaseTargetFieldSpec
        .isHbaseTargetWriteToWAL();
    Map<String, String> targetTableCellMap = hbaseTargetFieldSpec
        .getTargetTableCellMap(), toBeCleanedCellMap = hbaseSourceFieldSpec
        .getToBeCleanedCellMap();
    TargetTableSpec tts = new TargetTableSpec(hbaseTargetTableName,
        CommonUtils.getStringFromMap(targetTableCellMap,
            Constants.COLUMN_ENTRY_SEPARATOR,
            Constants.COLUMN_KEY_VALUE_SEPARATOR), hbaseTargetTableSplitKeySpec);
    if (!CommonUtils.doesTableExist(hbaseAdmin, hbaseTargetTableName)) {
      CommonUtils.createTable(conf, hbaseAdmin, tts);
    }

    conf.set("hbaseTargetTableName", hbaseTargetTableName);
    conf.set("hbaseTargetTableSplitKeySpec", hbaseTargetTableSplitKeySpec);
    conf.set("hbaseTargetTableCellMapString", CommonUtils.getStringFromMap(
        targetTableCellMap, Constants.COLUMN_ENTRY_SEPARATOR,
        Constants.COLUMN_KEY_VALUE_SEPARATOR));
    conf.setBoolean("hbaseTargetWriteToWAL", hbaseTargetWriteToWAL);

    conf.set("toBeCleanedCellMapString", CommonUtils.getStringFromMap(
        toBeCleanedCellMap, Constants.COLUMN_ENTRY_SEPARATOR,
        Constants.COLUMN_KEY_VALUE_SEPARATOR));

    Job job = createSubmittableJob(conf, sts);
    System.exit(job.waitForCompletion(true) ? 0 : 1);
  }
}
