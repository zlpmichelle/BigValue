package com.cloudera.bigdata.analysis.dataload.mapreduce;

import java.io.IOException;
import java.util.Map;

import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.hbase.client.HTable;
import org.apache.hadoop.hbase.client.Put;
import org.apache.hadoop.hbase.client.Result;
import org.apache.hadoop.hbase.io.ImmutableBytesWritable;
import org.apache.hadoop.hbase.mapreduce.TableMapper;
import org.apache.hadoop.io.LongWritable;
import org.apache.hadoop.io.Text;
import org.apache.hadoop.mapreduce.Counter;
import org.apache.log4j.Logger;

import com.cloudera.bigdata.analysis.dataload.Constants;
import com.cloudera.bigdata.analysis.dataload.exception.ETLException;
import com.cloudera.bigdata.analysis.dataload.transform.HBase2HBaseRowConverter;
import com.cloudera.bigdata.analysis.dataload.transform.ParsedResult;
import com.cloudera.bigdata.analysis.dataload.transform.TargetTableSpec;
import com.cloudera.bigdata.analysis.dataload.util.CommonUtils;

public class HBase2HBaseRowMapper extends TableMapper<Text, LongWritable> {
  private static Logger logger = Logger.getLogger(HBase2HBaseRowMapper.class);
  private Configuration conf = null;
  private HTable table = null;
  private long ts;

  /** Should skip bad lines */
  private boolean skipBadLines;
  private Counter badLineCount;

  private String toBeCleanedCellMapString;
  private Map<String, String> toBeCleanedCellMap;
  private String hbaseTargetTableName;
  private String hbaseTargetTableSplitKeySpec;
  private String hbaseTargetTableCellMapString;
  private boolean hbaseTargetWriteToWAL;

  public HBase2HBaseRowConverter converter;

  public long getTs() {
    return ts;
  }

  public boolean getSkipBadLines() {
    return skipBadLines;
  }

  public Counter getBadLineCount() {
    return badLineCount;
  }

  public void incrementBadLineCount(int count) {
    this.badLineCount.increment(count);
  }

  @Override
  protected void setup(Context context) throws IOException,
      InterruptedException {
    doSetup(context);
  }

  protected void doSetup(Context context) throws IOException,
      InterruptedException {
    super.setup(context);
    conf = context.getConfiguration();
    ts = System.currentTimeMillis();
    badLineCount = context.getCounter("Result2HBaseMapper", "Bad Lines");

    toBeCleanedCellMapString = context.getConfiguration().get(
        "toBeCleanedCellMapString");
    toBeCleanedCellMap = CommonUtils.getMapFromString(toBeCleanedCellMapString,
        Constants.COLUMN_ENTRY_SEPARATOR, Constants.COLUMN_KEY_VALUE_SEPARATOR);
    hbaseTargetTableName = context.getConfiguration().get(
        "hbaseTargetTableName");
    hbaseTargetTableSplitKeySpec = context.getConfiguration().get(
        "hbaseTargetTableSplitKeySpec");
    hbaseTargetTableCellMapString = context.getConfiguration().get(
        "hbaseTargetTableCellMapString");
    hbaseTargetWriteToWAL = context.getConfiguration().getBoolean(
        "hbaseTargetWriteToWAL", false);

    table = new HTable(conf, hbaseTargetTableName);
    try {
      TargetTableSpec tableSpec = new TargetTableSpec(hbaseTargetTableName,
          hbaseTargetTableCellMapString, hbaseTargetTableSplitKeySpec);
      converter = new HBase2HBaseRowConverter(tableSpec);
    } catch (Exception e) {
      e.printStackTrace();
    }
  }

  @Override
  protected void cleanup(Context context) throws IOException,
      InterruptedException {
    super.cleanup(context);
    table.close();
  }

  @Override
  protected void map(ImmutableBytesWritable rowkey, Result result,
      Context context) {

    try {
      ParsedResult parsed = new ParsedResult(result, toBeCleanedCellMap);
      Put put = converter.convert(parsed, hbaseTargetWriteToWAL);
      table.put(put);
    } catch (ETLException ex) {
      logger.error("Bad result at rowkey: " + rowkey.get()
          + " --> Malformat Result: " + result.toString());
      ex.printStackTrace();
      incrementBadLineCount(1);
      return;
    } catch (IOException e) {
      e.printStackTrace();
    }
  }
}
