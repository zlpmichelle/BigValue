package com.cloudera.bigdata.analysis.dataload.mapreduce;

import java.io.IOException;
import java.text.ParseException;
import java.text.SimpleDateFormat;
import java.util.ArrayList;
import java.util.Date;
import java.util.List;
import java.util.UUID;

import org.apache.hadoop.hbase.client.HTable;
import org.apache.hadoop.hbase.client.Put;
import org.apache.hadoop.hbase.io.ImmutableBytesWritable;
import org.apache.hadoop.hbase.util.Bytes;
import org.apache.hadoop.io.LongWritable;
import org.apache.hadoop.io.Text;
import org.apache.hadoop.mapreduce.Counter;
import org.apache.hadoop.mapreduce.Mapper;
import org.apache.hadoop.util.ReflectionUtils;
import org.apache.log4j.Logger;

import com.cloudera.bigdata.analysis.dataload.exception.FormatException;
import com.cloudera.bigdata.analysis.dataload.exception.RecordValidator;
import com.cloudera.bigdata.analysis.dataload.transform.ExtensibleHBaseRowConverter;
import com.cloudera.bigdata.analysis.dataload.util.Util;

/**
 * Write table content out to files in hdfs.
 */
public class ExtendedHBaseRowMapper extends
    Mapper<LongWritable, Text, ImmutableBytesWritable, Put> {
  private static final Logger LOGGER = Logger
      .getLogger(ExtendedHBaseRowMapper.class);

  private Counter badLineCount;
  private static final int EXCEPTION_LOG_PUT_BATCH_SIZE = 500;
  private RecordValidator validator;
  private List<Put> exceptionLogPutCache;
  // Log the malformed record into a hbase table
  HTable exceptionLogTable;
  public static final SimpleDateFormat df = new SimpleDateFormat(
      "yyyy-MM-dd HH:mm:ss.SSS");
  private boolean createMalformedTable = false;

  // timestamp for all inserted rows
  private long ts = 0;

  // private Counter mapTimeCount;
  // private long startTime;
  public ExtensibleHBaseRowConverter converter;

  public void incrementBadLineCount(int count) {
    this.badLineCount.increment(count);
  }

  // public void incrementMapTimeCount(long time) {
  // this.mapTimeCount.increment(time);
  // }

  /**
   * Handles initializing this class with objects specific to it (i.e., the
   * parser). Common initialization that might be leveraged by a subsclass is
   * done in <code>doSetup</code>. Hence a subclass may choose to override this
   * method and call <code>doSetup</code> as well before handling it's own
   * custom params.
   * 
   * @param context
   */
  @Override
  protected void setup(Context context) {
    doSetup(context);
  }

  /**
   * Handles common parameter initialization that a subclass might want to
   * leverage.
   * 
   * @param context
   */
  protected void doSetup(Context context) {

    // startTime = System.currentTimeMillis();
    badLineCount = context.getCounter("ExtendedHBaseRowMapper", "Bad Lines");

    instantiateValidator(context);

    // mapTimeCount = context.getCounter("ExtendedHBaseRowMapper",
    // "Total Map Time");
    createMalformedTable = context.getConfiguration().getBoolean(
        "createMalformedTable", false);
    String importDateString = context.getConfiguration().get("importDate");

    converter = Util.newExtendedHBaseRowConverter(context.getConfiguration());

    SimpleDateFormat df = new SimpleDateFormat("yyyyMMdd");
    if (importDateString != null && !importDateString.isEmpty()) {
      Date importDate;
      try {
        importDate = df.parse(importDateString);
        ts = importDate.getTime();
      } catch (ParseException e) {
        e.printStackTrace();
      }
    }

    converter.setTimeStamp(ts);

    // if there are exceptional records, commit it to excpetion_log every 500
    // records
    if (createMalformedTable) {
      exceptionLogPutCache = new ArrayList<Put>(EXCEPTION_LOG_PUT_BATCH_SIZE);
      try {
        exceptionLogTable = new HTable(context.getConfiguration(),
            Hdfs2HBaseWorker.EXCEPTION_LOG_TABLE_NAME);
      } catch (IOException ex) {
        ex.printStackTrace();
      }
    }
  }

  // protected void cleanup(Context context) {
  // System.out.println("***********The Map time of Bulk Load is: "
  // + (System.currentTimeMillis() - startTime) + " millisecond!");
  // try {
  // System.out.println("***********The Split time of Bulk Load is: "
  // + converter.getSplitTime() + " millisecond!");
  // System.out.println("***********The Build Compose time of Bulk Load is: "
  // + converter.getComposeTime() + " millisecond!");
  // System.out.println("***********The Build Row time of Bulk Load is: "
  // + converter.getBuildRowTime() + " millisecond!");
  // System.out.println("***********The Build Column time of Bulk Load is: "
  // + converter.getBuildColumnTime() + " millisecond!");
  // System.out.println("***********The Build Put time of Bulk Load is: "
  // + converter.getPutTime() + " millisecond!");
  // } catch (FormatException e) {
  // // TODO Auto-generated catch block
  // e.printStackTrace();
  // }
  // }

  /**
   * Convert a line of TXT text into an HBase table row.
   */
  @Override
  public void map(LongWritable offset, Text value, Context context)
      throws IOException {
    String lineStr = new String(value.getBytes(), 0, value.getLength(), context
        .getConfiguration().get("hdfsSourceTextFileEncoding"));
    try {
      validator.validate(lineStr);
      // Convert to Put
      Put put = converter.convertToPut(lineStr, context.getConfiguration()
          .getBoolean("hbaseTargetWriteToWAL", false));
      ImmutableBytesWritable row = new ImmutableBytesWritable(put.getRow());
      context.write(row, put);
    } catch (FormatException badLineEx) {
      if (createMalformedTable) {
        handleBadLine(context, lineStr, offset, badLineEx);
      } else {
        System.err
            .println("Bad line at offset: " + offset.get() + ":\n"
                + " --> Malformat Line: " + lineStr + "\n"
                + badLineEx.getMessage());
        badLineEx.printStackTrace();
        incrementBadLineCount(1);
        return;
      }
      // incrementMapTimeCount(System.currentTimeMillis() - startTime);
    } catch (Exception e) {
      e.printStackTrace();
    }
  }

  private void instantiateValidator(Context context) {
    String validatorClassName = context
        .getConfiguration()
        .get("validatorClass",
            "com.cloudera.bigdata.analysis.dataload.exception.DefaultRecordValidator");
    try {
      Class<?> validatorClass = Class.forName(validatorClassName);
      LOGGER.info("Instantiating validator: " + validatorClass.getName());
      validator = (RecordValidator) ReflectionUtils.newInstance(validatorClass,
          context.getConfiguration());
    } catch (Exception e) {
      LOGGER.error("Error in setting up bulkload's validator", e);
      throw new RuntimeException(e);
    }
  }

  private void handleBadLine(Context context, String lineStr,
      LongWritable offset, FormatException ex) throws IOException {
    String errorMsg = "Malformat at split: " + context.getInputSplit()
        + " offset at:" + offset.get();
    LOGGER.warn(errorMsg);
    Put put = new Put(Bytes.toBytes(context.getConfiguration().get(
        "hbaseTargetTableName")
        + "|" + df.format(new Date()) + "|" + UUID.randomUUID()));
    put.add(Hdfs2HBaseWorker.EXCEPTION_LOG_TABLE_COLUMN_FAMILY_BYTES,
        Hdfs2HBaseWorker.EXCEPTION_LOG_TABLE_COLUMN_QUALIFIER_RAW_LINE_BYTES,
        Bytes.toBytes(lineStr));
    put.add(Hdfs2HBaseWorker.EXCEPTION_LOG_TABLE_COLUMN_FAMILY_BYTES,
        Hdfs2HBaseWorker.EXCEPTION_LOG_TABLE_COLUMN_QUALIFIER_ERROR_INFO_BYTES,
        Bytes.toBytes(errorMsg + " MSG:" + ex.getMessage()));

    int count = exceptionLogPutCache.size();
    if (count == EXCEPTION_LOG_PUT_BATCH_SIZE) {
      exceptionLogTable.put(exceptionLogPutCache);
      incrementBadLineCount(EXCEPTION_LOG_PUT_BATCH_SIZE);
      exceptionLogPutCache.clear();
    }
    if (count < EXCEPTION_LOG_PUT_BATCH_SIZE) {
      exceptionLogPutCache.add(put);
    }
  }

  @Override
  protected void cleanup(Context context) {
    try {
      if (createMalformedTable) {
        if (!exceptionLogPutCache.isEmpty()) {
          exceptionLogTable.put(exceptionLogPutCache);
          incrementBadLineCount(exceptionLogPutCache.size());
          exceptionLogPutCache.clear();
        }
      }
    } catch (IOException ex) {
      LOGGER.warn(ex);
      throw new RuntimeException(ex);
    }
  }
}
