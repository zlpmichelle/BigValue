package com.cloudera.bigdata.analysis.dataload.transform;

import org.apache.hadoop.hbase.client.Put;
import org.apache.hadoop.hbase.util.Bytes;

import com.cloudera.bigdata.analysis.dataload.exception.FormatException;

public class SampleExtendedHBaseRowConverter extends
    ExtensibleHBaseRowConverter {

  static {
    ExtensibleHBaseRowConverter.define(SampleExtendedHBaseRowConverter.class,
        new SampleExtendedHBaseRowConverter());
  }

  public static final int FIELDS_LENTH = 32;
  public static final String RECORD_FIELDS_DELIMITER = "\\|!";
  public static final String HBASE_COLUMN_DELIMITER = "|";

  public static final String FAMILY_NAME = "f";

  public static final byte[] COLUMN_FAMILY = Bytes.toBytes(FAMILY_NAME);

  public static final byte[] QUALIFIER_TRDAT = Bytes.toBytes("TRDAT");
  public static final byte[] QUALIFIER_JRNNO = Bytes.toBytes("JRNNO");
  public static final byte[] QUALIFIER_MISC = Bytes.toBytes("MISC");

  // public static long splitTime = 0;
  // public static long composeTime = 0;
  // public static long buildRowTime = 0;
  // public static long buildColumnTime = 0;
  //
  // public static long putTime = 0;
  // public static long startSplitTime;
  // public static long startComposeTime;
  // public static long startRowTime;
  // public static long startColumnTime;
  // public static long startPutTime;

  public byte[] rowkey;

  // derivative
  public byte[] trdat;
  public byte[] jrnno;

  // misc fields
  public byte[] misc;

  private void parseString(String rawRecord) throws FormatException {

    // build the rowKey
    String[] fields = rawRecord.split(RECORD_FIELDS_DELIMITER);

    if (fields.length != FIELDS_LENTH) {
      throw new FormatException("Invalid line: actual fields " + fields.length
          + " expected fields " + FIELDS_LENTH);
    }

    String procodStr = fields[0].trim();
    String actnoStr = fields[1].trim();
    String trdatStr = fields[2].trim();
    String jrnnoStr = fields[3].trim();
    String seqnoStr = fields[4].trim();
    String prdnoStr = fields[5].trim();

    StringBuilder rowKeyBuilder = new StringBuilder();
    rowKeyBuilder.append(procodStr).append(HBASE_COLUMN_DELIMITER);
    rowKeyBuilder.append(actnoStr).append(HBASE_COLUMN_DELIMITER);
    rowKeyBuilder.append(prdnoStr).append(HBASE_COLUMN_DELIMITER);
    rowKeyBuilder.append(trdatStr).append(HBASE_COLUMN_DELIMITER);
    rowKeyBuilder.append(jrnnoStr).append(HBASE_COLUMN_DELIMITER);
    rowKeyBuilder.append(seqnoStr);
    rowkey = Bytes.toBytes(rowKeyBuilder.toString());

    // build the query predicate fields
    trdat = Bytes.toBytes(trdatStr);
    jrnno = Bytes.toBytes(jrnnoStr);

    // build the misc fields
    StringBuilder miscBuilder = new StringBuilder();
    for (int i = 6; i <= FIELDS_LENTH - 2; i++) {
      miscBuilder.append(fields[i].trim()).append(HBASE_COLUMN_DELIMITER);
    }

    miscBuilder.append(fields[FIELDS_LENTH - 1].trim());
    misc = Bytes.toBytes(miscBuilder.toString());
  }

  @Override
  public Put convertToPut(String line, boolean writeToWAL)
      throws FormatException {
    parseString(line);
    Put put = new Put(rowkey, timeStamp);
    put.add(SampleExtendedHBaseRowConverter.COLUMN_FAMILY,
        SampleExtendedHBaseRowConverter.QUALIFIER_TRDAT, trdat);
    put.add(SampleExtendedHBaseRowConverter.COLUMN_FAMILY,
        SampleExtendedHBaseRowConverter.QUALIFIER_JRNNO, jrnno);
    put.add(SampleExtendedHBaseRowConverter.COLUMN_FAMILY,
        SampleExtendedHBaseRowConverter.QUALIFIER_MISC, misc);
    return put;
  }

  // @Override
  // public long getSplitTime() {
  // return splitTime;
  // }
  //
  // @Override
  // public long getBuildRowTime() {
  // return buildRowTime;
  // }
  //
  // @Override
  // public long getBuildColumnTime() {
  // return buildColumnTime;
  // }
  //
  // @Override
  // public long getComposeTime() {
  // return composeTime;
  // }
  //
  // @Override
  // public long getPutTime() {
  // return putTime;
  // }
}
