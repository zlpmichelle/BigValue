package com.cloudera.bigdata.analysis.dataload.transform;

import java.util.HashMap;

import org.apache.hadoop.hbase.client.Put;
import org.apache.hadoop.hbase.util.Bytes;

import com.cloudera.bigdata.analysis.dataload.exception.FormatException;
import com.cloudera.bigdata.analysis.dataload.util.CommonUtils;

public class ImprovedExtendedHBaseRowConverter extends
    ExtensibleHBaseRowConverter {
  static {
    ImprovedExtendedHBaseRowConverter.define(
        ImprovedExtendedHBaseRowConverter.class,
        new ImprovedExtendedHBaseRowConverter());
  }

  public static final int FIELDS_LENTH = 117;

  public static final String RECORD_FIELDS_DELIMITER = "|";
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
    // startSplitTime = System.currentTimeMillis();
    HashMap<Integer, byte[]> fields = CommonUtils.split(
        Bytes.toBytes(rawRecord), rawRecord.length(),
        Bytes.toBytes(RECORD_FIELDS_DELIMITER));
    // splitTime = splitTime + System.currentTimeMillis() - startSplitTime;

    // startComposeTime = System.currentTimeMillis();

    if (fields.size() != FIELDS_LENTH) {
      throw new FormatException("Invalid line: actual fields " + fields.size()
          + " expected fields " + FIELDS_LENTH);
    }

    byte[] procodStr = fields.get(0);
    byte[] actnoStr = fields.get(2);
    byte[] prdnoStr = fields.get(3);
    // composeTime = composeTime + System.currentTimeMillis() -
    // startComposeTime;

    // startRowTime = System.currentTimeMillis();

    rowkey = new byte[procodStr.length + 1 + actnoStr.length + 1
        + prdnoStr.length];
    byte delimiter = (byte) HBASE_COLUMN_DELIMITER.charAt(0);

    CommonUtils.mergeByteArray(procodStr, rowkey, 0, delimiter, true);
    CommonUtils.mergeByteArray(actnoStr, rowkey, procodStr.length + 1,
        delimiter, true);
    CommonUtils.mergeByteArray(prdnoStr, rowkey, procodStr.length + 1
        + actnoStr.length + 1, delimiter, false);
    // buildRowTime = buildRowTime + System.currentTimeMillis() - startRowTime;
    // startColumnTime = System.currentTimeMillis();
    trdat = new byte[fields.get(1).length];
    CommonUtils.mergeByteArray(fields.get(1), trdat, 0, delimiter, false);
    jrnno = new byte[fields.get(30).length];
    CommonUtils.mergeByteArray(fields.get(30), jrnno, 0, delimiter, false);

    int length = 0;
    for (int i = 4; i <= FIELDS_LENTH - 88; i++) {
      length += fields.get(i).length + 1;
    }

    for (int i = 31; i <= FIELDS_LENTH - 2; i++) {
      length += fields.get(i).length + 1;
    }

    length += fields.get(FIELDS_LENTH - 1).length;
    misc = new byte[length];

    int offset = 0;
    for (int i = 4; i <= FIELDS_LENTH - 88; i++) {
      CommonUtils.mergeByteArray(fields.get(i), misc, offset, delimiter, true);
      offset += fields.get(i).length + 1;
    }

    for (int i = 31; i <= FIELDS_LENTH - 2; i++) {
      CommonUtils.mergeByteArray(fields.get(i), misc, offset, delimiter, true);
      offset += fields.get(i).length + 1;
    }

    CommonUtils.mergeByteArray(fields.get(FIELDS_LENTH - 1), misc, offset,
        delimiter, false);
    // buildColumnTime = buildColumnTime + System.currentTimeMillis()
    // - startColumnTime;
  }

  @Override
  public Put convertToPut(String line, boolean writeToWAL)
      throws FormatException {
    parseString(line);
    // startPutTime = System.currentTimeMillis();
    Put put = new Put(rowkey, timeStamp);
    put.add(ImprovedExtendedHBaseRowConverter.COLUMN_FAMILY,
        ImprovedExtendedHBaseRowConverter.QUALIFIER_TRDAT, trdat);
    put.add(ImprovedExtendedHBaseRowConverter.COLUMN_FAMILY,
        ImprovedExtendedHBaseRowConverter.QUALIFIER_JRNNO, jrnno);
    put.add(ImprovedExtendedHBaseRowConverter.COLUMN_FAMILY,
        ImprovedExtendedHBaseRowConverter.QUALIFIER_MISC, misc);
    // putTime = putTime + System.currentTimeMillis() - startPutTime;
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
