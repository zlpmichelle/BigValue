package com.cloudera.bigdata.analysis.index.util;

import java.nio.ByteBuffer;
import java.util.Random;
import java.util.UUID;

import org.apache.commons.lang.ArrayUtils;
import org.apache.hadoop.hbase.util.Bytes;

import com.cloudera.bigdata.analysis.index.Constants;

public abstract class RowKeyUtil {

  private static Random random = new Random();

  /**
   * Generate row key with an random prefix and UUID.
   */
  public static byte[] genUUID() {
    UUID uuid = UUID.randomUUID();
    ByteBuffer bb = ByteBuffer.wrap(new byte[16]);
    bb.putLong(uuid.getMostSignificantBits());
    bb.putLong(uuid.getLeastSignificantBits());
    return bb.array();
  }

  public static byte[] genUUIDString() {
    return Bytes.toBytes(UUID.randomUUID().toString());
  }

  // public static byte[] genRowKey() {
  // return Bytes.add(genRandomRowKeyPrefix(), genUUID());
  // }

  public static byte[] genRowKey(byte[] rowKeyPrefix) {
    // return string, not byte array
    // pain text rowkey
    return genRowKey(rowKeyPrefix, genUUIDString());

    // byte array rowkey
    // return genRowKey(rowKeyPrefix, genUUID());
  }

  public static byte[] genRowKey(byte[] rowKeyPrefix, byte[] uuid) {
    return Bytes.add(rowKeyPrefix, Constants.HBASE_TABLE_DELIMITER, uuid);
  }

  /**
   * Format an int value to row key prefix style.
   * 
   * @param intValue
   *          an int value.
   * @return a string with row key prefix style.
   */
  public static String formatToRowKeyPrefix(int intValue) {
    return String.format(Constants.ROWKEY_PREFIX_PATTERN, intValue);
  }

  /**
   * Gets prefix of given row key
   * 
   * @param rowKey
   *          a given row key
   * @return the prefix of given row key
   */
  public static byte[] getRowKeyPrefix(byte[] rowKey) {
    return ArrayUtils.subarray(rowKey, 0, Constants.ROWKEY_PREFIX_LENGTH);
  }

  /**
   * Generate random row key prefix. If prefix length is 4, it could be any one
   * from 0000 to 9999 NOTE: thread-safe is not required for this method.
   * 
   * @return the generated prefix.
   */
  public static String genRandomRowKeyPrefixStr() {
    // for Random.nextInt method, returned value is between [0,given value),
    // so, we should plus 1!
    int randomPrefixIntValue = random
        .nextInt(Constants.ROWKEY_PREFIX_MAX_VALUE);
    return RowKeyUtil.formatToRowKeyPrefix(randomPrefixIntValue);
  }

  /**
   * Generate random row key prefix. If prefix length is 4, it could be any one
   * from 0000 to 9999 NOTE: thread-safe is not required for this method.
   * 
   * @return the generated prefix.
   */
  public static byte[] genRandomRowKeyPrefix() {
    return Bytes.toBytes(genRandomRowKeyPrefixStr());
  }

  /**
   * Check whether the given row key is record's.
   * 
   * @param rowKey
   *          a row key.
   * @return if record, return true, otherwise, false.
   */
  public static boolean isRecord(byte[] rowKey) {
    return rowKey[Constants.ROWKEY_PREFIX_LENGTH] == Constants.B_HBASE_TABLE_DELIMITER;
  }

  /**
   * Check whether the given row key is index's.
   * 
   * @param rowKey
   *          a row key.
   * @return if index, return true, otherwise, false.
   */
  public static boolean isIndex(byte[] rowKey) {
    return rowKey[Constants.ROWKEY_PREFIX_LENGTH] == Constants.B_IDX_ROWKEY_DELIMITER;
  }
}
