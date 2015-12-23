package com.cloudera.bigdata.analysis.index.util;

import java.util.BitSet;

import org.apache.commons.lang.ArrayUtils;
import org.apache.hadoop.hbase.util.Bytes;
import org.apache.hadoop.hdfs.util.ByteArray;

public abstract class ByteArrayUtils {

  public static boolean isEmpty(ByteArray byteArray) {
    if (byteArray == null || ArrayUtils.isEmpty(byteArray.getBytes())) {
      return true;
    }
    return false;
  }

  public static boolean isNotEmpty(ByteArray byteArray) {
    return byteArray != null && ArrayUtils.isNotEmpty(byteArray.getBytes());
  }

  public static int compare(ByteArray leftByteArray, ByteArray rightByteArray) {
    return Bytes.compareTo(leftByteArray.getBytes(), rightByteArray.getBytes());
  }

  /**
   * Increment byte array value by add 1 to the long value of the byte array
   * stands for. NOTE: if we just add 1 to the last byte of array, there's a
   * potential problem: it could be wrong if last byte is 255, but adding 1 to
   * long value of a byte array might have the same problem if the byte array is
   * 111....11, but by contract, the later possibility is very low!
   * 
   * @param byteArray
   */
  public static ByteArray increaseOneByte(ByteArray byteArray) {
    return new ByteArray(increaseOneByte(byteArray.getBytes()));
  }

  /**
   * Increment byte array value by add 1 to the long value of the byte array
   * stands for. NOTE: if we just add 1 to the last byte of array, there's a
   * potential problem: it could be wrong if last byte is 255, but adding 1 to
   * long value of a byte array might have the same problem if the byte array is
   * 111....11, but by contract, the later possibility is very low!
   * 
   * @param byteArray
   */
  public static byte[] increaseOneByte(byte[] byteArray) {
    byte[] value = ArrayUtils.clone(byteArray);
    int index = value.length - 1;
    // Get the rightest none 0xFF byte
    while (index >= 0 && value[index] == -1) {
      index--;
    }
    if (index >= 0) {
      value[index]++;
    }
    return value;
  }

  public static ByteArray decreaseOneByte(ByteArray byteArray) {
    return new ByteArray(decreaseOneByte(byteArray.getBytes()));
  }

  public static byte[] decreaseOneByte(byte[] byteArray) {
    byte[] value = ArrayUtils.clone(byteArray);
    int index = value.length - 1;
    // Get the rightest none 0x00 byte
    while (index >= 0 && value[index] == 0) {
      index--;
    }
    if (index >= 0) {
      value[index]--;
    }
    return value;
  }

  public static ByteArray getMinValueByteArray(int length) {

    BitSet min = new BitSet(8 * length);
    for (int i = 0; i < 8 * length; i++) {
      min.set(i, false);
    }
    return new ByteArray(Bytes.toBytes(min.toString()));
  }
}
