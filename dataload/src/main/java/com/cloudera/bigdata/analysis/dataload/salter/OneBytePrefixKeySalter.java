package com.cloudera.bigdata.analysis.dataload.salter;

import java.util.Arrays;

import org.apache.hadoop.hbase.util.Bytes;

/**
 * This will prepend one byte before the rowkey The prepended byte is the hash
 * value of the original row key.
 *
 */
public class OneBytePrefixKeySalter extends NBytePrefixKeySalter {
  private final static int ONE_BYTE = 1;
  private int slots;

  public OneBytePrefixKeySalter() {
    this(256);
  }

  public OneBytePrefixKeySalter(int limit) {
    super(ONE_BYTE);
    this.slots = limit;
  }

  protected byte[] hash(byte[] key) {
    byte[] result = new byte[ONE_BYTE];
    result[0] = 0x00;
    int hash = 1;
    if (key == null || key.length == 0) {
      return result;
    }
    for (int i = 0; i < key.length; i++) {
      hash = 31 * hash + (int) (key[i]);
    }
    hash = hash & 0x7fffffff;
    result[0] = (byte) (hash % slots);
    return result;
  }

  @Override
  public byte[][] getAllSalts() {

    byte[][] salts = new byte[slots][];
    for (int i = 0; i < salts.length; i++) {
      salts[i] = new byte[] { (byte) i };
    }
    Arrays.sort(salts, Bytes.BYTES_RAWCOMPARATOR);
    return salts;
  }
}
