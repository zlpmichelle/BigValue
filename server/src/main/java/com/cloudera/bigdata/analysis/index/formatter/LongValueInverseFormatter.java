package com.cloudera.bigdata.analysis.index.formatter;

import java.io.DataInput;
import java.io.DataOutput;
import java.io.IOException;

import org.apache.hadoop.hbase.util.Bytes;
import org.apache.hadoop.hdfs.util.ByteArray;

import com.cloudera.bigdata.analysis.index.Index;

/**
 * If a long value is required to store with inverse value, i.e. Time, This
 * formatter will use a max long number minus this value.
 */
public class LongValueInverseFormatter implements IndexFieldValueFormatter {

  /*-------------------------------------------   Major Logic Methods   --------------------------------------------*/

  @Override
  public byte[] format(Index.Field field, byte[] value) {
    return Bytes.toBytes(Long.MAX_VALUE - Bytes.toLong(value));
  }

  @Override
  public byte[] deformat(Index.Field field, byte[] value) {
    return Bytes.toBytes(Long.MAX_VALUE - Bytes.toLong(value));
  }

  @Override
  public ByteArray format(Index.Field field, ByteArray value) {
    return new ByteArray(format(field, value.getBytes()));
  }

  @Override
  public ByteArray deformat(Index.Field field, ByteArray value) {
    return new ByteArray(deformat(field, value.getBytes()));
  }

  /*--------------------------------------------     Common Methods    ---------------------------------------------*/

  @Override
  public void write(DataOutput dataOutput) throws IOException {

  }

  @Override
  public void readFields(DataInput dataInput) throws IOException {

  }
}
