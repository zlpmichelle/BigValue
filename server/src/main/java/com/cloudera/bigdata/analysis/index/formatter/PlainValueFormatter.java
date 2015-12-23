package com.cloudera.bigdata.analysis.index.formatter;

import java.io.DataInput;
import java.io.DataOutput;
import java.io.IOException;

import org.apache.hadoop.hdfs.util.ByteArray;

import com.cloudera.bigdata.analysis.index.Index;

/**
 * This formatter do nothing for given value, this is a "Null Object" design
 * pattern.
 */
public class PlainValueFormatter implements IndexFieldValueFormatter {

  /*-------------------------------------------   Major Logic Methods   --------------------------------------------*/

  @Override
  public byte[] format(Index.Field field, byte[] value) {
    return value;
  }

  @Override
  public byte[] deformat(Index.Field field, byte[] value) {
    return value;
  }

  @Override
  public ByteArray format(Index.Field field, ByteArray value) {
    return value;
  }

  @Override
  public ByteArray deformat(Index.Field field, ByteArray value) {
    return value;
  }

  /*--------------------------------------------     Common Methods    ---------------------------------------------*/

  @Override
  public void write(DataOutput dataOutput) throws IOException {

  }

  @Override
  public void readFields(DataInput dataInput) throws IOException {

  }
}
