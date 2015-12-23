package com.cloudera.bigdata.analysis.index.formatter;

import org.apache.hadoop.hdfs.util.ByteArray;
import org.apache.hadoop.io.Writable;

import com.cloudera.bigdata.analysis.index.Index;

/**
 * Each field of index could have its own format according to it's type, logic,
 * etc. In general, a field need a proper formatter to format its value. so we
 * provide a common interface that all formatter should implement.
 * 
 * @see com.cloudera.bigdata.analysis.index.formatter.LengthVariableValueFormatter
 * @see com.cloudera.bigdata.analysis.index.formatter.LongValueInverseFormatter
 * @see com.cloudera.bigdata.analysis.index.formatter.PlainValueFormatter
 */
public interface IndexFieldValueFormatter extends Writable {
  byte[] format(Index.Field field, byte[] value);

  byte[] deformat(Index.Field field, byte[] value);

  ByteArray format(Index.Field field, ByteArray value);

  ByteArray deformat(Index.Field field, ByteArray value);
}
