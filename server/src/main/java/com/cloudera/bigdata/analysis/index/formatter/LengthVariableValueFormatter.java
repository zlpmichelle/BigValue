package com.cloudera.bigdata.analysis.index.formatter;

import java.io.DataInput;
import java.io.DataOutput;
import java.io.IOException;

import org.apache.commons.lang.ArrayUtils;
import org.apache.hadoop.hbase.util.Bytes;
import org.apache.hadoop.hdfs.util.ByteArray;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import com.cloudera.bigdata.analysis.index.Index;

/**
 * For fields which length is variable, they can NOT add into row key as a
 * fragment. If make it as a part of index(row key), we have to guarantee: 1.
 * the length is fix. 2. safely and easily retrieve original value from
 * formatted value. This is what this class do. It align value to store from
 * left in a fix length byte array, fill 0x00 for none-used space, and use last
 * byte in the array to store original value length. When retrieve value from
 * the byte array, program will read the last byte first, then cut the sub array
 * from start to value length.
 * 
 */
public class LengthVariableValueFormatter implements IndexFieldValueFormatter {

  private static final Logger LOG = LoggerFactory
      .getLogger(LengthVariableValueFormatter.class);

  public LengthVariableValueFormatter() {
  }

  /*-------------------------------------------   Major Logic Methods   --------------------------------------------*/

  @Override
  public byte[] format(Index.Field field, byte[] value) {
    byte[] clonedValue = ArrayUtils.clone(value);
    int maxLength = field.getMaxLength();
    if (clonedValue.length == maxLength) {
      if (!field.isLengthVariable()) {
        return clonedValue;
      } else {
        throw new RuntimeException(
            "if indexed qualifier is length-variable,the value length should be"
                + " <= (qualifier_max_length-1, because the last byte is use to indicate the actual length of value.");
      }
    } else if (clonedValue.length < maxLength) {
      if (field.isLengthVariable()) {
        byte actualLength = (byte) clonedValue.length;
        clonedValue = Bytes.padTail(clonedValue, maxLength - actualLength);
        // append actual length at the tail of array.
        clonedValue[maxLength - 1] = actualLength;
      }
      return clonedValue;
    } else {
      String message = "The real length of value:" + Bytes.toStringBinary(value)
          + "is longer than specified max length " + field.getMaxLength();
      RuntimeException exception = new RuntimeException(message);
      LOG.error(message, exception);
      throw exception;
    }
  }

  @Override
  public byte[] deformat(Index.Field field, byte[] value) {
    return null;
  }

  @Override
  public ByteArray format(Index.Field field, ByteArray value) {
    return new ByteArray(format(field, value.getBytes()));
  }

  @Override
  public ByteArray deformat(Index.Field field, ByteArray value) {
    return null;
  }

  /*--------------------------------------------     Common Methods    ---------------------------------------------*/

  @Override
  public void write(DataOutput dataOutput) throws IOException {
  }

  @Override
  public void readFields(DataInput dataInput) throws IOException {
  }
}
