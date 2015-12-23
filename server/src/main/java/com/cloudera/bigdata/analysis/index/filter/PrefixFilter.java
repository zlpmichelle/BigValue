package com.cloudera.bigdata.analysis.index.filter;

import org.apache.hadoop.hbase.exceptions.DeserializationException;
import org.apache.hadoop.hbase.filter.FilterBase;

import com.cloudera.bigdata.analysis.index.protobuf.generated.PrefixFilterProtos;
import com.google.protobuf.HBaseZeroCopyByteString;
import com.google.protobuf.InvalidProtocolBufferException;

public class PrefixFilter extends FilterBase {

  protected byte[] prefix = null;

  public PrefixFilter(final byte[] prefix) {
    this.prefix = prefix;
  }

  /**
   * corresponding to write()
   * 
   * @return The filter serialized using pb
   */
  public byte[] toByteArray() {
    PrefixFilterProtos.PrefixFilter.Builder builder = PrefixFilterProtos.PrefixFilter
        .newBuilder();
    if (this.prefix != null)
      builder.setPrefix(HBaseZeroCopyByteString.wrap(this.prefix));
    return builder.build().toByteArray();
  }

  /**
   * corresponding to readFields()
   * 
   * @param pbBytes
   *          A pb serialized {@link PrefixFilter} instance
   * @return An instance of {@link PrefixFilter} made from <code>bytes</code>
   * @throws org.apache.hadoop.hbase.exceptions.DeserializationException
   * @see #toByteArray
   */
  public static PrefixFilter parseFrom(final byte[] pbBytes)
      throws DeserializationException {
    PrefixFilterProtos.PrefixFilter proto;
    try {
      proto = PrefixFilterProtos.PrefixFilter.parseFrom(pbBytes);
    } catch (InvalidProtocolBufferException e) {
      throw new DeserializationException(e);
    }
    return new PrefixFilter(proto.hasPrefix() ? proto.getPrefix().toByteArray()
        : null);
  }
}
