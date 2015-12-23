package com.cloudera.bigdata.analysis.query;

import java.util.Comparator;

import org.apache.hadoop.hbase.client.Result;
import org.apache.hadoop.hbase.util.Bytes;
import org.apache.hadoop.hdfs.util.ByteArray;

/**
 * When records returned from remote server, all records should sort with this
 * comparator according to {@link com.cloudera.bigdata.analysis.query.Query}
 * object specified. This is a generic comparator based on reflection. it can
 * sort by any field of record object. the only thing should be careful, the
 * order-by qualifier must keep the same name with field!
 */
public class ResultComparator implements Comparator<Result> {

  private ByteArray orderByColumnFamily;

  private ByteArray orderByQualifier;

  private Order order;

  public ResultComparator() {
  }

  public ResultComparator(ByteArray orderByColumnFamily,
      ByteArray orderByQualifier, Order order) {
    this.orderByColumnFamily = orderByColumnFamily;
    this.orderByQualifier = orderByQualifier;
    this.order = order;
  }

  @Override
  public int compare(Result r1, Result r2) {
    // Yes, for ASC, we should let less value place in the tail of queue.
    if (order == Order.ASC) {
      return Bytes.compareTo(
          r1.getValue(orderByColumnFamily.getBytes(),
              orderByQualifier.getBytes()),
          r2.getValue(orderByColumnFamily.getBytes(),
              orderByQualifier.getBytes()));
    } else {
      return Bytes.compareTo(
          r2.getValue(orderByColumnFamily.getBytes(),
              orderByQualifier.getBytes()),
          r1.getValue(orderByColumnFamily.getBytes(),
              orderByQualifier.getBytes()));
    }
  }
}