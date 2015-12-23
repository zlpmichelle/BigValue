package com.cloudera.bigdata.analysis.dataload;

import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.hbase.client.HConnection;
import org.apache.hadoop.hbase.client.HTableInterface;
import org.apache.hadoop.hbase.client.HTableInterfaceFactory;
import org.apache.hadoop.hbase.executor.ExecutorService;

public abstract class HTableFactoryAdapter implements HTableInterfaceFactory {

  public HTableInterface createHTableInterface(Configuration config,
      byte[] tableName, ExecutorService pool) {
    return null;
  }

  public HTableInterface createHTableInterface(byte[] tableName,
      HConnection connection) {
    return null;
  }

  public HTableInterface createHTableInterface(byte[] tableName,
      HConnection connection, ExecutorService pool) {
    return null;
  }

}
