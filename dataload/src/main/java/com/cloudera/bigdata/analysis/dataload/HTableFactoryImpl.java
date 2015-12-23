package com.cloudera.bigdata.analysis.dataload;

import java.io.IOException;
import java.util.concurrent.ExecutorService;

import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.hbase.client.HConnection;
import org.apache.hadoop.hbase.client.HTable;
import org.apache.hadoop.hbase.client.HTableInterface;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

/**
 * Customized version for HTableInterfaceFactory.
 */
public class HTableFactoryImpl extends HTableFactoryAdapter {
  private static final Logger LOG = LoggerFactory
      .getLogger(HTableFactoryImpl.class);

  // with MB unit
  private final int writeBufferSize;
  private final boolean autoFlush;

  public HTableFactoryImpl(int writeBufferSize, boolean autoFlush) {
    this.writeBufferSize = writeBufferSize;
    this.autoFlush = autoFlush;
  }

  @Override
  public HTableInterface createHTableInterface(Configuration config,
      byte[] tableName) {
    try {
      if (LOG.isDebugEnabled()) {
        LOG.info("Creating HTable for " + new String(tableName));
      }
      HTable tbl = new HTable(config, tableName);
      tbl.setWriteBufferSize(writeBufferSize * 1024 * 1024);
      tbl.setAutoFlush(autoFlush);
      return tbl;
    } catch (IOException ioe) {
      throw new RuntimeException(ioe);
    }
  }

  @Override
  public void releaseHTableInterface(HTableInterface table) {
    try {
      table.close();
      LOG.info(Thread.currentThread().getName() + "Closed HTable");
    } catch (IOException e) {
      throw new RuntimeException(e);
    }
  }

  public HTableInterface createHTableInterface(Configuration arg0, byte[] arg1,
      ExecutorService arg2) {
    // TODO Auto-generated method stub
    return null;
  }

  public HTableInterface createHTableInterface(byte[] arg0, HConnection arg1,
      ExecutorService arg2) {
    // TODO Auto-generated method stub
    return null;
  }
}
