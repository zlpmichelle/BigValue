package com.cloudera.bigdata.analysis.dataload.source;

import java.io.IOException;

import org.apache.hadoop.hbase.HColumnDescriptor;

/**
 * It can be shared for Records being put to HBase. It's not necessary to create
 * a new instance for each record. All the fields in implementation should be
 * defined as final.
 */
public interface HTableDefinition {
  /** Get the table name of the hbase table */
  public String getTableName();

  /** Get the column families of the hbase table */
  public HColumnDescriptor[] getColumnFamilies();

  /** Get the split keys of the hbase table, return null if no split keys */
  public byte[][] getSplitKeys() throws Exception;

  /** Get the memstore flush size */
  public long getMemStoreFlushSize();

  /** Close the hbase table */
  public void close() throws IOException;
}
