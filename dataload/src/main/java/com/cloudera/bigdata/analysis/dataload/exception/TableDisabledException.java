/**
 * The sample is org.apache.hadoop.hbase.TableNotDisabledException
 */
package com.cloudera.bigdata.analysis.dataload.exception;

import java.io.IOException;

import org.apache.hadoop.hbase.util.Bytes;

public class TableDisabledException extends IOException {
  private static final long serialVersionUID = 6945445100536188350L;

  /** default constructor */
  public TableDisabledException() {
    super();
  }

  /**
   * Constructor
   * 
   * @param s
   *          message
   */
  public TableDisabledException(String s) {
    super(s);
  }

  /**
   * @param tableName
   *          Name of table that is disabled
   */
  public TableDisabledException(byte[] tableName) {
    this(Bytes.toString(tableName));
  }

}
