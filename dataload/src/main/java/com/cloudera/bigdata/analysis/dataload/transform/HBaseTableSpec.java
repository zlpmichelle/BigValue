package com.cloudera.bigdata.analysis.dataload.transform;

import java.util.HashMap;
import java.util.Map;

import org.apache.hadoop.hbase.util.Bytes;

import com.cloudera.bigdata.analysis.dataload.exception.ETLException;

public abstract class HBaseTableSpec {

  public static final String COLUMN_FAMILY_NAME = "f";
  public static final byte[] COLUMN_FAMILY_NAME_BYTES = Bytes
      .toBytes(COLUMN_FAMILY_NAME);
  public static final String EXTERNAL_COLUMN_DELIMITER = ";";
  public static final String COLUMN_KEY_VALUE_DELIMITER = ":";

  public static final String ROW_KEY_NAME = "rowKey";

  public static final String DEFAUL_INTERNAL_COLUMN_DELIMITER = "|";

  protected String tableName;
  // rowSpecString is a string of rowkey spec and column spec, seperated by "|",
  // and key value are seperated by "="
  protected String rowSpecString;
  protected String splitKeySpec;

  protected String externalColumnDelimiter;
  protected String internalColumnDelimiter;

  ColumnSpec rowKeySpec;

  Map<String, ColumnSpec> columnMap = new HashMap<String, ColumnSpec>();

  public HBaseTableSpec(String tableName, String rowSpecString, String splitKeys)
      throws Exception {
    this(tableName, rowSpecString, splitKeys, EXTERNAL_COLUMN_DELIMITER);
  }

  public HBaseTableSpec(String tableName, String rowSpecString,
      String splitKeys, String columnDelimiter) throws Exception {
    this(tableName, rowSpecString, splitKeys, columnDelimiter,
        DEFAUL_INTERNAL_COLUMN_DELIMITER);
  }

  public HBaseTableSpec(String tableName, String rowSpecString,
      String splitKeySpec, String externalColumnDelimiter,
      String internalColumnDelimiter) throws Exception {
    this.tableName = tableName;
    this.rowSpecString = rowSpecString;
    this.splitKeySpec = splitKeySpec;
    this.externalColumnDelimiter = externalColumnDelimiter;
    this.internalColumnDelimiter = internalColumnDelimiter;
    initializeFieldSpecs();
  }

  public String getTableName() {
    return tableName;
  }

  public String getRowSpecString() {
    return rowSpecString;
  }

  public String getSplitKeySpec() {
    return splitKeySpec;
  }

  public String getInternalColumnDelimiter() {
    return internalColumnDelimiter;
  }

  public String toString() {
    return tableName + "-" + rowKeySpec.toString() + "-"
        + columnMap.values().toString() + "-" + splitKeySpec.toString();
  }

  public static class ColumnSpec {

    private String columnName;
    private String specString;

    public String getColumnName() {
      return columnName;
    }

    public String getSpecString() {
      return specString;
    }

    public ColumnSpec(String columnName, String specString) {
      this.columnName = columnName;
      this.specString = specString;
    }

    public String toString() {
      return columnName + ":" + specString;
    }

  }

  protected abstract void initializeFieldSpecs() throws ETLException;
}
