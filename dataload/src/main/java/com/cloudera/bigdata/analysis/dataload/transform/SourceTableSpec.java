package com.cloudera.bigdata.analysis.dataload.transform;

import org.apache.commons.lang.StringUtils;
import org.apache.hadoop.hbase.client.Scan;

import com.cloudera.bigdata.analysis.dataload.Constants;
import com.cloudera.bigdata.analysis.dataload.exception.ETLException;

public class SourceTableSpec extends HBaseTableSpec {
  private String rowkeySpecString;
  private String cellSplitSeparator;
  private RowkeySpec rowkeySpec;

  public SourceTableSpec(String tableName, String cellSpecString,
      String rowkeySpecString) throws Exception {
    this(tableName, cellSpecString, rowkeySpecString,
        Constants.CELL_SPLIT_CHARACTER);
  }

  public SourceTableSpec(String tableName, String cellSpecString,
      String rowkeySpecString, String cellSplitSeparator) throws Exception {
    super(tableName, cellSpecString, null);
    this.rowkeySpecString = rowkeySpecString;
    this.cellSplitSeparator = cellSplitSeparator;
  }

  public String getRowkeySpecString() {
    return rowkeySpecString;
  }

  public String getCellSplitSeparator() {
    return cellSplitSeparator;
  }

  public RowkeySpec getRowKeySpec() {
    return rowkeySpec;
  }

  public String toString() {
    return tableName + "-" + rowkeySpec.toString();
  }

  @Override
  protected void initializeFieldSpecs() throws ETLException {

    String columnNormalized = StringUtils.deleteWhitespace(rowSpecString);
    String[] columnSpecs = StringUtils.splitByWholeSeparatorPreserveAllTokens(
        columnNormalized, Constants.CELL_SPLIT_CHARACTER);

    if (columnSpecs.length <= 1)
      throw new ETLException(
          "Invalid source table column spec: No column found in "
              + rowSpecString);

    RowkeySpec rowkey = null;

    if (rowkeySpecString != null && !rowkeySpecString.isEmpty()
        && !rowkeySpecString.equalsIgnoreCase("all")) {
      String rowkeyNormalized = StringUtils.deleteWhitespace(rowkeySpecString);
      String[] rowkeySpecs = StringUtils
          .splitByWholeSeparatorPreserveAllTokens(rowkeyNormalized,
              Constants.CELL_SPLIT_CHARACTER);

      if (rowkeySpecs.length != 2)
        throw new ETLException("Invalid source table rowkey spec: "
            + rowkeySpecString);

      rowkey = new RowkeySpec(rowkeySpecs[0], rowkeySpecs[1]);

    }
    rowkeySpec = rowkey;
  }

  public static class RowkeySpec {

    private byte[] startKey;
    private byte[] endKey;

    public byte[] getStartKey() {
      return startKey;
    }

    public byte[] getEndKey() {
      return endKey;
    }

    public RowkeySpec(String startKey, String endKey) {
      this.startKey = startKey.getBytes();
      this.endKey = endKey.getBytes();
    }

    public String toString() {
      return startKey + ":" + endKey;
    }
  }

  public Scan buildScan() {
    Scan scan = new Scan();
    scan.setMaxVersions(1);
    if (getRowKeySpec() != null) {
      byte[] startKey = getRowKeySpec().getStartKey();
      byte[] endKey = getRowKeySpec().getEndKey();
      if (startKey != null && startKey.length != 0)
        scan.setStartRow(startKey);
      if (endKey != null && endKey.length != 0)
        scan.setStopRow(endKey);
    }

    String cellSpecString = getRowSpecString();
    String[] cellEntrys = StringUtils.splitByWholeSeparatorPreserveAllTokens(
        cellSpecString, Constants.CELL_SPLIT_CHARACTER);
    for (String cellEntry : cellEntrys) {
      String[] familyQualifierPairs = StringUtils
          .splitByWholeSeparatorPreserveAllTokens(cellEntry,
              Constants.FAMILY_QUALIFIER_SPLIT_CHARACTER);
      byte[] family = familyQualifierPairs[0].getBytes();
      byte[] qualifier = familyQualifierPairs[1].getBytes();

      if (family != null && family.length != 0 && qualifier != null
          && qualifier.length != 0)
        scan.addColumn(family, qualifier);
    }

    return scan;
  }

}
