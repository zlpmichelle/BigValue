package com.cloudera.bigdata.analysis.dataload.transform;

import java.util.Map;

import org.apache.commons.lang.StringUtils;
import org.apache.hadoop.hbase.client.Put;
import org.apache.hadoop.hbase.util.Bytes;

import com.cloudera.bigdata.analysis.dataload.Constants;
import com.cloudera.bigdata.analysis.dataload.exception.ETLException;
import com.cloudera.bigdata.analysis.dataload.util.ExpressionUtils;

public class HBase2HBaseRowConverter {

  private TargetTableSpec rowSpec;
  private long timeStamp;

  public HBase2HBaseRowConverter(TargetTableSpec rowSpec) throws Exception {
    this.rowSpec = rowSpec;
  }

  public void setTimeStamp(long timeStamp) {
    this.timeStamp = timeStamp;
  }

  public Put convert(ParsedResult result, boolean writeToWAL)
      throws ETLException {
    String rowKeyValue = ExpressionUtils.calculate(rowSpec.getRowKeySpec()
        .getSpecString(), result.getFieldMap());
    Put put = new Put(Bytes.toBytes(rowKeyValue), timeStamp);
    put.setWriteToWAL(writeToWAL);
    for (Map.Entry<String, TargetTableSpec.ColumnSpec> e : rowSpec
        .getColumnMap().entrySet()) {
      String columnName = e.getKey();
      String[] familyQualifierPairs = StringUtils
          .splitByWholeSeparatorPreserveAllTokens(columnName,
              Constants.FAMILY_QUALIFIER_SPLIT_CHARACTER);
      String familyName = familyQualifierPairs[0];
      String qualifierName = familyQualifierPairs[1];
      String columnSpecString = e.getValue().getSpecString();
      String qualifierValue = ExpressionUtils.calculate(columnSpecString,
          result.getFieldMap());
      put.add(Bytes.toBytes(familyName), Bytes.toBytes(qualifierName),
          Bytes.toBytes(qualifierValue));
    }
    return put;
  }
}
