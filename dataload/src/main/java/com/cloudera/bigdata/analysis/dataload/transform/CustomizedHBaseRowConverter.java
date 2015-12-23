package com.cloudera.bigdata.analysis.dataload.transform;

import java.util.Map;

import org.apache.commons.lang.StringUtils;
import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.hbase.client.Put;
import org.apache.hadoop.hbase.util.Bytes;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;
import org.apache.phoenix.hbase.index.util.ImmutableBytesPtr;
import org.apache.phoenix.schema.SaltingUtil;
import org.apache.hadoop.hbase.io.ImmutableBytesWritable;
import com.cloudera.bigdata.analysis.dataload.exception.ETLException;
import com.cloudera.bigdata.analysis.dataload.source.ParsedLine;
import com.cloudera.bigdata.analysis.dataload.source.TextRecordSpec;
import com.cloudera.bigdata.analysis.dataload.util.CommonUtils;
import com.cloudera.bigdata.analysis.dataload.util.ExpressionUtils;
import com.cloudera.bigdata.analysis.index.Constants;
import com.cloudera.bigdata.analysis.index.IndexEntryBuilderGroup;
import com.cloudera.bigdata.analysis.index.util.RowKeyUtil;
import com.cloudera.bigdata.analysis.dataload.salter.OneBytePrefixKeySalter;

enum ColumnType {
  STRING, INTEGER, LONG, DOUBLE, BOOLEAN
}

public class CustomizedHBaseRowConverter {
  private Configuration conf;
  private TextRecordSpec recordSpec;
  private TargetTableSpec rowSpec;
  private long timeStamp;
  private IndexEntryBuilderGroup indexEntryBuilderGroup;
  private byte[] rowKey;
  private byte[] randomRowKeyPrefix;
  private Put[] indexPuts;
  private Put put;
  private byte[] regionStartKey;
  private OneBytePrefixKeySalter salter;
  private int regionQuantity;

  private static final Logger LOG = LoggerFactory
      .getLogger(CustomizedHBaseRowConverter.class);

  public CustomizedHBaseRowConverter(TextRecordSpec recordSpec,
      TargetTableSpec tableSpec, Configuration conf) {
    this.recordSpec = recordSpec;
    this.rowSpec = tableSpec;
    this.conf = conf;

    regionQuantity = Integer.parseInt(conf.get("regionQuantity"));
    //System.out.println("----regionQuantity" + conf.get("regionQuantity"));
    //System.out.println("-------regionQuantity Int" + regionQuantity);

    //salter = new OneBytePrefixKeySalter(regionQuantity);

    if (conf.getBoolean("buildIndex", false)) {
      indexEntryBuilderGroup = IndexEntryBuilderGroup.getInstance(CommonUtils
          .convertStringToByteArray(Constants.CURRENT_TABLE));
      if (LOG.isDebugEnabled()) {
        LOG.debug("indexEntryBuilderGroup: "
            + CommonUtils.convertByteArrayToString(Constants.CURRENT_TABLE
                .getBytes()));
      }
    }

  }

  public void setTimeStamp(long timeStamp) {
    this.timeStamp = timeStamp;
  }

  public TextRecordSpec getRecordSpec() {
    return recordSpec;
  }

  public Put convertToRow(ParsedLine line, boolean writeToWAL,
      boolean buildIndex) throws ETLException {
    String rowKeyValue = new String();
    if (buildIndex) {
      // user hash + UUID as rowkey
      randomRowKeyPrefix = RowKeyUtil.genRandomRowKeyPrefix();
      rowKey = RowKeyUtil.genRowKey(randomRowKeyPrefix);
      rowKeyValue = CommonUtils.convertByteArrayToString(rowKey);
      put = new Put(Bytes.toBytes(rowKeyValue), timeStamp);
    } else {
      rowKeyValue = ExpressionUtils.calculate(rowSpec.getRowKeySpec()
          .getSpecString(), line.getFieldMap());
      // UUID has 10000000 kinds randomly
      // rowKeyValue = rowKeyValuePre + Constants.COLUMN_ENTRY_SEPARATOR
      // + UUID.randomUUID().toString();

      // salter rowkey
      //put = new Put(salter.salt(Bytes.toBytes(rowKeyValue)), timeStamp);
      put = new Put(SaltingUtil.getSaltedKey(new ImmutableBytesPtr(Bytes.toBytes(rowKeyValue)), regionQuantity), timeStamp);
    }

    put.setWriteToWAL(writeToWAL);
    for (Map.Entry<String, TargetTableSpec.ColumnSpec> e : rowSpec
        .getColumnMap().entrySet()) {
      String columnName = e.getKey();
      String columnSpecString = e.getValue().getSpecString();

      String[] familyQualifierPairs = StringUtils
          .splitByWholeSeparatorPreserveAllTokens(columnName,
              Constants.FAMILY_QUALIFIER_SPLIT_CHARACTER);

      String familyName = familyQualifierPairs[0];

      String qualifierName = familyQualifierPairs[1];
      String qualifierValue = ExpressionUtils.calculate(columnSpecString,
          line.getFieldMap());

      // support multiple data type before writing into hbase table
      String columnType = "STRING";
      if (familyQualifierPairs.length == 3) {
        columnType = familyQualifierPairs[2].toUpperCase();
      }

      ColumnType ct = ColumnType.valueOf(columnType);
      switch (ct) {
      case INTEGER:
        put.add(Bytes.toBytes(familyName), Bytes.toBytes(qualifierName),
            Bytes.toBytes(Integer.parseInt(qualifierValue)));
        //System.out.println("Integer" + qualifierValue);
        break;
      case LONG:
        put.add(Bytes.toBytes(familyName), Bytes.toBytes(qualifierName),
            Bytes.toBytes(Long.parseLong(qualifierValue)));
        //System.out.println("Long" + qualifierValue);
        break;
      case DOUBLE:
        put.add(Bytes.toBytes(familyName), Bytes.toBytes(qualifierName),
            Bytes.toBytes(Double.parseDouble(qualifierValue)));
        //System.out.println("Double" + qualifierValue);
        break;
      case BOOLEAN:
        put.add(Bytes.toBytes(familyName), Bytes.toBytes(qualifierName),
            Bytes.toBytes(Boolean.parseBoolean(qualifierValue)));
        //System.out.println("Boolean" + qualifierValue);
        break;
      default:
        put.add(Bytes.toBytes(familyName), Bytes.toBytes(qualifierName),
            Bytes.toBytes(qualifierValue));
      }
    }
    return put;
  }

  public Put[] convertToIndex(ParsedLine line, boolean writeToWAL,
      boolean buildIndex) throws ETLException {
    if (buildIndex) {
      if (LOG.isDebugEnabled()) {
        LOG.debug("Intercepted Put: " + Bytes.toStringBinary(rowKey));
      }
      // only if a record put...
      if (RowKeyUtil.isRecord(rowKey)) {
        if (LOG.isDebugEnabled()) {
          LOG.debug("rowkey: " + CommonUtils.convertByteArrayToString(rowKey));
        }
        regionStartKey = CommonUtils.findRegionStarKey(
            conf.get("hbaseTargetTableSplitKeySpec"),
            CommonUtils.convertByteArrayToString(randomRowKeyPrefix));
        indexPuts = indexEntryBuilderGroup.build(regionStartKey, put);
      }
      return indexPuts;
    } else {
      return null;
    }
  }
}
