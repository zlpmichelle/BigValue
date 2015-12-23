package com.cloudera.bigdata.analysis.dataload.extract;

import java.util.HashMap;
import java.util.Map;

import com.cloudera.bigdata.analysis.dataload.Constants;
import com.cloudera.bigdata.analysis.dataload.exception.ETLException;
import com.cloudera.bigdata.analysis.dataload.io.ConfigReader;
import com.cloudera.bigdata.analysis.dataload.util.CommonUtils;

public class HBaseTargetFieldSpec {
  private Map<String, String> confMap;
  private String hbaseGeneratedHfilesOutputPath;
  private String hbaseTargetTableName;
  private boolean hbaseTargetWriteToWAL = false;
  private String hbaseTargetTableSplitKeySpec;
  private String hbaseTargetTableCellSpec;
  private Map<String, String> targetTableCellMap;

  public String getHbaseGeneratedHfilesOutputPath() {
    return hbaseGeneratedHfilesOutputPath;
  }

  public String getHbaseTargetTableName() {
    return hbaseTargetTableName;
  }

  public boolean isHbaseTargetWriteToWAL() {
    return hbaseTargetWriteToWAL;
  }

  public String getHbaseTargetTableSplitKeySpec() {
    return hbaseTargetTableSplitKeySpec;
  }

  public String getHbaseTargetTableCellSpec() {
    return hbaseTargetTableCellSpec;
  }

  public Map<String, String> getTargetTableCellMap() {
    return targetTableCellMap;
  }

  public HBaseTargetFieldSpec(Map<String, String> confMap) {
    this.confMap = confMap;
    readHBaseTargetFieldSpecsFromConfig();
  }

  private void readHBaseTargetFieldSpecsFromConfig() {
    Map<String, String> targetTableCellMap = new HashMap<String, String>();
    this.targetTableCellMap = targetTableCellMap;

    hbaseGeneratedHfilesOutputPath = (!confMap
        .containsKey(Constants.HBASE_GENERATED_HFILES_OUTPUT_PATH) ? hbaseGeneratedHfilesOutputPath
        : confMap.get(Constants.HBASE_GENERATED_HFILES_OUTPUT_PATH));

    hbaseTargetTableName = (!confMap
        .containsKey(Constants.HBASE_TARGET_TABLE_NAME) ? hbaseTargetTableName
        : confMap.get(Constants.HBASE_TARGET_TABLE_NAME));

    hbaseTargetWriteToWAL = (!confMap
        .containsKey(Constants.HBASE_TARGET_WRITE_TO_WAL_FLAG) ? hbaseTargetWriteToWAL
        : Boolean.parseBoolean(confMap
            .get(Constants.HBASE_TARGET_WRITE_TO_WAL_FLAG)));

    hbaseTargetTableSplitKeySpec = (!confMap
        .containsKey(Constants.HBASE_TARGET_TABLE_SPLIT_KEY_SPEC) ? hbaseTargetTableSplitKeySpec
        : confMap.get(Constants.HBASE_TARGET_TABLE_SPLIT_KEY_SPEC));

    hbaseTargetTableCellSpec = (!confMap
        .containsKey(Constants.HBASE_TARGET_TABLE_CELL_SPEC) ? hbaseTargetTableCellSpec
        : confMap.get(Constants.HBASE_TARGET_TABLE_CELL_SPEC));

    if (!confMap.containsKey(Constants.HBASE_TARGET_TABLE_CELL_SPEC)) {
      ETLException.handle("Can't find properties "
          + Constants.HBASE_TARGET_TABLE_CELL_SPEC);
    }

    String[] targetTableCellKeys = confMap
        .get(Constants.HBASE_TARGET_TABLE_CELL_SPEC).trim()
        .split(Constants.CELL_SPLIT_CHARACTER);

    // if it uses CustomizedHBaseRowConverter
    for (String targetTableCellKey : targetTableCellKeys) {
      if (!confMap.containsKey(targetTableCellKey)) {
        ETLException
            .handle("Can't find properties for target table cell key \""
                + targetTableCellKey + "\"");
      }

      String targetTableCellValue = confMap.get(targetTableCellKey).trim();
      if (targetTableCellValue.isEmpty()) {
        ETLException.handle("Empty value for target table cell key \""
            + targetTableCellKey + "\"");
      }
      targetTableCellMap.put(targetTableCellKey, targetTableCellValue);
    }

    if (!targetTableCellMap.containsKey(Constants.ROW_KEY_NAME)) {
      ETLException
          .handle("Can't find 'rowkey' properties in target table cell key");
    }
    if (targetTableCellMap.size() != targetTableCellKeys.length) {
      ETLException.handle("Illegal number for target table cell key");
    }

  }

  @Override
  public String toString() {
    StringBuilder sb = new StringBuilder();
    sb.append("hbaseGeneratedHfilesOutputPath:")
        .append(hbaseGeneratedHfilesOutputPath).append("\n");
    sb.append("hbaseTargetTableName:").append(hbaseTargetTableName)
        .append("\n");
    sb.append("hbaseTargetWriteToWAL:").append(hbaseTargetWriteToWAL)
        .append("\n");
    sb.append("hbaseTargetTableSplitKeySpec:")
        .append(hbaseTargetTableSplitKeySpec).append("\n");
    sb.append("hbaseTargetTableCellSpec:").append(hbaseTargetTableCellSpec)
        .append("\n");
    sb.append("targetTableCellMap:").append(targetTableCellMap).append("\n");
    sb.append(
        "target table cell getStringFromMap:" + "\n"
            + CommonUtils.getStringFromMap(targetTableCellMap, "|", "="))
        .append("\n");
    sb.append("target table cell getMapFromString:"
        + "\n"
        + CommonUtils.getMapFromString(
            CommonUtils.getStringFromMap(targetTableCellMap, "|", "="), "|",
            "="));

    return sb.toString();
  }

  public static void main(String[] args) {
    HBaseTargetFieldSpec htfs = new HBaseTargetFieldSpec(new ConfigReader(
        "etl-hbase2hbase-conf.properties").getConfMap());
    System.out.println(htfs.toString());
  }
}
