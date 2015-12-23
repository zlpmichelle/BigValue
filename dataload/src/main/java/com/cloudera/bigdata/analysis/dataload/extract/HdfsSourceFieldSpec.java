package com.cloudera.bigdata.analysis.dataload.extract;

import java.util.ArrayList;
import java.util.Map;

import com.cloudera.bigdata.analysis.dataload.Constants;
import com.cloudera.bigdata.analysis.dataload.io.ConfigReader;

public class HdfsSourceFieldSpec {
  private Map<String, String> confMap = null;
  private String cdhHBaseMasterIpaddress = null;
  private String hdfsSourceFileInputPath;
  private String hdfsSourceFileEncoding;
  private String hdfsSourceFileRecordFieldsDelimiter;
  private String hdfsSourceFileRecordFieldsNumber;
  private String hdfsSourceFileRecordFieldTypeInt;

  private ArrayList<String> toBeCleanedCellNames = null;
  private Map<String, String> toBeCleanedCellMap = null;

  public String getCdhHBaseMasterIpaddress() {
    return cdhHBaseMasterIpaddress;
  }

  public String getHdfsSourceFileInputPath() {
    return hdfsSourceFileInputPath;
  }

  public String getHdfsSourceFileEncoding() {
    return hdfsSourceFileEncoding;
  }

  public String getHdfsSourceFileRecordFieldsDelimiter() {
    return hdfsSourceFileRecordFieldsDelimiter;
  }

  public String getHdfsSourceFileRecordFieldsNumber() {
    return hdfsSourceFileRecordFieldsNumber;
  }

  public String getHdfsSourceFileRecordFieldTypeInt() {
    return hdfsSourceFileRecordFieldTypeInt;
  }

  public ArrayList<String> getToBeCleanedCellNames() {
    return toBeCleanedCellNames;
  }

  public Map<String, String> getToBeCleanedCellMap() {
    return toBeCleanedCellMap;
  }

  public HdfsSourceFieldSpec(Map<String, String> confMap) {
    this.confMap = confMap;
    readHdfsSourceFieldSpecsFromConfig();
  }

  public void readHdfsSourceFieldSpecsFromConfig() {
    cdhHBaseMasterIpaddress = (!confMap
        .containsKey(Constants.CDH_HBASE_MASTER_IPADDRESS) ? cdhHBaseMasterIpaddress
        : confMap.get(Constants.CDH_HBASE_MASTER_IPADDRESS));

    hdfsSourceFileInputPath = (!confMap
        .containsKey(Constants.HDFS_SOURCE_FILE_INPUT_PATH) ? hdfsSourceFileInputPath
        : confMap.get(Constants.HDFS_SOURCE_FILE_INPUT_PATH));
    hdfsSourceFileEncoding = (!confMap
        .containsKey(Constants.HDFS_SOURCE_FILE_ENCODING) ? hdfsSourceFileEncoding
        : confMap.get(Constants.HDFS_SOURCE_FILE_ENCODING));
    hdfsSourceFileRecordFieldsDelimiter = (!confMap
        .containsKey(Constants.HDFS_SOURCE_FILE_RECORD_FIELDS_DELIMITER) ? hdfsSourceFileRecordFieldsDelimiter
        : confMap.get(Constants.HDFS_SOURCE_FILE_RECORD_FIELDS_DELIMITER));
    hdfsSourceFileRecordFieldsNumber = (!confMap
        .containsKey(Constants.HDFS_SOURCE_FILE_RECORD_FIELDS_NUMBER) ? hdfsSourceFileRecordFieldsNumber
        : confMap.get(Constants.HDFS_SOURCE_FILE_RECORD_FIELDS_NUMBER));
    hdfsSourceFileRecordFieldTypeInt = (!confMap
        .containsKey(Constants.HDFS_SOURCE_FILE_RECORD_FIELDS_TYPE_INT) ? hdfsSourceFileRecordFieldTypeInt
        : confMap.get(Constants.HDFS_SOURCE_FILE_RECORD_FIELDS_TYPE_INT));
  }

  @Override
  public String toString() {
    StringBuilder sb = new StringBuilder();
    sb.append("cdhHBaseMasterIpaddress:").append(cdhHBaseMasterIpaddress)
        .append("\n");

    sb.append("hdfsSourceFileInputPath:").append(hdfsSourceFileInputPath)
        .append("\n");
    sb.append("hdfsSourceFileEncoding:").append(hdfsSourceFileEncoding)
        .append("\n");
    sb.append("hdfsSourceFileRecordFieldsDelimiter:")
        .append(hdfsSourceFileRecordFieldsDelimiter).append("\n");
    sb.append("hdfsSourceFileRecordFieldsNumber:")
        .append(hdfsSourceFileRecordFieldsNumber).append("\n");
    sb.append("hdfsSourceFileRecordFieldTypeInt:").append(
        hdfsSourceFileRecordFieldTypeInt);

    return sb.toString();
  }

  public static void main(String[] args) {
    HdfsSourceFieldSpec hsfs = new HdfsSourceFieldSpec(new ConfigReader(
        "etl-hdfs2hbase-conf.properties").getConfMap());
    System.out.println(hsfs.toString());
  }
}
