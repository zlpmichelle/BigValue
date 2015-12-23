package com.cloudera.bigdata.analysis.dataload.transform;

import java.util.ArrayList;
import java.util.Map;

import com.cloudera.bigdata.analysis.dataload.Constants;

public class BulkLoadProcessFieldSpec {
  private Map<String, String> confMap = null;
  private boolean buildIndex = false;
  private String regionQuantity = null;
  private String indexConfFileName = null;
  private String hbaseCoprocessorLocation = null;
  private boolean onlyGenerateSplitKeySpec = false;
  private boolean preCreateRegions = true;
  private String rowkeyPrefix = null;
  private String recordsNumPerRegion = null;
  private String inputSplitSize = null;
  private String extendedHBaseRowConverterClass = null;
  private String importDate = null;
  private String validatorClass = "com.cloudera.bigdata.analysis.dataload.exception.RecordValidator";
  private boolean createMalformedTable = false;
  private boolean nativeTaskEnabled = true;

  // TODO whether to clean or not
  private boolean cleanup = false;
  private ArrayList<String> toBeCleanedCellNames = null;
  private Map<String, String> toBeCleanedCellMap = null;

  public Map<String, String> getConfMap() {
    return confMap;
  }

  public void setConfMap(Map<String, String> confMap) {
    this.confMap = confMap;
  }

  public boolean isBuildIndex() {
    return buildIndex;
  }

  public boolean isOnlyGenerateSplitKeySpec() {
    return onlyGenerateSplitKeySpec;
  }

  public boolean isPreCreateRegions() {
    return preCreateRegions;
  }

  public String getRegionQuantity() {
    return regionQuantity;
  }

  public void setRegionQuantity(String regionQuantity) {
    this.regionQuantity = regionQuantity;
  }

  public String getHBaseCoprocessorLocation() {
    return hbaseCoprocessorLocation;
  }

  public void setHBaseCoprocessorLocation(String hbaseCoprocessorLocation) {
    this.hbaseCoprocessorLocation = hbaseCoprocessorLocation;
  }

  public String getIndexConfFileName() {
    return indexConfFileName;
  }

  public void setIndexConfFileName(String indexConfFileName) {
    this.indexConfFileName = indexConfFileName;
  }

  public String getRowkeyPrefix() {
    return rowkeyPrefix;
  }

  public void setRowkeyPrefix(String rowkeyPrefix) {
    this.rowkeyPrefix = rowkeyPrefix;
  }

  public String getRecordsNumPerRegion() {
    return recordsNumPerRegion;
  }

  public void setRecordsNumPerRegion(String recordsNumPerRegion) {
    this.recordsNumPerRegion = recordsNumPerRegion;
  }

  public String getInputSplitSize() {
    return inputSplitSize;
  }

  public void setInputSplitSize(String inputSplitSize) {
    this.inputSplitSize = inputSplitSize;
  }

  public boolean isCleanup() {
    return cleanup;
  }

  public void setCleanup(boolean cleanup) {
    this.cleanup = cleanup;
  }

  public ArrayList<String> getToBeCleanedCellNames() {
    return toBeCleanedCellNames;
  }

  public void setToBeCleanedCellNames(ArrayList<String> toBeCleanedCellNames) {
    this.toBeCleanedCellNames = toBeCleanedCellNames;
  }

  public Map<String, String> getToBeCleanedCellMap() {
    return toBeCleanedCellMap;
  }

  public void setToBeCleanedCellMap(Map<String, String> toBeCleanedCellMap) {
    this.toBeCleanedCellMap = toBeCleanedCellMap;
  }

  public String getExtendedHBaseRowConverterClass() {
    return extendedHBaseRowConverterClass;
  }

  public void setExtendedHBaseRowConverterClass(
      String extendedHBaseRowConverterClass) {
    this.extendedHBaseRowConverterClass = extendedHBaseRowConverterClass;
  }

  public String getImportDate() {
    return importDate;
  }

  public void setImportDate(String importDate) {
    this.importDate = importDate;
  }

  public String getValidatorClass() {
    return validatorClass;
  }

  public void setValidatorClass(String validatorClass) {
    this.validatorClass = validatorClass;
  }

  public boolean isCreateMalformedTable() {
    return createMalformedTable;
  }

  public void setcreateMalformedTable(boolean createMalformedTable) {
    this.createMalformedTable = createMalformedTable;
  }

  public boolean isNativeTaskEnabled() {
    return nativeTaskEnabled;
  }

  public void setNativeTaskEnabled(boolean nativeTaskEnabled) {
    this.nativeTaskEnabled = nativeTaskEnabled;
  }

  public BulkLoadProcessFieldSpec(Map<String, String> confMap) {
    this.confMap = confMap;
    readBulkLoadProcessFieldSpecsFromConfig();
  }

  private void readBulkLoadProcessFieldSpecsFromConfig() {
    buildIndex = (!confMap.containsKey(Constants.BUILD_INDEX) ? buildIndex
        : Boolean.parseBoolean(confMap.get(Constants.BUILD_INDEX)));
    regionQuantity = (!confMap.containsKey(Constants.REGION_QUANTITY) ? regionQuantity
        : confMap.get(Constants.REGION_QUANTITY));
    indexConfFileName = (!confMap.containsKey(Constants.INDEX_CONF_FILE_NAME) ? indexConfFileName
        : confMap.get(Constants.INDEX_CONF_FILE_NAME));
    hbaseCoprocessorLocation = (!confMap
        .containsKey(Constants.HBASE_COPROCESSOR_LOCATION) ? hbaseCoprocessorLocation
        : confMap.get(Constants.HBASE_COPROCESSOR_LOCATION));

    onlyGenerateSplitKeySpec = (!confMap
        .containsKey(Constants.ONLY_GENERATE_SPLITKEYSPEC) ? onlyGenerateSplitKeySpec
        : Boolean.parseBoolean(confMap
            .get(Constants.ONLY_GENERATE_SPLITKEYSPEC)));
    preCreateRegions = (!confMap.containsKey(Constants.PRE_CREATE_REGIONS) ? preCreateRegions
        : Boolean.parseBoolean(confMap.get(Constants.PRE_CREATE_REGIONS)));
    rowkeyPrefix = (!confMap.containsKey(Constants.ROWKEY_PREFIX) ? rowkeyPrefix
        : confMap.get(Constants.ROWKEY_PREFIX));
    recordsNumPerRegion = (!confMap
        .containsKey(Constants.RECORDS_NUM_PER_REGION) ? recordsNumPerRegion
        : confMap.get(Constants.RECORDS_NUM_PER_REGION));
    inputSplitSize = (!confMap.containsKey(Constants.INPUT_SPLIT_SIZE) ? inputSplitSize
        : confMap.get(Constants.INPUT_SPLIT_SIZE));

    extendedHBaseRowConverterClass = (!confMap
        .containsKey(Constants.EXTENDEDHBASEROWCONVERTER_CLASS_KEY) ? extendedHBaseRowConverterClass
        : confMap.get(Constants.EXTENDEDHBASEROWCONVERTER_CLASS_KEY));

    importDate = (!confMap.containsKey(Constants.IMPORT_DATE) ? importDate
        : confMap.get(Constants.IMPORT_DATE));
    validatorClass = (!confMap.containsKey(Constants.VALIDATOR_CLASS) ? validatorClass
        : confMap.get(Constants.VALIDATOR_CLASS));
    createMalformedTable = (!confMap
        .containsKey(Constants.CREATE_MALFORMED_TABLE) ? createMalformedTable
        : Boolean.parseBoolean(confMap.get(Constants.CREATE_MALFORMED_TABLE)));
    nativeTaskEnabled = (!confMap.containsKey(Constants.NATIVETASK_ENABLED) ? nativeTaskEnabled
        : Boolean.parseBoolean(confMap.get(Constants.NATIVETASK_ENABLED)));
  }

  public String toString() {
    StringBuilder sb = new StringBuilder();
    sb.append("buildIndex:").append(buildIndex).append("\n");
    sb.append("regionQuantity:").append(regionQuantity).append("\n");
    sb.append("indexConfFileName:").append(indexConfFileName).append("\n");
    sb.append("hbaseCoprocessorLocation:").append(hbaseCoprocessorLocation)
        .append("\n");

    sb.append("onlyGenerateSplitKeySpec:").append(preCreateRegions)
        .append("\n");
    sb.append("preCreateRegions:").append(preCreateRegions).append("\n");
    sb.append("rowkeyPrefix:").append(rowkeyPrefix).append("\n");
    sb.append("recordsNumPerRegion:").append(recordsNumPerRegion).append("\n");
    sb.append("extendedHBaseRowConverterClass:")
        .append(extendedHBaseRowConverterClass).append("\n");
    sb.append("inputSplitSize:").append(inputSplitSize).append("\n");
    sb.append("importDate:").append(importDate).append("\n");
    sb.append("validatorClass:").append(validatorClass).append("\n");
    sb.append("createMalformedTable:").append(createMalformedTable)
        .append("\n");
    sb.append("nativeTaskEnabled:").append(nativeTaskEnabled);

    return sb.toString();
  }
}
