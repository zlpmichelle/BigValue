package com.cloudera.bigdata.analysis.dataload.source;

import java.util.ArrayList;
import java.util.HashMap;
import java.util.List;
import java.util.Map;

import org.apache.commons.lang.StringUtils;

import com.cloudera.bigdata.analysis.dataload.Constants;
import com.cloudera.bigdata.analysis.dataload.util.ExpressionUtils;

public class TextRecordSpec {

  private String fieldDelimiter;
  private String fieldNameTypeValueDelimiter;

  private String escapedFieldDelimiter;
  private String escapedFieldNameTypeValueDelimiter;

  // record Spec from "hdfs.source.file.record.fields.delimiter",
  // "hdfs.source.text.file.record.fields.number" and
  // "hdfs.source.file.record.fields.type.int"
  private String recordSpecString;
  private String encoding;

  private List<FieldSpec> fieldList = new ArrayList<FieldSpec>();

  Map<String, FieldSpec> fieldMap = new HashMap<String, FieldSpec>();

  public static class FieldSpec {

    private int fieldIndex;
    private String fieldName;
    private String fieldType;

    public FieldSpec(int fieldIndex, String fieldName, String fieldType) {
      this.fieldIndex = fieldIndex;
      this.fieldName = fieldName;
      this.fieldType = fieldType.toUpperCase();
    }

    public int getFieldIndex() {
      return fieldIndex;
    }

    public String getFieldName() {
      return fieldName;
    }

    public String getFieldType() {
      return fieldType;
    }

    public String toString() {
      StringBuilder sb = new StringBuilder();
      sb.append(fieldIndex)
          .append(Constants.DEFAULT_FIELD_NAME_TYPE_VALUE_DELIMITER)
          .append(fieldName)
          .append(Constants.DEFAULT_FIELD_NAME_TYPE_VALUE_DELIMITER)
          .append(fieldType);
      return sb.toString();
    }
  }

  public TextRecordSpec(String recordSpecString) {
    this(recordSpecString, Constants.DEFAULT_TEXT_ENCODING,
        Constants.DEFAULT_FIELD_DELIMITER);
  }

  public TextRecordSpec(String recordSpecString, String encoding,
      String delimiterBetweenFields) {
    this(recordSpecString, encoding, delimiterBetweenFields,
        Constants.DEFAULT_FIELD_NAME_TYPE_VALUE_DELIMITER);
  }

  public TextRecordSpec(String recordSpecString, String encoding,
      String fieldDelimiter, String fieldNameTypeValueDelimiter) {
    this.recordSpecString = recordSpecString;
    this.encoding = encoding;
    this.fieldDelimiter = fieldDelimiter.equals("\\001") ? "\001"
        : fieldDelimiter;
    this.fieldNameTypeValueDelimiter = fieldNameTypeValueDelimiter;
    this.escapedFieldDelimiter = ExpressionUtils
        .getEscapedDelimiter(fieldDelimiter);
    this.escapedFieldNameTypeValueDelimiter = ExpressionUtils
        .getEscapedDelimiter(fieldNameTypeValueDelimiter);
    initializeFieldSpecs();
  }

  public String getFieldDelimiter() {
    return fieldDelimiter;
  }

  public String getFieldNameTypeValueDelimiter() {
    return fieldNameTypeValueDelimiter;
  }

  public String getEscapedFieldDelimiter() {
    return escapedFieldDelimiter;
  }

  public String getEscapedFieldNameTypeValueDelimiter() {
    return escapedFieldNameTypeValueDelimiter;
  }

  public String getRecordSpecString() {
    return recordSpecString;
  }

  public String getEncoding() {
    return encoding;
  }

  public void setFieldList(List<FieldSpec> fieldList) {
    this.fieldList = fieldList;
  }

  public List<FieldSpec> getFieldList() {
    return fieldList;
  }

  public Map<String, FieldSpec> getFieldMap() {
    return fieldMap;
  }

  public String toString() {
    return fieldList.toString();
  }

  private void initializeFieldSpecs() {
    String normalized = StringUtils.deleteWhitespace(recordSpecString);
    // handling hive ^A (\001) as delimiter
    normalized = StringUtils.replace(normalized, "\\001", "\001");

    String[] fieldSpecs = StringUtils.splitByWholeSeparatorPreserveAllTokens(
        normalized, fieldDelimiter);

    int index = -1;
    FieldSpec field = null;
    for (int i = 0; i < fieldSpecs.length; i++) {
      index++;
      // TODO add escaped characters handling for keyvalue
      String[] ss = StringUtils.splitByWholeSeparatorPreserveAllTokens(
          fieldSpecs[i], fieldNameTypeValueDelimiter);
      if (ss.length == 1) {
        field = new FieldSpec(index, ss[0],
            Constants.HDFS_SOURCE_FILE_RECORD_DEFAULT_FIELD_TYPE);
      } else if (ss.length == 2) {
        // convert [f1:STRING, f2:STRING, f3:INT] to
        // [0:f1:STRING, 1:f2:STRING, 2:f3:INT]
        field = new FieldSpec(index, ss[0], ss[1]);
      }
      fieldList.add(field);
      fieldMap.put(ss[0], field);
    }
  }
}
