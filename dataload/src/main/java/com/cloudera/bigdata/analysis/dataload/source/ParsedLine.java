package com.cloudera.bigdata.analysis.dataload.source;

import java.util.HashMap;
import java.util.List;
import java.util.Map;

import org.apache.commons.lang.StringUtils;

import com.cloudera.bigdata.analysis.dataload.Constants;
import com.cloudera.bigdata.analysis.dataload.exception.FormatException;
import com.cloudera.bigdata.analysis.dataload.source.TextRecordSpec.FieldSpec;

public class ParsedLine {

  private Map<String, String> fieldMap = new HashMap<String, String>();

  public ParsedLine(TextRecordSpec recordSpec, String rawRecord)
      throws FormatException {

    String[] fields = null;

    // handle '\t' delimiter, if there is a '\t' in file delimiter, will
    // construct escaped delimiter string which appends with "\\", because
    // split() needs to handle escaped character
    if (recordSpec.getFieldDelimiter().contains("\\t")
        || recordSpec.getFieldDelimiter().contains("\\n")
        || recordSpec.getFieldDelimiter().contains("\\b")
        || recordSpec.getFieldDelimiter().contains("\\f")
        || recordSpec.getFieldDelimiter().contains("\\r")) {
      fields = rawRecord.split(recordSpec.getEscapedFieldDelimiter());
      // TODO enhance to handle null field split manually
    } else {
      // splitByWholeSeparatorPreserveAllTokens() can not handling '\t', but it
      // is good at handling other escaped delimiter and null field split
      fields = StringUtils.splitByWholeSeparatorPreserveAllTokens(rawRecord,
          recordSpec.getFieldDelimiter());
    }

    int fieldNumber = recordSpec.getFieldList().size();

    if (fieldNumber != fields.length) {
      throw new FormatException("Invalid line: actual fields " + fields.length
          + " expected fields " + fieldNumber);
    }

    List<FieldSpec> fieldSpecList = recordSpec.getFieldList();

    Integer iInt = null;
    for (int i = 0; i < fieldNumber; i++) {
      fieldMap.put(fieldSpecList.get(i).getFieldName(), fields[i]);
      // check data type here
      if (fieldSpecList.get(i).getFieldType()
          .equals(Constants.HDFS_SOURCE_FILE_RECORD_INT_FIELD_TYPE)) {
        try {
          iInt = Integer.parseInt(fields[i]);
          if (iInt instanceof Integer) {
          }
        } catch (NumberFormatException e) {
          throw new FormatException("Invalid Type for field \""
              + fieldSpecList.get(i).getFieldName() + "\": actual value is \""
              + fields[i] + "\", expected type is \""
              + Constants.HDFS_SOURCE_FILE_RECORD_INT_FIELD_TYPE + "\"");
        }
      }
    }
  }

  public Map<String, String> getFieldMap() {
    return this.fieldMap;
  }
}
