package com.cloudera.bigdata.analysis.dataload.transform;

import java.io.UnsupportedEncodingException;
import java.util.HashMap;
import java.util.Map;

import org.apache.commons.lang.StringUtils;
import org.apache.hadoop.hbase.Cell;
import org.apache.hadoop.hbase.CellUtil;
import org.apache.hadoop.hbase.client.Result;
import org.apache.hadoop.hbase.util.Bytes;

import com.cloudera.bigdata.analysis.dataload.Constants;
import com.cloudera.bigdata.analysis.dataload.exception.ETLException;

public class ParsedResult {
  public byte[] rowkey;
  private Map<String, String> fieldMap = new HashMap<String, String>();

  public ParsedResult(Result result, Map<String, String> cellMap)
      throws ETLException, UnsupportedEncodingException {
    if (result.isEmpty())
      throw new ETLException("Empty result is found!");
    rowkey = result.getRow();
    this.fieldMap.put(Constants.ROW_KEY_NAME, Bytes.toString(rowkey));

    for (Map.Entry<String, String> e : cellMap.entrySet()) {
      Cell cell = result.getColumnLatestCell(e.getValue().getBytes(), e
          .getKey().getBytes());
      // if (!kv.nonNullRowAndColumn())
      // throw new ETLException("KeyValue is null: " + e.getKey() + "-"
      // + e.getValue());
      StringBuffer sb = new StringBuffer();
      sb.append(e.getValue())
          .append(Constants.FAMILY_QUALIFIER_SPLIT_CHARACTER)
          .append(e.getKey());
      this.fieldMap.put(sb.toString(), StringUtils.defaultIfEmpty(new String(
          CellUtil.cloneValue(cell), "utf-8"), ""));
    }
  }

  public Map<String, String> getFieldMap() {
    return this.fieldMap;
  }
}
