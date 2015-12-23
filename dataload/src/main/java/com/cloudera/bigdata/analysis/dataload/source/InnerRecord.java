package com.cloudera.bigdata.analysis.dataload.source;

import java.io.IOException;
import java.util.HashMap;
import java.util.Map;

import org.apache.hadoop.hbase.client.Put;
import org.apache.hadoop.hbase.client.Row;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import com.cloudera.bigdata.analysis.dataload.util.CommonUtils;

public class InnerRecord extends Record {
  private static final Logger LOG = LoggerFactory.getLogger(InnerRecord.class);

  private HTableDefinition tableDefinition;
  private byte[] rowKey;
  private boolean writeToWAL;
  private Map<byte[], Map<byte[], byte[]>> valueMap;

  public InnerRecord(HTableDefinition tableDefinition, byte[] rowKey,
      Map<byte[], Map<byte[], byte[]>> valueMap, boolean writeToWAL) {
    this.tableDefinition = tableDefinition;
    this.rowKey = rowKey;
    this.writeToWAL = writeToWAL;
    this.valueMap = valueMap;
  }

  @Override
  public void preConvert(int workerId) throws IOException {
    // TODO Auto-generated method stub
  }

  @Override
  public Map<HTableDefinition, Row[]> convertToHBaseRecord() throws IOException {
    Map<HTableDefinition, Row[]> rowMap = new HashMap<HTableDefinition, Row[]>();
    Row[] putArray = new Row[1];
    Put put = new Put(rowKey);
    LOG.debug("put rowKey: " + CommonUtils.convertByteArrayToString(rowKey));
    for (Map.Entry<byte[], Map<byte[], byte[]>> familyEntry : valueMap
        .entrySet()) {
      Map<byte[], byte[]> qualMap = familyEntry.getValue();
      for (Map.Entry<byte[], byte[]> qualEntry : qualMap.entrySet()) {
        if (LOG.isDebugEnabled()) {
          LOG.debug("in converToHBaseRecord: family: "
              + CommonUtils.convertByteArrayToString(familyEntry.getKey())
              + " qulifier: "
              + CommonUtils.convertByteArrayToString(qualEntry.getKey())
              + " value: "
              + CommonUtils.convertByteArrayToString(qualEntry.getValue()));
        }
        put.add(familyEntry.getKey(), qualEntry.getKey(), qualEntry.getValue());
        if (LOG.isDebugEnabled()) {
          LOG.debug("in converToHBaseRecord: after put.add");
        }
      }
    }
    put.setWriteToWAL(writeToWAL);
    putArray[0] = put;
    if (tableDefinition == null) {
      LOG.info("in converToHBaseRecord: tableDefinition is null");
    }

    rowMap.put(tableDefinition, putArray);

    return rowMap;
  }

  @Override
  public void finishUpdate() throws IOException {
    // TODO Auto-generated method stub

  }

}
