package com.cloudera.bigdata.analysis.index;

import java.util.List;

import org.apache.hadoop.hbase.CellUtil;
import org.apache.hadoop.hbase.client.Put;
import org.apache.hadoop.hbase.util.Bytes;
import org.apache.hadoop.hdfs.util.ByteArray;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import com.cloudera.bigdata.analysis.index.util.IndexUtil;

/**
 * The builder to build index entry based on given index.
 */
public class IndexEntryBuilder {

  private static final Logger LOG = LoggerFactory
      .getLogger(IndexEntryBuilder.class);

  private Index index;

  public IndexEntryBuilder(Index index) {
    if (LOG.isDebugEnabled()) {
      LOG.debug("index:" + index.toString());
    }
    this.index = index;
  }

  public Put build(byte[] regionStartKey, Put put) {
    byte[] indexRowKey = Bytes.add(regionStartKey,
        Constants.IDX_ROWKEY_DELIMITER, index.getName().getBytes());
    indexRowKey = Bytes.add(indexRowKey, Constants.IDX_ROWKEY_DELIMITER,
        buildIndexKey(put));
    indexRowKey = Bytes.add(indexRowKey, Constants.IDX_ROWKEY_DELIMITER,
        put.getRow());
    if (LOG.isDebugEnabled()) {
      LOG.debug("index.name:"
          + IndexUtil.convertByteArrayToString(index.getName().getBytes()));
      LOG.debug("Build Index:" + Bytes.toStringBinary(indexRowKey));
    }
    Put indexPut = new Put(indexRowKey);
    indexPut.add(Constants.FAMILY_I, null, null);
    indexPut.setWriteToWAL(false);
    return indexPut;
  }

  private byte[] buildIndexKey(Put put) {
    byte[] indexKey = null;
    boolean firstLoop = true;
    final List<Index.Field> fields = index.getFields();
    for (Index.Field field : fields) {
      ByteArray columnFamily = field.getColumnFamily();
      ByteArray qualifier = field.getQualifier();
      byte[] cellValue = CellUtil.cloneValue(put.get(columnFamily.getBytes(),
          qualifier.getBytes()).get(0));

      byte[] formattedCellValue = field.getFormatter().format(field, cellValue);
      //
      if (firstLoop) {
        indexKey = formattedCellValue;
        firstLoop = false;
      } else {
        indexKey = Bytes.add(indexKey, Constants.HBASE_TABLE_DELIMITER,
            formattedCellValue);
      }
    }
    return indexKey;
  }

  @Override
  public String toString() {
    return "IndexEntryBuilder{" + "index=" + index + '}';
  }
}
