package com.cloudera.bigdata.analysis.index.search;

import java.io.ByteArrayInputStream;
import java.io.ByteArrayOutputStream;
import java.io.DataInput;
import java.io.DataInputStream;
import java.io.DataOutput;
import java.io.DataOutputStream;
import java.io.IOException;
import java.util.ArrayList;
import java.util.List;

import org.apache.hadoop.hbase.Cell;
import org.apache.hadoop.hbase.exceptions.DeserializationException;
import org.apache.hadoop.hbase.filter.Filter;
import org.apache.hadoop.hbase.filter.FilterBase;
import org.apache.hadoop.hbase.util.Bytes;
import org.apache.hadoop.hdfs.util.ByteArray;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import com.cloudera.bigdata.analysis.index.Constants;
import com.cloudera.bigdata.analysis.index.Index;
import com.cloudera.bigdata.analysis.index.protobuf.generated.FilterProto;
import com.cloudera.bigdata.analysis.index.util.ByteArrayUtils;
import com.cloudera.bigdata.analysis.index.util.WritableUtil;
import com.google.protobuf.HBaseZeroCopyByteString;
import com.google.protobuf.InvalidProtocolBufferException;

/**
 * When a given index range is not lcne (left-continuous-none-empty),
 * {@link com.cloudera.bigdata.analysis.index.search.IndexBasedSearchStrategy} will
 * use this filter to filter each row, make sure whether each index entry should
 * include or exclude from final result set.
 */
public class IndexScanFilter extends FilterBase {

  public static Logger LOG = LoggerFactory.getLogger(IndexScanFilter.class
      .getName());

  private List<Index.Field> indexFields;

  private IndexRange indexRange;

  private int scannedCount = 0;

  public IndexScanFilter() {
  }

  public IndexScanFilter(List<Index.Field> indexFields, IndexRange indexRange) {
    this.indexFields = indexFields;
    this.indexRange = indexRange;
  }

  /*-------------------------------------------   Major Logic Methods   --------------------------------------------*/

  /**
   * Get the key part from index entry row key, Start from first compare with
   * start/end boundary of index range, Check whether current row key is in the
   * range, if not, filter it!
   */
  @Override
  public boolean filterRowKey(byte[] buffer, int offset, int length) {
    if (buffer == null) {
      return true;
    }
    if (Constants.ENABLE_STATISTICS) {
      scannedCount++;
    }
    IndexRange.StartBoundary startBoundary = indexRange.getStartBoundary();
    IndexRange.Boundary.LcneFeature startBoundaryLcneFeature = startBoundary
        .getLcneFeature();
    IndexRange.EndBoundary endBoundary = indexRange.getEndBoundary();
    IndexRange.Boundary.LcneFeature endBoundaryLcneFeature = endBoundary
        .getLcneFeature();
    int endFieldIndexToCompare = Math.max(
        startBoundaryLcneFeature.getLastNeFieldIndex(),
        endBoundaryLcneFeature.getLastNeFieldIndex());
    int i = 0;
    for (Index.Field field : indexFields) {
      if (i <= endFieldIndexToCompare) {
        boolean greaterThanOrEqualToStartBoundary = false;
        boolean lessThanOrEqualToEndBoundary = false;
        ByteArray fieldValueOfStartBoundary = startBoundary.get(field);
        ByteArray fieldValueOfEndBoundary = endBoundary.get(field);
        if (ByteArrayUtils.isEmpty(fieldValueOfStartBoundary)) {
          greaterThanOrEqualToStartBoundary = true;
        } else {
          int cmpWithStartBoundary = Bytes.compareTo(buffer,
              offset + field.getStartPositionInRowKey(), field.getMaxLength(),
              fieldValueOfStartBoundary.getBytes(), 0,
              fieldValueOfStartBoundary.getBytes().length);
          if (cmpWithStartBoundary >= 0) {
            greaterThanOrEqualToStartBoundary = true;
          }
        }
        if (ByteArrayUtils.isEmpty(fieldValueOfEndBoundary)) {
          lessThanOrEqualToEndBoundary = true;
        } else {
          int cmpWithEndBoundary = Bytes.compareTo(buffer,
              offset + field.getStartPositionInRowKey(), field.getMaxLength(),
              fieldValueOfEndBoundary.getBytes(), 0,
              fieldValueOfEndBoundary.getBytes().length);
          if (cmpWithEndBoundary <= 0) {
            lessThanOrEqualToEndBoundary = true;
          }
        }
        if (!greaterThanOrEqualToStartBoundary || !lessThanOrEqualToEndBoundary) {
          return true;
        }
      } else {
        break;
      }
      i++;
    }
    return false;
  }

  @Override
  public ReturnCode filterKeyValue(Cell v) throws IOException {
    return null;
  }

  /*--------------------------------------------     Common Methods    ---------------------------------------------*/
  public void write(DataOutput dataOutput) throws IOException {
    dataOutput.writeInt(scannedCount);
    dataOutput.writeInt(indexFields.size());
    for (Index.Field indexField : indexFields) {
      WritableUtil.writeInstance(dataOutput, indexField);
    }
    WritableUtil.writeInstance(dataOutput, indexRange);
  }

  public void readFields(DataInput dataInput) throws IOException {
    scannedCount = dataInput.readInt();
    int fieldsCount = dataInput.readInt();
    indexFields = new ArrayList<Index.Field>(fieldsCount);
    for (int i = 0; i < fieldsCount; i++) {
      indexFields.add(WritableUtil.readInstance(dataInput, Index.Field.class));
    }
    indexRange = WritableUtil.readInstance(dataInput, IndexRange.class);
  }

  /**
   * @return The filter serialized using pb
   */
  public byte[] toByteArray() {
    FilterProto.IndexScanFilter.Builder builder = FilterProto.IndexScanFilter
        .newBuilder();
    ByteArrayOutputStream bas = new ByteArrayOutputStream();
    DataOutputStream dos;
    try {
      dos = new DataOutputStream(bas);
      write(dos);
      builder.setData(HBaseZeroCopyByteString.wrap(bas.toByteArray()));
      dos.close();
      bas.close();
    } catch (IOException e) {
      LOG.error(e.getMessage());
    }
    return builder.build().toByteArray();
  }

  /**
   * @param pbBytes
   *          A pb serialized {@link IndexScanFilter} instance
   * @return An instance of {@link IndexScanFilter} made from <code>bytes</code>
   * @throws org.apache.hadoop.hbase.exceptions.DeserializationException
   * @see #toByteArray
   */
  public static IndexScanFilter parseFrom(final byte[] pbBytes)
      throws DeserializationException {
    try {
      FilterProto.IndexScanFilter proto = FilterProto.IndexScanFilter
          .parseFrom(pbBytes);
      IndexScanFilter filter = new IndexScanFilter();
      ByteArrayInputStream bis = new ByteArrayInputStream(proto.getData()
          .toByteArray());
      DataInputStream dis;
      dis = new DataInputStream(bis);
      filter.readFields(dis);
      dis.close();
      bis.close();
      return filter;
    } catch (InvalidProtocolBufferException e) {
      throw new DeserializationException(e);
    } catch (IOException e) {
      throw new DeserializationException(e);
    }
  }

  /**
   * corresponding to write()
   * 
   * @return The filter serialized using pb
   */
  /*
   * public byte[] toByteArray() { IndexScanFilterProtos.IndexScanFilter.Builder
   * builder = IndexScanFilterProtos.IndexScanFilter .newBuilder(); if
   * (this.indexFields != null) { for (Index.Field indexField :
   * this.indexFields) { IndexScanFilterProtos.IndexField.Builder ifBuilder =
   * IndexScanFilterProtos.IndexField .newBuilder();
   * ifBuilder.setColumnFamily(HBaseZeroCopyByteString.wrap(indexField
   * .getColumnFamily().getBytes()));
   * ifBuilder.setQualifier(HBaseZeroCopyByteString.wrap(indexField
   * .getQualifier().getBytes()));
   * ifBuilder.setMaxLength(indexField.getMaxLength());
   * ifBuilder.setIsLengthVariable(indexField.isLengthVariable());
   * ifBuilder.setIsOrderInverted(indexField.isOrderInverted());
   * ifBuilder.setStartPositionInRowKey(indexField .getStartPositionInRowKey());
   * ifBuilder.setFormatter(ProtobufUtil.toFormatter(indexField
   * .getFormatter())); builder.addIndexFields(ifBuilder); } } if
   * (this.indexRange != null) { IndexScanFilterProtos.IndexRange.Builder
   * irBuilder = IndexScanFilterProtos.IndexRange .newBuilder();
   * irBuilder.setStartBoundary(ProtobufUtil.toStartBoundery(indexRange
   * .getStartBoundary()));
   * irBuilder.setEndBoundary(ProtobufUtil.toEndBoundery(indexRange
   * .getEndBoundary())); builder.setIndexRange(irBuilder); }
   * builder.setScannedCount(this.scannedCount);
   * 
   * return builder.build().toByteArray(); }
   */

  /**
   * corresponding to readFields()
   * 
   * @param pbBytes
   *          A pb serialized {@link PrefixFilter} instance
   * @return An instance of {@link PrefixFilter} made from <code>bytes</code>
   * @throws org.apache.hadoop.hbase.exceptions.DeserializationException
   * @see #toByteArray
   */
  /*
   * public static IndexScanFilter parseFrom(final byte[] pbBytes) throws
   * DeserializationException { IndexScanFilterProtos.IndexScanFilter proto; try
   * { proto = IndexScanFilterProtos.IndexScanFilter.parseFrom(pbBytes); } catch
   * (InvalidProtocolBufferException e) { throw new DeserializationException(e);
   * } int count = proto.getIndexFieldsCount(); ArrayList<Index.Field>
   * indexFields = new ArrayList<Index.Field>(count); for (int i = 0; i < count;
   * ++i) { IndexField current = proto.getIndexFields(i); ByteArray columnFamily
   * = new ByteArray(current.getColumnFamily() .toByteArray()); ByteArray
   * qualifier = new ByteArray(current.getQualifier().toByteArray()); int
   * maxLength = current.getMaxLength(); boolean isLengthVariable =
   * current.getIsLengthVariable(); boolean isOrderInverted =
   * current.getIsOrderInverted(); int startPositionInRowKey =
   * current.getStartPositionInRowKey(); IndexFieldValueFormatter formatter =
   * ProtobufUtil.toFormatter(current .getFormatter()); indexFields.add(new
   * Index.Field(columnFamily, qualifier, maxLength, isLengthVariable,
   * isOrderInverted, startPositionInRowKey, formatter)); }
   * 
   * IndexScanFilterProtos.IndexRange current = proto.getIndexRange();
   * StartBoundary startBoundary = ProtobufUtil.toFormatter(current
   * .getStartBoundary()); EndBoundary endBoundary = ProtobufUtil
   * .toFormatter(current.getEndBoundary()); return new
   * IndexScanFilter(indexFields, new IndexRange(startBoundary, endBoundary)); }
   */

  /*--------------------------------------------    Getters/Setters    ---------------------------------------------*/

  public int getScannedCount() {
    return scannedCount;
  }
}
