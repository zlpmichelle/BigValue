package com.cloudera.bigdata.analysis.index.search;

import java.io.DataInput;
import java.io.DataOutput;
import java.io.IOException;
import java.util.ArrayList;
import java.util.Arrays;
import java.util.Iterator;
import java.util.LinkedHashMap;
import java.util.List;
import java.util.Map;
import java.util.Set;

import org.apache.commons.lang.ArrayUtils;
import org.apache.hadoop.hbase.util.Bytes;
import org.apache.hadoop.hdfs.util.ByteArray;
import org.apache.hadoop.io.Writable;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import com.cloudera.bigdata.analysis.index.Constants;
import com.cloudera.bigdata.analysis.index.Index;
import com.cloudera.bigdata.analysis.index.util.ByteArrayUtils;
import com.cloudera.bigdata.analysis.index.util.WritableUtil;
import com.cloudera.bigdata.analysis.query.InvalidQueryException;

/**
 * <p>
 * It stands for a row key range of index entries,
 * {@link com.cloudera.bigdata.analysis.index.search.IndexBasedSearchStrategy} will
 * use a set of index range to retrieve records directly. It's required to
 * override equals and hash code method, they are used to exclude duplicated
 * ranges.
 * </p>
 * <p>
 * this class implement {@link java.lang.Comparable}, it's ordered in
 * {@link com.cloudera.bigdata.analysis.index.search.IndexRangeSet} and note that:
 * the order of index range will follow the order they present in index
 * entries!!
 * </p>
 * <p>
 * there might be duplicated ranges after resolving, i.e. duplicated conditions
 * will spawn 2 the same ranges, so, this class and start/end boundary must
 * implement equals and hashCode method strictly!
 * </p>
 * 
 */
public class IndexRange implements
    com.cloudera.bigdata.analysis.index.util.Cloneable<IndexRange>,
    Comparable<IndexRange>, Writable {

  private static final Logger LOG = LoggerFactory.getLogger(IndexRange.class);

  private StartBoundary startBoundary;

  private EndBoundary endBoundary;

  public IndexRange() {
  }

  public IndexRange(StartBoundary startBoundary, EndBoundary endBoundary) {
    this.startBoundary = startBoundary;
    this.endBoundary = endBoundary;
  }

  /*-------------------------------------------   Major Logic Methods   --------------------------------------------*/

  /**
   * Check whether the given two index ranges are intersected on specified
   * column family and qualifier.
   */
  public static boolean isIntersected(Index.Field field,
      IndexRange thisIndexRange, IndexRange thatIndexRange) {
    ByteArray thisStartValue = thisIndexRange.getStartBoundary().get(field);
    ByteArray thisEndValue = thisIndexRange.getEndBoundary().get(field);
    if (ByteArrayUtils.isEmpty(thisStartValue)
        && ByteArrayUtils.isEmpty(thisEndValue)) {
      return true;
    } else if (ByteArrayUtils.isEmpty(thisStartValue)
        && ByteArrayUtils.isNotEmpty(thisEndValue)) {
      return thatIndexRange.includes(field, thisEndValue);
    } else if (ByteArrayUtils.isNotEmpty(thisStartValue)
        && ByteArrayUtils.isEmpty(thisEndValue)) {
      return thatIndexRange.includes(field, thisStartValue);
    } else {
      return thatIndexRange.includes(field, thisStartValue)
          || thatIndexRange.includes(field, thisEndValue);
    }
  }

  /**
   * Do "and" operation with given two index ranges.
   * 
   * @param thisIndexRange
   *          first index range
   * @param thatIndexRange
   *          second index range
   * @return the result index range after "and" operation.
   */
  public static IndexRange and(IndexRange thisIndexRange,
      IndexRange thatIndexRange) {
    final List<Index.Field> fields = thisIndexRange.getStartBoundary()
        .getFields();
    for (Index.Field field : fields) {
      if (!isIntersected(field, thisIndexRange, thatIndexRange)) {
        throw new InvalidQueryException("IndexRange: " + thisIndexRange
            + " and IndexRange: " + thatIndexRange
            + " doesn't intersect, they can not do AND calculation!");
      }
    }
    StartBoundary andMergedStartRowKey = StartBoundary.and(
        thisIndexRange.getStartBoundary(), thatIndexRange.getStartBoundary());
    EndBoundary andMergedEndRowKey = EndBoundary.and(
        thisIndexRange.getEndBoundary(), thatIndexRange.getEndBoundary());
    IndexRange andMergedIndexRange = new IndexRange(andMergedStartRowKey,
        andMergedEndRowKey);
    return andMergedIndexRange;
  }

  /**
   * Check whether this range includes given value of qualifier.
   */
  public boolean includes(Index.Field field, ByteArray value) {
    if (ByteArrayUtils.isEmpty(value)) {
      throw new RuntimeException("Can NOT check null value!");
    }
    ByteArray startValue = startBoundary.get(field);
    ByteArray endValue = endBoundary.get(field);
    // if this is full set, include everything.
    if (ByteArrayUtils.isEmpty(startValue) && ByteArrayUtils.isEmpty(endValue)) {
      return true;
    } else if (ByteArrayUtils.isEmpty(startValue)
        && ByteArrayUtils.isNotEmpty(endValue)) {
      return ByteArrayUtils.compare(value, endValue) <= 0;
    } else if (ByteArrayUtils.isNotEmpty(startValue)
        && ByteArrayUtils.isEmpty(endValue)) {
      return ByteArrayUtils.compare(value, startValue) >= 0;
    } else {
      return (ByteArrayUtils.compare(value, endValue) <= 0)
          && (ByteArrayUtils.compare(value, startValue) >= 0);
    }
  }

  public boolean includes(ByteArray indexKey) {
    // parse index key to qualifier-value map.
    return false;
  }

  /**
   * The definite boundary of an IndexRange means: for the fields of start key
   * and end key, NO empty value presents before a none-null value! This decides
   * whether this range can be used to locate index entries directly! If a
   * range's boundary is indefinite, it's useless, we can't afford to filter
   * index entries first, then fetch records by filtered index entries, because
   * from index entry to record, it is by "get" operation one by one.
   * <p/>
   * here are some boundary definite cases: null,null,null q1,q2,null
   * q1,null,null
   * <p/>
   * here are boundary indefinite cases: q1,null,q3 null,q2,q3
   * 
   * @return if has, return true;
   */
  public boolean isLcne() {
    return startBoundary.isLcne() && endBoundary.isLcne();
  }

  /*--------------------------------------------     Common Methods    ---------------------------------------------*/

  @Override
  public void write(DataOutput dataOutput) throws IOException {
    WritableUtil.writeInstance(dataOutput, startBoundary);
    WritableUtil.writeInstance(dataOutput, endBoundary);
  }

  @Override
  public void readFields(DataInput dataInput) throws IOException {
    startBoundary = WritableUtil.readInstance(dataInput, StartBoundary.class);
    endBoundary = WritableUtil.readInstance(dataInput, EndBoundary.class);
  }

  @Override
  public String toString() {
    return "IndexRange{" + "startBoundary=" + startBoundary + ", endBoundary="
        + endBoundary + '}';
  }

  @Override
  public boolean equals(Object o) {
    if (this == o)
      return true;
    if (o == null || getClass() != o.getClass())
      return false;
    IndexRange that = (IndexRange) o;
    if (!endBoundary.equals(that.endBoundary))
      return false;
    if (!startBoundary.equals(that.startBoundary))
      return false;
    return true;
  }

  @Override
  public int hashCode() {
    int result = startBoundary.hashCode();
    result = 31 * result + endBoundary.hashCode();
    return result;
  }

  @Override
  public IndexRange clone() {
    return new IndexRange(startBoundary.clone(), endBoundary.clone());
  }

  /**
   * For IndexRange comparison, who's start row key is at front, who places
   * front. NOTE: It's REQUIRED to compare end boundary!!! because many ranges
   * could have the same start boundary and be careful: if return 0, keep
   * consistent with equals method!
   */
  @Override
  public int compareTo(IndexRange anotherIndexRange) {
    int startBoundaryCmpResult = startBoundary.compareTo(anotherIndexRange
        .getStartBoundary());

    if (startBoundaryCmpResult != 0) {
      return startBoundaryCmpResult;
    } else {
      int endBoundaryCmpResult = endBoundary.compareTo(anotherIndexRange
          .getEndBoundary());
      return endBoundaryCmpResult;
    }
  }

  /*--------------------------------------------    Getters/Setters    ---------------------------------------------*/

  public StartBoundary getStartBoundary() {
    return startBoundary;
  }

  public void setStartBoundary(StartBoundary startBoundary) {
    this.startBoundary = startBoundary;
  }

  public EndBoundary getEndBoundary() {
    return endBoundary;
  }

  public void setEndBoundary(EndBoundary endBoundary) {
    this.endBoundary = endBoundary;
  }

  /*--------------------------------------------     Inner Classes    ----------------------------------------------*/

  public static class Boundary implements Writable {

    /**
     * the feature of LCNE, this field is very important to determine how to
     * search based on an index range!
     */
    protected LcneFeature lcneFeature;

    /**
     * Given fields are immutable!!
     */
    protected List<Index.Field> fields;

    /**
     * The fields-value pairs of row key, they keep the same order as the fields
     * in index.
     */
    protected Map<Index.Field, ByteArray> fieldValues;

    public Boundary() {
    }

    public Boundary(List<Index.Field> fields) {
      this.fields = fields;
      this.fieldValues = new LinkedHashMap<Index.Field, ByteArray>();
      for (Index.Field field : fields) {
        this.fieldValues.put(field, null);
      }
    }

    public boolean isLcne() {
      return getLcneFeature().getLastLcneFieldIndex() == getLcneFeature()
          .getLastNeFieldIndex();
    }

    public void set(Index.Field field, ByteArray value) {
      fieldValues.put(field, value);
    }

    public ByteArray get(Index.Field field) {
      return fieldValues.get(field);
    }

    public Index.Field getField(int position) {
      return fields.get(position);
    }

    private LcneFeature analyzeLcneFeature() {
      byte[] lcneFragment = null;
      // flag firstLoop is used to indicate whether insert delimiter when
      // building row key
      boolean firstLoop = true;
      boolean foundEmptyValueBefore = false;
      int i = 0;
      int lastNeQualifierPosition = -1;
      int lastLcneQualifierPosition = -1;
      // append left-continuous values to row key. don't worry about the value
      // order or what qualifier
      // it belongs to, because the value entry set are organized according to
      // index structure!!
      for (Map.Entry<Index.Field, ByteArray> fieldValueEntry : fieldValues
          .entrySet()) {
        Index.Field field = fieldValueEntry.getKey();
        ByteArray value = fieldValueEntry.getValue();
        if (ByteArrayUtils.isNotEmpty(value)) {
          lastNeQualifierPosition = i;
          if (!foundEmptyValueBefore) {
            lastLcneQualifierPosition = i;
            if (firstLoop) {
              lcneFragment = value.getBytes();
              firstLoop = false;
            } else {
              lcneFragment = Bytes.add(lcneFragment,
                  Constants.HBASE_TABLE_DELIMITER, value.getBytes());
            }
          }
        } else {
          foundEmptyValueBefore = true;
        }
        i++;
      }
      return new LcneFeature(lcneFragment, lastLcneQualifierPosition,
          lastNeQualifierPosition);
    }

    /*
     * Ignore lcneFeature, it's better to generate when used.
     */
    @Override
    public void write(DataOutput dataOutput) throws IOException {
      dataOutput.writeInt(fields.size());
      for (Index.Field field : fields) {
        WritableUtil.writeInstance(dataOutput, field);
      }
      dataOutput.writeInt(fieldValues.size());
      for (Map.Entry<Index.Field, ByteArray> fieldValueEntry : fieldValues
          .entrySet()) {
        WritableUtil.writeInstance(dataOutput, fieldValueEntry.getKey());
        Bytes.writeByteArray(dataOutput, fieldValueEntry.getValue().getBytes());
      }
    }

    @Override
    public void readFields(DataInput dataInput) throws IOException {
      int fieldsCount = dataInput.readInt();
      fields = new ArrayList<Index.Field>(fieldsCount);
      for (int i = 0; i < fieldsCount; i++) {
        fields.add(WritableUtil.readInstance(dataInput, Index.Field.class));
      }
      int fieldValuesCount = dataInput.readInt();
      fieldValues = new LinkedHashMap<Index.Field, ByteArray>(fieldValuesCount);
      for (int i = 0; i < fieldValuesCount; i++) {
        fieldValues.put(
            WritableUtil.readInstance(dataInput, Index.Field.class),
            new ByteArray(Bytes.readByteArray(dataInput)));
      }
    }

    @Override
    public String toString() {
      StringBuilder sb = new StringBuilder("{");
      for (Map.Entry<Index.Field, ByteArray> fieldValueEntry : fieldValues
          .entrySet()) {
        Index.Field field = fieldValueEntry.getKey();
        ByteArray value = fieldValueEntry.getValue();
        if (ByteArrayUtils.isNotEmpty(value)) {
          sb.append(Bytes.toStringBinary(value.getBytes()));
        } else {
          sb.append("null");
        }
        sb.append(",");
      }
      sb.append("}");
      return sb.toString();
    }

    public LcneFeature getLcneFeature() {
      if (lcneFeature == null) {
        lcneFeature = analyzeLcneFeature();
      }
      return lcneFeature;
    }

    public Map<Index.Field, ByteArray> getFieldValues() {
      return fieldValues;
    }

    public List<Index.Field> getFields() {
      return fields;
    }

    /**
     * LCNE means: Left-Continuous-None-Empty. This class is used to describe
     * the left-continuous-none-empty feature of an index range boundary. When
     * searching over an index range, it will indicate
     * {@link com.cloudera.bigdata.analysis.index.search.IndexBasedSearchStrategy}
     * to scan index with a
     * {@link com.cloudera.bigdata.analysis.index.search.IndexScanFilter} or locate
     * records directly!!
     */
    public static class LcneFeature {
      /**
       * a byte array organized with left-continuous-none-empty fields. the
       * format follows the key part of index entry
       */
      private byte[] lcneFragment;
      /** last left-continuous-none-empty qualifier position **/
      private int lastLcneFieldIndex;
      /** last none-empty qualifier position **/
      private int lastNeFieldIndex;

      public LcneFeature(byte[] lcneFragment, int lastLcneFieldIndex,
          int lastNeFieldIndex) {
        this.lcneFragment = lcneFragment;
        this.lastLcneFieldIndex = lastLcneFieldIndex;
        this.lastNeFieldIndex = lastNeFieldIndex;
      }

      @Override
      public String toString() {
        return "LcneFeature{" + "lcneFragment=" + Arrays.toString(lcneFragment)
            + ", lastLcneFieldIndex=" + lastLcneFieldIndex
            + ", lastNeFieldIndex=" + lastNeFieldIndex + '}';
      }

      public byte[] getLcneFragment() {
        return lcneFragment;
      }

      public int getLastLcneFieldIndex() {
        return lastLcneFieldIndex;
      }

      public int getLastNeFieldIndex() {
        return lastNeFieldIndex;
      }
    }
  }

  public static class StartBoundary extends Boundary implements
      com.cloudera.bigdata.analysis.index.util.Cloneable<StartBoundary>,
      Comparable<StartBoundary>, Writable {

    public StartBoundary() {
    }

    public StartBoundary(List<Index.Field> fields) {
      super(fields);
    }

    public static StartBoundary and(StartBoundary leftRowKey,
        StartBoundary rightRowKey) {
      StartBoundary resolvedStartRowKey = leftRowKey.clone();
      final List<Index.Field> fields = resolvedStartRowKey.getFields();
      for (Index.Field field : fields) {
        ByteArray leftValue = resolvedStartRowKey.get(field);
        ByteArray rightValue = rightRowKey.get(field);
        if (ByteArrayUtils.isNotEmpty(leftValue)
            && ByteArrayUtils.isNotEmpty(rightValue)) {
          if (ByteArrayUtils.compare(rightValue, leftValue) > 0) {
            resolvedStartRowKey.set(field, rightValue);
          }
        } else if (ByteArrayUtils.isEmpty(leftValue)
            && ByteArrayUtils.isNotEmpty(rightValue)) {
          resolvedStartRowKey.set(field, rightValue);
        }
      }
      return resolvedStartRowKey;
    }

    @Override
    public boolean equals(Object o) {
      if (this == o)
        return true;
      if (o == null || getClass() != o.getClass())
        return false;
      StartBoundary boundary = (StartBoundary) o;

      if (fieldValues != null ? !fieldValues.equals(boundary.fieldValues)
          : boundary.fieldValues != null)
        return false;
      if (fieldValues.size() != boundary.fieldValues.size())
        return false;
      Set<Map.Entry<Index.Field, ByteArray>> thisEntrySet = fieldValues
          .entrySet();
      Iterator<Map.Entry<Index.Field, ByteArray>> thisEntrySetIterator = thisEntrySet
          .iterator();
      Set<Map.Entry<Index.Field, ByteArray>> thatEntrySet = boundary.fieldValues
          .entrySet();
      Iterator<Map.Entry<Index.Field, ByteArray>> thatEntrySetIterator = thatEntrySet
          .iterator();
      while (thisEntrySetIterator.hasNext()) {
        Map.Entry<Index.Field, ByteArray> thisEntry = thisEntrySetIterator
            .next();
        Map.Entry<Index.Field, ByteArray> thatEntry = thatEntrySetIterator
            .next();
        if (!thisEntry.getKey().equals(thatEntry.getKey())) {
          return false;
        }
        if (ByteArrayUtils.isEmpty(thisEntry.getValue())
            && ByteArrayUtils.isNotEmpty(thatEntry.getValue())) {
          return false;
        }
        if (ByteArrayUtils.isNotEmpty(thisEntry.getValue())
            && ByteArrayUtils.isEmpty(thatEntry.getValue())) {
          return false;
        }
        if (ByteArrayUtils.isNotEmpty(thisEntry.getValue())
            && ByteArrayUtils.isNotEmpty(thatEntry.getValue())
            && !thisEntry.getValue().equals(thatEntry.getValue())) {
          return false;
        }
      }
      return true;
    }

    @Override
    public int hashCode() {
      // the hash seed should be different with end boundary!!
      int result = 17;
      for (Map.Entry<Index.Field, ByteArray> entry : fieldValues.entrySet()) {
        result = 31 * result + entry.getKey().hashCode();
        if (entry.getValue() != null) {
          result = 31 * result + entry.getValue().hashCode();
        }
      }
      return result;
    }

    @Override
    public StartBoundary clone() {
      StartBoundary startRowKey = new StartBoundary(fields);
      for (Index.Field field : fields) {
        if (ByteArrayUtils.isNotEmpty(get(field))) {
          startRowKey.set(field,
              new ByteArray(ArrayUtils.clone(get(field).getBytes())));
        }
      }
      return startRowKey;
    }

    /**
     * Be careful, keep consistent with equals method, in other words, make sure
     * return 0 if e1.equals(e2)==true!
     */
    @Override
    public int compareTo(StartBoundary anotherStartRowKey) {
      for (Index.Field field : fields) {
        ByteArray thisValue = get(field);
        ByteArray thatValue = anotherStartRowKey.get(field);
        if (ByteArrayUtils.isNotEmpty(thisValue)
            && ByteArrayUtils.isNotEmpty(thatValue)) {
          int cmpResult = ByteArrayUtils.compare(thisValue, thatValue);
          if (cmpResult == 0) {
            continue;
          } else {
            return cmpResult;
          }
        } else if (ByteArrayUtils.isNotEmpty(thisValue)
            && ByteArrayUtils.isEmpty(thatValue)) {
          return 1;
        } else if (ByteArrayUtils.isEmpty(thisValue)
            && ByteArrayUtils.isNotEmpty(thatValue)) {
          return -1;
        } else {
          continue;
        }
      }
      return 0;
    }
  }

  public static class EndBoundary extends Boundary implements
      com.cloudera.bigdata.analysis.index.util.Cloneable<EndBoundary>,
      Comparable<EndBoundary>, Writable {

    public EndBoundary() {
    }

    public EndBoundary(List<Index.Field> fields) {
      super(fields);
    }

    public static EndBoundary and(EndBoundary leftRowKey,
        EndBoundary rightRowKey) {
      EndBoundary resolvedEndRowKey = leftRowKey.clone();
      final List<Index.Field> fields = resolvedEndRowKey.getFields();
      for (Index.Field field : fields) {
        ByteArray leftValue = resolvedEndRowKey.get(field);
        ByteArray rightValue = rightRowKey.get(field);
        if (ByteArrayUtils.isNotEmpty(leftValue)
            && ByteArrayUtils.isNotEmpty(rightValue)) {
          if (ByteArrayUtils.compare(rightValue, leftValue) < 0) {
            resolvedEndRowKey.set(field, rightValue);
          }
        } else if (ByteArrayUtils.isEmpty(leftValue)
            && ByteArrayUtils.isNotEmpty(rightValue)) {
          resolvedEndRowKey.set(field, rightValue);
        }
      }
      return resolvedEndRowKey;
    }

    @Override
    public boolean equals(Object o) {
      if (this == o)
        return true;
      if (o == null || getClass() != o.getClass())
        return false;
      EndBoundary boundary = (EndBoundary) o;
      if (fieldValues != null ? !fieldValues.equals(boundary.fieldValues)
          : boundary.fieldValues != null)
        return false;
      if (fieldValues.size() != boundary.fieldValues.size())
        return false;
      Set<Map.Entry<Index.Field, ByteArray>> thisEntrySet = fieldValues
          .entrySet();
      Iterator<Map.Entry<Index.Field, ByteArray>> thisEntrySetIterator = thisEntrySet
          .iterator();
      Set<Map.Entry<Index.Field, ByteArray>> thatEntrySet = boundary.fieldValues
          .entrySet();
      Iterator<Map.Entry<Index.Field, ByteArray>> thatEntrySetIterator = thatEntrySet
          .iterator();
      while (thisEntrySetIterator.hasNext()) {
        Map.Entry<Index.Field, ByteArray> thisEntry = thisEntrySetIterator
            .next();
        Map.Entry<Index.Field, ByteArray> thatEntry = thatEntrySetIterator
            .next();
        if (!thisEntry.getKey().equals(thatEntry.getKey())) {
          return false;
        }
        if (ByteArrayUtils.isEmpty(thisEntry.getValue())
            && ByteArrayUtils.isNotEmpty(thatEntry.getValue())) {
          return false;
        }
        if (ByteArrayUtils.isNotEmpty(thisEntry.getValue())
            && ByteArrayUtils.isEmpty(thatEntry.getValue())) {
          return false;
        }
        if (ByteArrayUtils.isNotEmpty(thisEntry.getValue())
            && ByteArrayUtils.isNotEmpty(thatEntry.getValue())
            && !thisEntry.getValue().equals(thatEntry.getValue())) {
          return false;
        }
      }
      return true;
    }

    @Override
    public int hashCode() {
      // the hash seed should be different with start boundary!!
      int result = 19;
      for (Map.Entry<Index.Field, ByteArray> entry : fieldValues.entrySet()) {
        result = 31 * result + entry.getKey().hashCode();
        if (entry.getValue() != null) {
          result = 31 * result + entry.getValue().hashCode();
        }
      }
      return result;
    }

    @Override
    public EndBoundary clone() {
      EndBoundary endRowKey = new EndBoundary(fields);
      for (Index.Field field : fields) {
        if (ByteArrayUtils.isNotEmpty(get(field))) {
          endRowKey.set(field,
              new ByteArray(ArrayUtils.clone(get(field).getBytes())));
        }
      }
      return endRowKey;
    }

    @Override
    public int compareTo(EndBoundary anotherEndRowKey) {
      for (Index.Field field : fields) {
        ByteArray thisValue = get(field);
        ByteArray thatValue = anotherEndRowKey.get(field);
        if (ByteArrayUtils.isNotEmpty(thisValue)
            && ByteArrayUtils.isNotEmpty(thatValue)) {
          int cmpResult = ByteArrayUtils.compare(thisValue, thatValue);
          if (cmpResult == 0) {
            continue;
          } else {
            return cmpResult;
          }
        } else if (ByteArrayUtils.isNotEmpty(thisValue)
            && ByteArrayUtils.isEmpty(thatValue)) {
          return -1;
        } else if (ByteArrayUtils.isEmpty(thisValue)
            && ByteArrayUtils.isNotEmpty(thatValue)) {
          return 1;
        } else {
          continue;
        }
      }
      return 0;
    }
  }

}
