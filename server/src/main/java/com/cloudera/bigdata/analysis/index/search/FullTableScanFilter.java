package com.cloudera.bigdata.analysis.index.search;

import java.io.DataInput;
import java.io.DataOutput;
import java.io.IOException;
import java.util.HashMap;
import java.util.Iterator;
import java.util.List;
import java.util.Map;
import java.util.Set;

import org.apache.hadoop.hbase.KeyValue;
import org.apache.hadoop.hbase.exceptions.DeserializationException;
import org.apache.hadoop.hbase.filter.FilterBase;
import org.apache.hadoop.hbase.util.Bytes;
import org.apache.hadoop.hdfs.util.ByteArray;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import com.cloudera.bigdata.analysis.index.Constants;
import com.cloudera.bigdata.analysis.index.protobuf.ProtoUtil;
import com.cloudera.bigdata.analysis.index.protobuf.generated.FilterProto;
import com.cloudera.bigdata.analysis.index.util.WritableUtil;
import com.cloudera.bigdata.analysis.query.AndCondition;
import com.cloudera.bigdata.analysis.query.Condition;
import com.cloudera.bigdata.analysis.query.Operator;
import com.cloudera.bigdata.analysis.query.OrCondition;
import com.cloudera.bigdata.analysis.query.Query;
import com.cloudera.bigdata.analysis.query.SimpleCondition;
import com.google.protobuf.InvalidProtocolBufferException;

/**
 * A filter used to full-table scan search. It filters rows based on an
 * {@link com.cloudera.bigdata.analysis.query.Query} object, the filter rule is
 * whether given row match conditions of query object.
 * 
 * The filter also need the resolved
 * {@link com.cloudera.bigdata.analysis.index.search.IndexRangeSet} to verify
 * whether current row match condition!!
 */
public class FullTableScanFilter extends FilterBase {

  private static final Logger LOG = LoggerFactory
      .getLogger(FullTableScanFilter.class);

  private Query query;

  private Map<ByteArray, Set<ByteArray>> qualifiersToMatch;

  private Map<Condition, String> likeConditionRegexMap = new HashMap<Condition, String>();

  private Boolean rowMatched;

  public FullTableScanFilter() {
    super();
  }

  public FullTableScanFilter(Query query) {
    setQuery(query);
  }

  /*-------------------------------------------   Major Logic Methods   --------------------------------------------*/

  /**
   * to make method: void filterRow(List<KeyValue> keyValueList) called, this
   * method has always to return true.
   * 
   * @return
   */
  @Override
  public boolean hasFilterRow() {
    return true;
  }

  /**
   * Check a row of record whether match query condition, if yes, return true,
   * and this row won't filtered, otherwise, return false, filter it out.
   * 
   * @param keyValueList
   *          the key value list of a row.
   */
  @Override
  public void filterRow(List<KeyValue> keyValueList) {
    Condition condition = query.getCondition();
    if (match(condition, keyValueList)) {
      rowMatched = true;
    } else {
      rowMatched = false;
    }
  }

  @Override
  public boolean filterRow() {
    return !rowMatched;
  }

  /**
   * Check whether corresponding qualifier's value match given condition.
   * 
   * @param condition
   *          the condition to match
   * @return true if matched.
   */
  private boolean match(Condition condition, List<KeyValue> keyValueList) {
    boolean matched = false;
    if (condition instanceof SimpleCondition) {
      SimpleCondition simpleCondition = (SimpleCondition) condition;
      byte[] columnFamily = simpleCondition.getColumnFamily().getBytes();
      byte[] qualifier = simpleCondition.getQualifier().getBytes();
      byte[] conditionValue = simpleCondition.getValue().getBytes();
      byte[] recordValue = getValue(keyValueList, columnFamily, qualifier);
      Operator operator = simpleCondition.getOperator();
      switch (operator) {
      case EQ:
        matched = Bytes.compareTo(recordValue, conditionValue) == 0;
        break;
      case GT:
        matched = Bytes.compareTo(recordValue, conditionValue) > 0;
        break;
      case GTE:
        matched = Bytes.compareTo(recordValue, conditionValue) >= 0;
        break;
      case LT:
        matched = Bytes.compareTo(recordValue, conditionValue) < 0;
        break;
      case LTE:
        matched = Bytes.compareTo(recordValue, conditionValue) <= 0;
        break;
      case NEQ:
        matched = Bytes.compareTo(recordValue, conditionValue) != 0;
        break;
      case LIKE:
        // For like operation, the operand has to be String value!!
        // if doesn't contain, it means first resolve this condition, calculate
        // regex value
        // and cache it so as not to calculate every time.
        if (!likeConditionRegexMap.containsKey(condition)) {
          String conditionRegex = Bytes
              .toString(conditionValue)
              .replaceAll(Constants.S_SINGLE_WILDCARD,
                  Constants.S_SINGLE_WILDCARD_PATTERN)
              .replaceAll(Constants.S_MULTIPLE_WILDCARD,
                  Constants.S_MULTIPLE_WILDCARD_PATTERN);
          likeConditionRegexMap.put(condition, conditionRegex);
        }
        String conditionRegex = likeConditionRegexMap.get(condition);
        matched = Bytes.toString(recordValue).matches(conditionRegex);
        break;
      }
    } else if (condition instanceof OrCondition) {
      OrCondition orCondition = (OrCondition) condition;
      final Set<Condition> childConditions = orCondition.getChildren();
      final Iterator<Condition> iterator = childConditions.iterator();
      // for CompositeConditions, there are 2 children at least!
      matched = (match(iterator.next(), keyValueList));
      // For OR composite conditions:
      // only when current child condition does NOT match, we then need check
      // next, otherwise return true directly
      // only when all child conditions do NOT match, the final result is NOT
      // matched!
      // this is the same to || operation in java.
      while (!matched && iterator.hasNext()) {
        matched = match(iterator.next(), keyValueList);
      }
    } else if (condition instanceof AndCondition) {
      AndCondition andCondition = (AndCondition) condition;
      final Set<Condition> childConditions = andCondition.getChildren();
      final Iterator<Condition> iterator = childConditions.iterator();
      // for CompositeConditions, there are 2 children at least!
      matched = (match(iterator.next(), keyValueList));
      // For AND composite conditions:
      // only when current child condition match, we then need check next,
      // otherwise return false directly
      // only when all child conditions match, the final result is matched!
      // this is the same to && operation in java.
      while (matched && iterator.hasNext()) {
        matched = match(iterator.next(), keyValueList);
      }
    }
    return matched;
  }

  private byte[] getValue(List<KeyValue> keyValueList, byte[] columnFamily,
      byte[] qualifier) {
    for (KeyValue keyValue : keyValueList) {
      if (Bytes.equals(columnFamily, keyValue.getFamily())
          && Bytes.equals(qualifier, keyValue.getQualifier())) {
        return keyValue.getValue();
      }
    }
    throw new RuntimeException("Invalid column family or qualifier!");
  }

  /*--------------------------------------------     Common Methods    ---------------------------------------------*/

  public void write(DataOutput dataOutput) throws IOException {
    WritableUtil.writeInstance(dataOutput, query);

  }

  public void readFields(DataInput dataInput) throws IOException {
    setQuery(WritableUtil.readInstance(dataInput, Query.class));
  }

  /**
   * @return The filter serialized using pb
   */
  public byte[] toByteArray() {
    FilterProto.FulltableScanFilter.Builder builder = FilterProto.FulltableScanFilter
        .newBuilder();

    if (this.query != null)
      builder.setQuery(ProtoUtil.toQueryProto(query));
    return builder.build().toByteArray();
  }

  /**
   * @param pbBytes
   *          A pb serialized {@link FullTableScanFilter} instance
   * @return An instance of {@link FullTableScanFilter} made from
   *         <code>bytes</code>
   * @throws org.apache.hadoop.hbase.exceptions.DeserializationException
   * @see #toByteArray
   */
  public static FullTableScanFilter parseFrom(final byte[] pbBytes)
      throws DeserializationException {
    FilterProto.FulltableScanFilter proto;
    try {
      proto = FilterProto.FulltableScanFilter.parseFrom(pbBytes);
    } catch (InvalidProtocolBufferException e) {
      throw new DeserializationException(e);
    }
    return new FullTableScanFilter(proto.hasQuery() ? ProtoUtil.toQuery(proto
        .getQuery()) : null);
  }

  /*--------------------------------------------    Getters/Setters    ---------------------------------------------*/

  public Query getQuery() {
    return query;
  }

  public void setQuery(Query query) {
    this.query = query;
    this.qualifiersToMatch = query.getQualifiers();
  }

  public boolean isRowMatched() {
    return rowMatched;
  }
}
