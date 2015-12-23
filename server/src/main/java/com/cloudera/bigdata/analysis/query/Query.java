package com.cloudera.bigdata.analysis.query;

import java.io.DataInput;
import java.io.DataOutput;
import java.io.IOException;
import java.util.Iterator;
import java.util.LinkedHashMap;
import java.util.LinkedHashSet;
import java.util.Map;
import java.util.Set;

import org.apache.hadoop.hbase.util.Bytes;
import org.apache.hadoop.hdfs.util.ByteArray;
import org.apache.hadoop.io.Writable;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import com.cloudera.bigdata.analysis.index.Constants;
import com.cloudera.bigdata.analysis.index.util.WritableUtil;

/**
 * A query object encapsulates all information of a query request coming from
 * users, i.e. what table, what condition, what order and result limit.
 */
public class Query implements Writable {

  private static final Logger LOG = LoggerFactory.getLogger(Query.class);

  private ByteArray table;

  private int resultsLimit;

  private Condition condition;

  private Map<ByteArray, Set<ByteArray>> qualifiers = new LinkedHashMap<ByteArray, Set<ByteArray>>();

  private boolean ordered = false;

  private ByteArray orderByColumnFamily;

  private ByteArray orderByQualifier;

  private Order order;

  private boolean paged = false;

  /*-------------------------------------------   Major Logic Methods   --------------------------------------------*/

  public boolean isPaged() {
    return paged;
  }

  public void setPaged(boolean paged) {
    this.paged = paged;
  }

  public Map<ByteArray, Set<ByteArray>> getQualifiers() {
    return qualifiers;
  }

  public int getQualifiersCount() {
    int qualifiersCount = 0;
    for (Set<ByteArray> qualifierSet : qualifiers.values()) {
      qualifiersCount += qualifierSet.size();
    }
    return qualifiersCount;
  }

  public boolean contains(ByteArray columnFamily, ByteArray qualifier) {
    if (qualifiers.containsKey(columnFamily)) {
      return qualifiers.get(columnFamily).contains(qualifier);
    } else {
      return false;
    }
  }

  /**
   * If contains any LIKE operator, it is fuzzy query, For fuzzy query,
   * index-base search doesn't work, only use full-table scan search.
   * 
   * @return
   */
  public boolean isFuzzy() {
    return hasLikeOperator(this.condition);
  }

  private boolean hasLikeOperator(Condition condition) {
    if (condition instanceof SimpleCondition) {
      SimpleCondition simpleCondition = (SimpleCondition) condition;
      return simpleCondition.getOperator() == Operator.LIKE;
    } else if (condition instanceof CompositeCondition) {
      CompositeCondition compositeCondition = (CompositeCondition) condition;
      Set<Condition> children = compositeCondition.getChildren();
      Iterator<Condition> iterator = children.iterator();
      while (iterator.hasNext()) {
        if (hasLikeOperator(iterator.next())) {
          return true;
        }
      }
    }
    return false;
  }

  public void valid() {
    // todo, verify!!
  }

  /*--------------------------------------------     Common Methods    ---------------------------------------------*/

  @Override
  public void write(DataOutput dataOutput) throws IOException {
    dataOutput.writeInt(resultsLimit);
    Bytes.writeByteArray(dataOutput, table.getBytes());
    WritableUtil.writeInstance(dataOutput, condition);
    dataOutput.writeInt(qualifiers.size());
    for (Map.Entry<ByteArray, Set<ByteArray>> entry : qualifiers.entrySet()) {
      Bytes.writeByteArray(dataOutput, entry.getKey().getBytes());
      Set<ByteArray> qualifierSet = entry.getValue();
      dataOutput.writeInt(qualifierSet.size());
      for (ByteArray qualifier : qualifierSet) {
        Bytes.writeByteArray(dataOutput, qualifier.getBytes());
      }
    }
    dataOutput.writeBoolean(ordered);
    if (ordered) {
      Bytes.writeByteArray(dataOutput, orderByColumnFamily.getBytes());
      Bytes.writeByteArray(dataOutput, orderByQualifier.getBytes());
      dataOutput.writeInt(order.getCode());
    }
    dataOutput.writeBoolean(paged);
  }

  @Override
  public void readFields(DataInput dataInput) throws IOException {
    resultsLimit = dataInput.readInt();
    table = new ByteArray(Bytes.readByteArray(dataInput));
    condition = WritableUtil.readInstance(dataInput, Condition.class);
    int familyCnt = dataInput.readInt();
    for (int i = 0; i < familyCnt; i++) {
      ByteArray columnFamily = new ByteArray(Bytes.readByteArray(dataInput));
      int qualifierCnt = dataInput.readInt();
      Set<ByteArray> qualifiers = new LinkedHashSet<ByteArray>(qualifierCnt);
      for (int j = 0; j < qualifierCnt; j++) {
        qualifiers.add(new ByteArray(Bytes.readByteArray(dataInput)));
      }
      this.qualifiers.put(columnFamily, qualifiers);
    }
    ordered = dataInput.readBoolean();
    if (ordered) {
      orderByColumnFamily = new ByteArray(Bytes.readByteArray(dataInput));
      orderByQualifier = new ByteArray(Bytes.readByteArray(dataInput));
      order = Order.fromCode(dataInput.readInt());
    }
    paged = dataInput.readBoolean();
  }

  @Override
  public String toString() {
    StringBuilder sb = new StringBuilder();
    for (Map.Entry<ByteArray, Set<ByteArray>> entry : qualifiers.entrySet()) {
      ByteArray columnFamily = entry.getKey();
      for (ByteArray qualifier : entry.getValue()) {
        sb.append(Bytes.toStringBinary(columnFamily.getBytes())).append(":")
            .append(Bytes.toStringBinary(qualifier.getBytes())).append(",");
      }
    }
    String str = "Query{" + "table=" + Bytes.toStringBinary(table.getBytes())
        + ", resultsLimit=" + resultsLimit + ", condition=" + condition
        + ", qualifiers=" + sb.toString() + ", paged=" + paged;
    if (isOrdered()) {
      str += ", orderByColumnFamily="
          + Bytes.toStringBinary(orderByColumnFamily.getBytes())
          + ", orderByQualifier="
          + Bytes.toStringBinary(orderByQualifier.getBytes()) + ", order="
          + order;
    }
    str += '}';
    return str;
  }

  /*--------------------------------------------    Getters/Setters    ---------------------------------------------*/

  public ByteArray getTable() {
    return table;
  }

  public void setTable(ByteArray table) {
    this.table = table;
  }

  public int getResultsLimit() {
    return resultsLimit;
  }

  public void setResultsLimit(int resultsLimit) {
    if (resultsLimit > Constants.QUERY_MAX_RESULT_LIMIT) {
      throw new ExceedingMaxResultLimitException();
    }
    this.resultsLimit = resultsLimit;
  }

  public Condition getCondition() {
    return condition;
  }

  public void setCondition(Condition condition) {
    this.condition = condition;
    qualifiers.clear();
    resolveQualifiers(this.condition);
  }

  private void resolveQualifiers(Condition condition) {
    if (condition instanceof SimpleCondition) {
      SimpleCondition simpleCondition = (SimpleCondition) condition;
      ByteArray columnFamily = simpleCondition.getColumnFamily();
      ByteArray qualifier = simpleCondition.getQualifier();
      if (!qualifiers.containsKey(columnFamily)) {
        qualifiers.put(columnFamily, new LinkedHashSet<ByteArray>());
      }
      qualifiers.get(columnFamily).add(qualifier);
    } else if (condition instanceof CompositeCondition) {
      CompositeCondition compositeCondition = (CompositeCondition) condition;
      Set<Condition> children = compositeCondition.getChildren();
      for (Condition child : children) {
        resolveQualifiers(child);
      }
    }
  }

  public boolean isOrdered() {
    return ordered;
  }

  public void setOrdered(boolean ordered) {
    this.ordered = ordered;
  }

  public ByteArray getOrderByColumnFamily() {
    return orderByColumnFamily;
  }

  public void setOrderByColumnFamily(ByteArray orderByColumnFamily) {
    this.orderByColumnFamily = orderByColumnFamily;
    this.ordered = true;
  }

  public ByteArray getOrderByQualifier() {
    return orderByQualifier;
  }

  public void setOrderByQualifier(ByteArray orderByQualifier) {
    this.orderByQualifier = orderByQualifier;
    this.ordered = true;
  }

  public Order getOrder() {
    return order;
  }

  public void setOrder(Order order) {
    this.order = order;
    this.ordered = true;
  }
}
