package com.cloudera.bigdata.analysis.index.search;

import java.util.LinkedHashSet;
import java.util.List;
import java.util.Set;

import org.apache.hadoop.hdfs.util.ByteArray;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import com.cloudera.bigdata.analysis.index.Index;
import com.cloudera.bigdata.analysis.index.formatter.IndexFieldValueFormatter;
import com.cloudera.bigdata.analysis.index.util.ByteArrayUtils;
import com.cloudera.bigdata.analysis.query.AndCondition;
import com.cloudera.bigdata.analysis.query.Condition;
import com.cloudera.bigdata.analysis.query.InvalidQueryException;
import com.cloudera.bigdata.analysis.query.Operator;
import com.cloudera.bigdata.analysis.query.OrCondition;
import com.cloudera.bigdata.analysis.query.Query;
import com.cloudera.bigdata.analysis.query.SimpleCondition;

/**
 * When coprocessor receiving a query request, it uses this class to decide what
 * kind of {@link com.cloudera.bigdata.analysis.index.search.SearchStrategy} should
 * apply. There are two kinds of search strategy:
 * {@link com.cloudera.bigdata.analysis.index.search.IndexBasedSearchStrategy} and
 * {@link FullTableScanBasedSearchStrategy}. The former search records via
 * indexes, fast and efficient, but only support limited query conditions. The
 * later search records by full-table scan scan, performance is less than the
 * former, but support any queries.
 * <p>
 * This decider analysis given query object, if it's fuzzy query, use full-table
 * scan search directly, otherwise, take 2 rounds check to decide what strategy
 * should apply. The first round check call pre-match, check whether all query's
 * fields are index supported,if yes, take second round check, otherwise use
 * full-table scan search directly. For second round check, resolve each
 * condition, then get a set of resolved search range, if all search ranges are
 * boundary definite, use index search, otherwise, use full-table scan search.
 * </p>
 * 
 * @see com.cloudera.bigdata.analysis.index.IndexCoprocessor#query(com.cloudera.bigdata.analysis.query.Query)
 */
public class SearchStrategyDecider {

  private static final Logger LOG = LoggerFactory
      .getLogger(SearchStrategyDecider.class);

  public SearchStrategy decide(Query query) {
    Set<Index> indexes = Index.get(query.getTable());
    int queryQualifiersCount = query.getQualifiersCount();
    Index optimumIndex = null;
    // -1 as initial value is required! for some indexes, there could be 0
    // fields left-continuously matched!
    int leftContinuouslyMatchedQualifierCountOfOptimumIndex = -1;

    // iterate index one by one, evaluate very index, compare with existing
    // optimum index, if it's better, replace it.
    // the evaluation rule is:
    // Compare current index and existing optimum index, who match more
    // left-continuous fields, who win!
    // for example:
    // for query (a==1&&c==2), there are 2 candidate indexes: [a,b,c] and [a,c],
    // the [a,c] is better than [a,b,c].
    for (Index index : indexes) {
      List<Index.Field> fields = index.getFields();
      // if fields of index is less than query, obviously, this index is out!
      if (index.getFieldsCount() < queryQualifiersCount) {
        continue;
      }
      if (query.isOrdered()) {
        // if the order-by qualifier of query is not the first qualifier of
        // index,
        // it means the records of index referred to are not sorted by the query
        // requested order,
        // so, this index still is out!
        if (!query.getOrderByColumnFamily().equals(
            index.getOrderByColumnFamily())
            || !query.getOrderByQualifier().equals(index.getOrderByQualifier())) {
          continue;
        }

        // if query specified order and the the specified order is different
        // with index's,
        // this index still is out!
        if (query.getOrder() != null && query.getOrder() != index.getOrder()) {
          continue;
        }
      }
      int matchedQueryQualifierCount = 0;
      int leftContinuouslyMatchedQualifierCount = 0;
      boolean leftContinuouslyMatchingBroken = false;
      for (Index.Field field : fields) {
        if (query.contains(field.getColumnFamily(), field.getQualifier())) {
          matchedQueryQualifierCount++;
          if (!leftContinuouslyMatchingBroken) {
            leftContinuouslyMatchedQualifierCount++;
          }
        } else {
          leftContinuouslyMatchingBroken = true;
        }
      }
      // if matched query fields is less than query fields total,
      // if means this index does NOT contain some fields of query,this index is
      // still out!
      // In other word, the fields of query must be a sub-set of index fields!
      if (matchedQueryQualifierCount < queryQualifiersCount) {
        continue;
      }

      // if the count of left-continuously-matched fields for current index is
      // greater then
      // existing optimum index, choose it as optimum index.
      if (leftContinuouslyMatchedQualifierCount > leftContinuouslyMatchedQualifierCountOfOptimumIndex) {
        optimumIndex = index;
        leftContinuouslyMatchedQualifierCountOfOptimumIndex = leftContinuouslyMatchedQualifierCount;
      }
    }

    SearchStrategy searchStrategy = null;
    if (optimumIndex != null && !query.isFuzzy()) {
      IndexRangeSet optimumIndexRangeSet = resolveIndexSearchRange(
          query.getCondition(), optimumIndex);
      LOG.info("do index-based search...");
      searchStrategy = new IndexBasedSearchStrategy(optimumIndex,
          optimumIndexRangeSet);
      if (LOG.isDebugEnabled()) {
        LOG.debug("resolved range set:" + optimumIndexRangeSet);
      }
    } else {
      LOG.info("do full-table-scan-based search...");
      searchStrategy = new FullTableScanBasedSearchStrategy();
    }
    return searchStrategy;
  }

  /**
   * Resolve target index ranges according to given condition.
   * 
   * @param condition
   *          the given condition
   * @param index
   *          the given index
   * @return a set of resolved index ranges.
   */
  private IndexRangeSet resolveIndexSearchRange(Condition condition, Index index) {
    IndexRangeSet resolvedIndexRangeSet = new IndexRangeSet();
    if (condition instanceof SimpleCondition) {
      SimpleCondition simpleCondition = (SimpleCondition) condition;
      ByteArray columnFamily = simpleCondition.getColumnFamily();
      ByteArray qualifier = simpleCondition.getQualifier();
      ByteArray value = simpleCondition.getValue();
      Operator operator = simpleCondition.getOperator();
      Index.Field field = index.getField(columnFamily, qualifier);
      IndexFieldValueFormatter fieldValueFormatter = field.getFormatter();
      switch (operator) {
      case EQ: {
        IndexRange.StartBoundary startRowKey = new IndexRange.StartBoundary(
            index.getFields());
        IndexRange.EndBoundary endRowKey = new IndexRange.EndBoundary(
            index.getFields());
        ByteArray formattedStartValue = fieldValueFormatter
            .format(field, value);
        startRowKey.set(field, formattedStartValue);
        ByteArray formattedEndValue = fieldValueFormatter.format(field, value);
        endRowKey.set(field, formattedEndValue);
        resolvedIndexRangeSet.add(new IndexRange(startRowKey, endRowKey));
        break;
      }
      case GT: {
        IndexRange.StartBoundary startRowKey = new IndexRange.StartBoundary(
            index.getFields());
        IndexRange.EndBoundary endRowKey = new IndexRange.EndBoundary(
            index.getFields());
        if (!field.isOrderInverted()) {
          ByteArray increasedValue = ByteArrayUtils.increaseOneByte(value);
          ByteArray formattedIncreasedValue = fieldValueFormatter.format(field,
              increasedValue);
          startRowKey.set(field, formattedIncreasedValue);
        } else {
          // if field is order-inverted, the field value is stored by an
          // complement style.
          // for example: if max value is 10, then, 10 => 10-10 => 0, 9 => 10-9
          // =>1, ...,
          // 2 => 10-2 => 8, 1 => 10-1 => 9.
          // so, a > 9 (a >= 10) should convert to a < 1 (a <= 0)!!
          ByteArray increasedValue = ByteArrayUtils.increaseOneByte(value);
          ByteArray formattedIncreasedValue = fieldValueFormatter.format(field,
              increasedValue);
          endRowKey.set(field, formattedIncreasedValue);
        }
        resolvedIndexRangeSet.add(new IndexRange(startRowKey, endRowKey));
        break;
      }
      case GTE: {
        IndexRange.StartBoundary startRowKey = new IndexRange.StartBoundary(
            index.getFields());
        IndexRange.EndBoundary endRowKey = new IndexRange.EndBoundary(
            index.getFields());
        if (!field.isOrderInverted()) {
          ByteArray formattedValue = fieldValueFormatter.format(field, value);
          startRowKey.set(field, formattedValue);
        } else {
          // if field is order-inverted, the field value is stored by an
          // complement style.
          // for example: if max value is 10, then, 10 => 10-10 => 0, 9 => 10-9
          // =>1, ...,
          // 2 => 10-2 => 8, 1 => 10-1 => 9.
          // so, a >= 9 should convert to a <= 1 !!
          ByteArray formattedValue = fieldValueFormatter.format(field, value);
          endRowKey.set(field, formattedValue);
        }
        resolvedIndexRangeSet.add(new IndexRange(startRowKey, endRowKey));
        break;
      }
      case LT: {
        IndexRange.StartBoundary startRowKey = new IndexRange.StartBoundary(
            index.getFields());
        IndexRange.EndBoundary endRowKey = new IndexRange.EndBoundary(
            index.getFields());
        if (!field.isOrderInverted()) {
          ByteArray decreasedValue = ByteArrayUtils.decreaseOneByte(value);
          ByteArray formattedDecreasedValue = fieldValueFormatter.format(field,
              decreasedValue);
          endRowKey.set(field, formattedDecreasedValue);
        } else {
          // if field is order-inverted, the field value is stored by an
          // complement style.
          // for example: if max value is 10, then, 10 => 10-10 => 0, 9 => 10-9
          // =>1, ...,
          // 2 => 10-2 => 8, 1 => 10-1 => 9.
          // so, a < 9 (a <= 8) should convert to a > 1 (a >= 2)!!
          ByteArray decreasedValue = ByteArrayUtils.decreaseOneByte(value);
          ByteArray formattedDecreasedValue = fieldValueFormatter.format(field,
              decreasedValue);
          startRowKey.set(field, formattedDecreasedValue);
        }
        resolvedIndexRangeSet.add(new IndexRange(startRowKey, endRowKey));
        break;
      }
      case LTE: {
        IndexRange.StartBoundary startRowKey = new IndexRange.StartBoundary(
            index.getFields());
        IndexRange.EndBoundary endRowKey = new IndexRange.EndBoundary(
            index.getFields());
        if (!field.isOrderInverted()) {
          ByteArray minValue = ByteArrayUtils.getMinValueByteArray(value
              .getBytes().length);
          startRowKey.set(field, minValue);
          ByteArray formattedValue = fieldValueFormatter.format(field, value);
          endRowKey.set(field, formattedValue);
        } else {
          // if field is order-inverted, the field value is stored by an
          // complement style.
          // for example: if max value is 10, then, 10 => 10-10 => 0, 9 => 10-9
          // =>1, ...,
          // 2 => 10-2 => 8, 1 => 10-1 => 9.
          // so, a <= 9 should convert to a >= 1 !!
          ByteArray formattedValue = fieldValueFormatter.format(field, value);
          startRowKey.set(field, formattedValue);
        }
        resolvedIndexRangeSet.add(new IndexRange(startRowKey, endRowKey));
        break;
      }
      case NEQ: {
        IndexRange.StartBoundary startRowKey1 = new IndexRange.StartBoundary(
            index.getFields());
        IndexRange.EndBoundary endRowKey1 = new IndexRange.EndBoundary(
            index.getFields());
        IndexRange.StartBoundary startRowKey2 = new IndexRange.StartBoundary(
            index.getFields());
        IndexRange.EndBoundary endRowKey2 = new IndexRange.EndBoundary(
            index.getFields());
        if (!field.isOrderInverted()) {
          ByteArray decreasedValue = ByteArrayUtils.decreaseOneByte(value);
          ByteArray formattedDecreasedValue = fieldValueFormatter.format(field,
              decreasedValue);
          endRowKey1.set(field, formattedDecreasedValue);

          ByteArray increasedValue = ByteArrayUtils.increaseOneByte(value);
          ByteArray formattedIncreasedValue = fieldValueFormatter.format(field,
              increasedValue);
          startRowKey2.set(field, formattedIncreasedValue);
        } else {
          // if field is order-inverted, the field value is stored by an
          // complement style.
          // for example: if max value is 10, then, 10 => 10-10 => 0, 9 => 10-9
          // =>1, ...,
          // 2 => 10-2 => 8, 1 => 10-1 => 9.
          // so, a!=3 (a<=2 && a>=4) should convert to a!=7 (a>=8 && a<=6) !!
          ByteArray increasedValue = ByteArrayUtils.increaseOneByte(value);
          ByteArray formattedIncreasedValue = fieldValueFormatter.format(field,
              increasedValue);
          endRowKey1.set(field, formattedIncreasedValue);
          ByteArray decreasedValue = ByteArrayUtils.decreaseOneByte(value);
          ByteArray formattedDecreasedValue = fieldValueFormatter.format(field,
              decreasedValue);
          startRowKey2.set(field, formattedDecreasedValue);
        }
        resolvedIndexRangeSet.add(new IndexRange(startRowKey1, endRowKey1));
        resolvedIndexRangeSet.add(new IndexRange(startRowKey2, endRowKey2));
        break;
      }
      case LIKE: {
        // LIKE condition can NOT do merge or union operation! It should be
        // filtered before by Query.isFuzzy() method!
        // for example: what's the result of (a like %1%) and (a > 100)? it's
        // very difficult or impossible to resolve
        // LIKE condition or merge/union them, so, just let it go, we will use
        // other strategy
        // to scan full index, or FullTableScanBasedSearchStrategy to scan full
        // table.
        String message = "LIKE operator is invalid! It should be filtered before do range resolving calculation!";
        RuntimeException runtimeException = new RuntimeException(message);
        LOG.error(message, runtimeException);
        throw runtimeException;
      }
      }
    } else if (condition instanceof AndCondition) {
      // for AndCondition, do full-cross "and" merger for all its child
      // condition's search ranges!
      AndCondition andCondition = (AndCondition) condition;
      Set<Condition> childConditions = andCondition.getChildren();
      Set<IndexRangeSet> childIndexRangeSets = new LinkedHashSet<IndexRangeSet>();
      // level first iteration. this is more friendly for debug and log trace.
      for (Condition childCondition : childConditions) {
        IndexRangeSet childIndexRangeSet = resolveIndexSearchRange(
            childCondition, index);
        childIndexRangeSets.add(childIndexRangeSet);
      }
      for (IndexRangeSet childIndexRangeSet : childIndexRangeSets) {
        resolvedIndexRangeSet = IndexRangeSet.and(childIndexRangeSet,
            resolvedIndexRangeSet);
      }
    } else if (condition instanceof OrCondition) {
      // for OrCondition, do nothing, just collection collect all its child
      // condition's ranges, and return.
      OrCondition orCondition = (OrCondition) condition;
      Set<Condition> childConditions = orCondition.getChildren();
      Set<IndexRangeSet> childIndexRangeSets = new LinkedHashSet<IndexRangeSet>();
      // level first iteration. this is more friendly for debug and log trace.
      for (Condition childCondition : childConditions) {
        IndexRangeSet childIndexRangeSet = resolveIndexSearchRange(
            childCondition, index);
        childIndexRangeSets.add(childIndexRangeSet);
      }
      for (IndexRangeSet childIndexRangeSet : childIndexRangeSets) {
        resolvedIndexRangeSet = IndexRangeSet.or(childIndexRangeSet,
            resolvedIndexRangeSet);
      }
    } else {
      throw new InvalidQueryException("Unknown Condition Type of " + condition);
    }
    return resolvedIndexRangeSet;
  }

  public static void main(String args[]) {
    Index.refreshIndexMap(args[0]);

    LOG.info("index refreshed mape size: " + Index.indexMap.size() + ": "
        + Index.indexMap.toString());
  }

}
