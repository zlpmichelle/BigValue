package com.cloudera.bigdata.analysis.index.search;

import java.io.IOException;
import java.util.ArrayList;
import java.util.Collections;
import java.util.LinkedHashSet;
import java.util.List;
import java.util.Set;

import org.apache.commons.collections.CollectionUtils;
import org.apache.commons.lang.ArrayUtils;
import org.apache.hadoop.hbase.Cell;
import org.apache.hadoop.hbase.client.Get;
import org.apache.hadoop.hbase.client.Result;
import org.apache.hadoop.hbase.client.Scan;
import org.apache.hadoop.hbase.protobuf.ProtobufUtil;
import org.apache.hadoop.hbase.protobuf.generated.ClientProtos;
import org.apache.hadoop.hbase.regionserver.HRegion;
import org.apache.hadoop.hbase.regionserver.RegionScanner;
import org.apache.hadoop.hbase.util.Bytes;
import org.apache.hadoop.hdfs.util.ByteArray;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import com.cloudera.bigdata.analysis.index.Constants;
import com.cloudera.bigdata.analysis.index.Index;
import com.cloudera.bigdata.analysis.index.protobuf.ProtoUtil;
import com.cloudera.bigdata.analysis.index.protobuf.generated.QueryTypeProto;
import com.cloudera.bigdata.analysis.index.util.ByteArrayUtils;
import com.cloudera.bigdata.analysis.index.util.RegionUtil;
import com.cloudera.bigdata.analysis.query.Query;
import com.cloudera.bigdata.analysis.query.QueryResult;
import com.cloudera.bigdata.analysis.query.ResultComparator;
import com.cloudera.bigdata.analysis.query.SearchMode;

/**
 * This strategy search records via indexes. It search index entries directly by
 * a {@link com.cloudera.bigdata.analysis.index.search.IndexRangeSet} which
 * resolved from
 * {@link com.cloudera.bigdata.analysis.index.search.SearchStrategyDecider}, get
 * target record row key from index entry, then get this record by row key.
 * index entry and target record always reside at the same region, so retrieving
 * record by index is very fast. NOTE: This strategy is generic, it does NOT
 * depend on any table data structure!!
 */
public class IndexBasedSearchStrategy implements SearchStrategy {

  private static final Logger LOG = LoggerFactory
      .getLogger(IndexBasedSearchStrategy.class);

  private Index index;

  private IndexRangeSet indexRangeSet;

  public IndexBasedSearchStrategy(Index index, IndexRangeSet indexRangeSet) {
    this.index = index;
    this.indexRangeSet = indexRangeSet;
  }

  /*-------------------------------------------   Major Logic Methods   --------------------------------------------*/

  @Override
  public QueryResult doSearch(HRegion region, Query query) {
    byte[] prefix = RegionUtil.getRegionStartKey(region);
    ByteArray indexName = index.getName();
    int scannedCount = 0;
    int qualifiersCount = query.getQualifiersCount();
    int resultLimit = query.getResultsLimit();
    boolean paged = query.isPaged();
    // NOTE: two index entry might refer to the same record,
    // in other words, record row key could be duplicated, so we use set
    // to exclude duplicated record row key automatically.
    Set<ByteArray> recordRowKeySet = new LinkedHashSet<ByteArray>(0);
    IndexScanFilter indexScanFilter = null;
    for (IndexRange indexRange : indexRangeSet) {
      if (LOG.isDebugEnabled()) {
        LOG.debug("do search on index range: " + indexRange);
      }
      if (!paged && recordRowKeySet.size() >= resultLimit) {
        break;
      }
      byte[] scanStartRowKey = buildScanRowKey(prefix, indexName.getBytes(),
          indexRange.getStartBoundary());
      byte[] scanEndRowKey = buildScanRowKey(prefix, indexName.getBytes(),
          indexRange.getEndBoundary());
      if (LOG.isDebugEnabled()) {
        LOG.debug("scan start key: " + Bytes.toStringBinary(scanStartRowKey));
        LOG.debug("scan stop key: " + Bytes.toStringBinary(scanEndRowKey));
      }
      Scan scan = new Scan(scanStartRowKey, scanEndRowKey);
      if (qualifiersCount >= 2 || !indexRange.isLcne()) {
        indexScanFilter = new IndexScanFilter(index.getFields(), indexRange);
        scan.setFilter(indexScanFilter);
      }
      scan.setCacheBlocks(Constants.P_SERVER_SEARCH_ENABLE_BLOCK_CACHE_FOR_SEARCH);
      RegionScanner scanner = null;
      try {
        scanner = region.getScanner(scan);
        while (true) {
          List<Cell> row = new ArrayList<Cell>();
          boolean hasNext = scanner.next(row);
          if (CollectionUtils.isNotEmpty(row)) {
            if (Constants.ENABLE_STATISTICS && indexScanFilter == null) {
              scannedCount++;
            }
            if (LOG.isDebugEnabled()) {
              LOG.debug("returned row:" + row);
            }
            byte[] indexEntry = row.get(0).getRow();
            byte[] recordRowKey = Bytes.tail(indexEntry,
                Constants.CR_ROW_KEY_LENGTH);
            if (LOG.isDebugEnabled()) {
              LOG.debug("Record Row Key:" + Bytes.toStringBinary(recordRowKey));
            }
            recordRowKeySet.add(new ByteArray(recordRowKey));
          }
          if (!hasNext) {
            break;
          }
          if (!paged && recordRowKeySet.size() >= resultLimit) {
            break;
          }
        }
      } catch (IOException e) {
        LOG.error(e.getMessage(), e);
        throw new RuntimeException(e);
      } finally {
        if (scanner != null) {
          try {
            scanner.close();
          } catch (IOException e) {
            LOG.error(e.getMessage(), e);
            throw new RuntimeException(e);
          }
        }
      }
    }
    if (Constants.ENABLE_STATISTICS) {
      if (indexScanFilter == null) {
        LOG.info("Scanned records:" + scannedCount);
      } else {
        LOG.info("Scanned records:" + indexScanFilter.getScannedCount());
      }
    }
    List<Result> records = new ArrayList<Result>();
    try {
      for (ByteArray recordRowKey : recordRowKeySet) {
        records.add(region.get(new Get(recordRowKey.getBytes())));
      }
    } catch (IOException e) {
      LOG.error(e.getMessage(), e);
      throw new RuntimeException(e);
    }
    // Results from the same region is supposed to sort too!!
    // they might not ordered even the search range is ordered!
    // i.e. range [x,5]-[x,5] is before range [3,x]-[3,x],
    // however, for row key 7,5 (match first range) is after row key 3,6 (match
    // second range)
    // so, we can see even range is ordered, but it doesn't mean all records are
    // ordered!!
    // we still need sort records coming from the same region!
    if (query.isOrdered()) {
      ByteArray orderByColumnFamily = query.getOrderByColumnFamily();
      ByteArray orderByQualifier = query.getOrderByQualifier();
      Collections.sort(records, new ResultComparator(orderByColumnFamily,
          orderByQualifier, query.getOrder()));
    }

    QueryTypeProto.QueryResult.Builder builder = QueryTypeProto.QueryResult
        .newBuilder();
    builder.setSearchMode(QueryTypeProto.SearchMode
        .valueOf(SearchMode.INDEX_BASED_SEARCH.getCode()));
    builder.setQuery(ProtoUtil.toQueryProto(query));

    if (records.size() != 0) {
      for (Result result : records) {
        ClientProtos.Result rp = ProtobufUtil.toResult(result);
        builder.addRecords(rp);
      }
    } else {
      LOG.warn("records size is 0");
    }
    return new QueryResult(builder.build());
  }

  /**
   * Build search row key based on given index range row key. NOTE: all given
   * row key are boundary definite! they have checked before pass to
   * {@link com.cloudera.bigdata.analysis.index.search.IndexBasedSearchStrategy}
   * so, this method build row key using fields from left to right none-null
   * qualifier.
   */
  private byte[] buildScanRowKey(byte[] prefix, byte[] indexName,
      IndexRange.Boundary indexRangeBoundary) {
    byte[] rowKey = Bytes
        .add(prefix, Constants.IDX_ROWKEY_DELIMITER, indexName);
    rowKey = Bytes.add(rowKey, Constants.IDX_ROWKEY_DELIMITER);
    IndexRange.Boundary.LcneFeature lcneFeature = indexRangeBoundary
        .getLcneFeature();
    byte[] lcneFragment = lcneFeature.getLcneFragment();
    // lcneFragment could be null, i.e. there is no any condition.
    if (ArrayUtils.isNotEmpty(lcneFragment)) {
      if (indexRangeBoundary instanceof IndexRange.EndBoundary) {
        rowKey = Bytes
            .add(rowKey, ByteArrayUtils.increaseOneByte(lcneFragment));
      } else {
        // for scan start row key, append directly.
        rowKey = Bytes.add(rowKey, lcneFragment);
      }
    } else {
      // if lcne fragment is empty, and it's stop row key,
      // increase one byte based on existing row key directly.
      if (indexRangeBoundary instanceof IndexRange.EndBoundary) {
        rowKey = ByteArrayUtils.increaseOneByte(rowKey);
      }
    }
    return rowKey;
  }

  /*--------------------------------------------    Getters/Setters    ---------------------------------------------*/

  public IndexRangeSet getIndexRangeSet() {
    return indexRangeSet;
  }

  public Index getIndex() {
    return index;
  }
}
