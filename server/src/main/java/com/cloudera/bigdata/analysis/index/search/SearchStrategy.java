package com.cloudera.bigdata.analysis.index.search;

import org.apache.hadoop.hbase.regionserver.HRegion;

import com.cloudera.bigdata.analysis.query.Query;
import com.cloudera.bigdata.analysis.query.QueryResult;

/**
 * The interface of all search strategy.
 * 
 * @see com.cloudera.bigdata.analysis.index.search.IndexBasedSearchStrategy
 * @see FullTableScanBasedSearchStrategy
 */
public interface SearchStrategy {
  /**
   * Do search job for given query on given region.
   * 
   * @param region
   *          the given region
   * @param query
   *          the query object
   * @return the query result.
   */
  QueryResult doSearch(HRegion region, Query query);
}
