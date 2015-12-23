package com.cloudera.bigdata.analysis.index.search;

import java.io.IOException;
import java.util.ArrayList;
import java.util.List;

import org.apache.hadoop.hbase.Cell;
import org.apache.hadoop.hbase.client.Result;
import org.apache.hadoop.hbase.client.Scan;
import org.apache.hadoop.hbase.protobuf.ProtobufUtil;
import org.apache.hadoop.hbase.protobuf.generated.ClientProtos;
import org.apache.hadoop.hbase.regionserver.HRegion;
import org.apache.hadoop.hbase.regionserver.RegionScanner;
import org.apache.hadoop.hbase.util.Bytes;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import com.cloudera.bigdata.analysis.index.Constants;
import com.cloudera.bigdata.analysis.index.protobuf.ProtoUtil;
import com.cloudera.bigdata.analysis.index.protobuf.generated.QueryTypeProto;
import com.cloudera.bigdata.analysis.index.util.RegionUtil;
import com.cloudera.bigdata.analysis.query.Query;
import com.cloudera.bigdata.analysis.query.QueryResult;
import com.cloudera.bigdata.analysis.query.SearchMode;

/**
 * This strategy will scan full table scan with a {@link FullTableScanFilter} to
 * retrieve matched records of given query.
 * 
 * @see FullTableScanFilter
 */
public class FullTableScanBasedSearchStrategy implements SearchStrategy {

  private static final Logger LOG = LoggerFactory
      .getLogger(FullTableScanBasedSearchStrategy.class);

  // region server level pool, all regions of server share the same thread pool.
  // private static ThreadPoolExecutor fullTableSearchThreadPoolExecutor =
  // new ThreadPoolExecutor(32,32*10,10,TimeUnit.MINUTES, new
  // LinkedBlockingDeque<Runnable>());

  @Override
  public QueryResult doSearch(HRegion region, Query query) {
    QueryTypeProto.QueryResult.Builder builder = QueryTypeProto.QueryResult
        .newBuilder();
    builder.setSearchMode(QueryTypeProto.SearchMode
        .valueOf(SearchMode.FULL_TABLE_SCAN_BASED_SEARCH.getCode()));
    builder.setQuery(ProtoUtil.toQueryProto(query));
    Scan scan = new Scan();
    scan.setFilter(new FullTableScanFilter(query));

    // change it to write cache policy!
    scan.setCacheBlocks(Constants.P_SERVER_SEARCH_ENABLE_BLOCK_CACHE_FOR_SEARCH);
    // skip index entries.
    scan.setStartRow(Bytes.add(RegionUtil.getRegionStartKey(region),
        Constants.HBASE_TABLE_DELIMITER));
    int resultLimit = query.getResultsLimit();
    RegionScanner scanner = null;
    try {
      scanner = region.getScanner(scan);
      List<Cell> rows = new ArrayList<Cell>();
      boolean hasNext = false;
      do {
        rows.clear();
        hasNext = scanner.next(rows);
        if (rows.size() > 0) {
          Result result = Result.create(rows);
          ClientProtos.Result rp = ProtobufUtil.toResult(result);
          builder.addRecords(rp);
          // decide whether add it to final result set based on result limit and
          // order.
          resultLimit--;
        } else {
          break;
        }
      } while (hasNext && resultLimit > 0);
    } catch (IOException e) {
      e.printStackTrace();
      LOG.error(e.getMessage(), e);
    } finally {
      if (scanner != null) {
        try {
          scanner.close();
        } catch (IOException e) {
          e.printStackTrace();
          LOG.error(e.getMessage(), e);
        }
      }
    }

    QueryTypeProto.QueryResult qp = builder.build();
    return new QueryResult(qp);
  }

  // @Override
  // public QueryResult doSearch(HRegion region, Query query) {
  // LOG.info("do full-table scan search for "+query);
  // int concurrentScanThreads = 1;
  // List<Future<QueryResult>> queryResultFutures = new
  // ArrayList<Future<QueryResult>>(concurrentScanThreads);
  // int volumePerScan = ServerConstants.VOLUME_PER_REGION /
  // concurrentScanThreads;
  // LOG.info("ServerConstants.VOLUME_PER_REGION:" +
  // ServerConstants.VOLUME_PER_REGION);
  // LOG.info("volumePerScan:" + volumePerScan);
  // int regionStarKey =
  // Integer.parseInt(Bytes.toStringBinary(RegionUtil.getRegionStartKey(region)));
  // StopWatch stopWatch = new StopWatch();
  // stopWatch.start();
  // for (int i = 0; i < concurrentScanThreads; i++) {
  // byte[] scanStartKey = Bytes.add(
  // Bytes.toBytes(RowKeyUtil.formatToRowKeyPrefix(regionStarKey + i *
  // volumePerScan)),
  // MetaData.BS_REC_ROWKEY_DELIMITER);
  // byte[] scanEndKey = null;
  // //if i == concurrentScanThreads-1, keep scanEndKey null! otherwise some
  // records will lost.
  // //for example: volumePerScan = 100, concurrentScanThreads = 3, the scan
  // range will be:
  // //[0,33),[33,66),[66,99) this is wrong, last range should be: [66,null)
  // if(i<concurrentScanThreads-1){
  // // scan stop at end key, records with end key will be scanned at next loop,
  // so no duplicated records in final result set!
  // scanEndKey = Bytes.add(
  // Bytes.toBytes(RowKeyUtil.formatToRowKeyPrefix(regionStarKey + (i + 1) *
  // volumePerScan)),
  // MetaData.BS_REC_ROWKEY_DELIMITER);
  // }
  // queryResultFutures.add(fullTableSearchThreadPoolExecutor.submit(new
  // FullTableSearchThread(region, query, scanStartKey, scanEndKey)));
  // }
  // stopWatch.stop();
  // LOG.info("scan:" + stopWatch.getTime());
  // // fullTableSearchThreadPoolExecutor.submit()
  // QueryResult queryResult = new QueryResult(query,
  // QueryResult.SearchMode.FULL_TABLE_SCAN_BASED_SEARCH);
  // try {
  // stopWatch.reset();
  // StopWatch stopWatch1 = new StopWatch();
  // int scanTime = 0;
  // stopWatch.start();
  // for (Future<QueryResult> queryResultFuture : queryResultFutures) {
  // stopWatch1.reset();
  // stopWatch1.start();
  // QueryResult scanQueryResult = queryResultFuture.get();
  // stopWatch1.stop();
  // scanTime+=stopWatch1.getTime();
  // queryResult.addAll(scanQueryResult);
  // }
  // stopWatch.stop();
  // LOG.info("scan all:"+scanTime);
  // LOG.info("add all:"+stopWatch.getTime());
  // } catch (InterruptedException e) {
  // e.printStackTrace();
  // } catch (ExecutionException e) {
  // e.printStackTrace();
  // }
  // // fullTableSearchThreadPoolExecutor.
  // // Bytes.add(RegionUtil.getRegionStartKey(region),
  // MetaData.BS_REC_ROWKEY_DELIMITER)
  // return queryResult;
  // }
  //
  // private static class FullTableSearchThread implements Callable<QueryResult>
  // {
  //
  // private HRegion region;
  //
  // private Query query;
  //
  // private byte[] scanStartKey;
  //
  // private byte[] scanEndKey;
  //
  // private QueryResult queryResult;
  //
  // public FullTableSearchThread(HRegion region, Query query, byte[]
  // scanStartKey, byte[] scanEndKey){
  // this.region = region;
  // this.query = query;
  // this.scanStartKey = scanStartKey;
  // this.scanEndKey = scanEndKey;
  // this.queryResult = new QueryResult(query,
  // QueryResult.SearchMode.FULL_TABLE_SCAN_BASED_SEARCH);
  // }
  //
  // @Override
  // public QueryResult call() throws Exception {
  // Scan scan = new Scan();
  // scan.setFilter(new FullTableScanFilter(query));
  // scan.setCacheBlocks(PropUtil.getBooleanProperty(ServerConstants.P_SERVER_SEARCH_ENABLE_BLOCK_CACHE_FOR_SEARCH));
  // //skip index entries.
  // scan.setStartRow(scanStartKey);
  // scan.setStopRow(scanEndKey);
  // int resultLimit = query.getResultsLimit();
  // RegionScanner scanner = null;
  // try {
  // scanner = region.getScanner(scan);
  // new
  // List<KeyValue> row = new ArrayList<KeyValue>();
  // boolean hasNext = false;
  // do {
  // row.clear();
  // hasNext = scanner.next(row);
  // if (row.size() > 0) {
  // Result result = new Result(row);
  // //decide whether add it to final result set based on result limit and
  // order.
  // queryResult.add(result);
  // resultLimit--;
  // } else {
  // break;
  // }
  // } while (hasNext && resultLimit > 0);
  // } catch (IOException e) {
  // e.printStackTrace();
  // LOG.error(e.getMessage(), e);
  // } finally {
  // if(scanner!=null){
  // try {
  // scanner.close();
  // } catch (IOException e) {
  // e.printStackTrace();
  // LOG.error(e.getMessage(), e);
  // }
  // }
  // }
  // return queryResult;
  // }
  // }
}
