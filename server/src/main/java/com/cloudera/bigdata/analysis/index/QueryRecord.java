package com.cloudera.bigdata.analysis.index;

import java.io.IOException;
import java.util.ArrayList;
import java.util.Collections;
import java.util.List;
import java.util.Map;
import java.util.Set;

import org.apache.commons.beanutils.BeanComparator;
import org.apache.commons.cli.BasicParser;
import org.apache.commons.cli.CommandLine;
import org.apache.commons.cli.CommandLineParser;
import org.apache.commons.cli.Options;
import org.apache.commons.cli.ParseException;
import org.apache.commons.collections.comparators.ReverseComparator;
import org.apache.commons.lang.StringUtils;
import org.apache.commons.lang.time.StopWatch;
import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.fs.FileStatus;
import org.apache.hadoop.fs.FileSystem;
import org.apache.hadoop.fs.Path;
import org.apache.hadoop.hbase.HBaseConfiguration;
import org.apache.hadoop.hbase.client.HBaseAdmin;
import org.apache.hadoop.hbase.client.HTable;
import org.apache.hadoop.hbase.client.Result;
import org.apache.hadoop.hbase.client.coprocessor.Batch;
import org.apache.hadoop.hbase.ipc.BlockingRpcCallback;
import org.apache.hadoop.hbase.protobuf.ProtobufUtil;
import org.apache.hadoop.hbase.protobuf.generated.ClientProtos;
import org.apache.hadoop.hbase.util.Bytes;
import org.apache.hadoop.hdfs.util.ByteArray;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import com.cloudera.bigdata.analysis.exception.ETLException;
import com.cloudera.bigdata.analysis.index.protobuf.ProtoUtil;
import com.cloudera.bigdata.analysis.index.protobuf.generated.QueryTypeProto;
import com.cloudera.bigdata.analysis.index.protobuf.generated.RecordServiceProto;
import com.cloudera.bigdata.analysis.index.protobuf.generated.QueryTypeProto.SearchMode;
import com.cloudera.bigdata.analysis.index.util.ConditionExpressionParser;
import com.cloudera.bigdata.analysis.index.util.IndexUtil;
import com.cloudera.bigdata.analysis.query.Condition;
import com.cloudera.bigdata.analysis.query.Order;
import com.cloudera.bigdata.analysis.query.Query;
import com.cloudera.bigdata.analysis.query.QueryResult;

public class QueryRecord {

  private static final Logger LOG = LoggerFactory.getLogger(QueryRecord.class);
  private static final String USAGE_STR = "com.cloudera.bigdata.analysis.dataload.index.QueryRecord -table CaptureRecord_Test -resultsLimit 5 -condition \"(f,locId,NEQ,370101)\"";

  // mark flag
  public static int queriedRegionsNum = 0;
  public static int queriedRecordsNumOnRegion = 0;
  public static boolean hasNext = false;

  private Configuration conf;
  private static HBaseAdmin hbaseAdmin;

  public static int getQueriedRegionsNum() {
    return queriedRegionsNum;
  }

  public static void setQueriedRegionsNum(int queriedRegionsNum) {
    QueryRecord.queriedRegionsNum = queriedRegionsNum;
  }

  public static int getQueriedRecordsNumOnRegion() {
    return queriedRecordsNumOnRegion;
  }

  public static void setQueriedRecordsNumOnRegion(int queriedRecordsNumOnRegion) {
    QueryRecord.queriedRecordsNumOnRegion = queriedRecordsNumOnRegion;
  }

  public static boolean getHasNext() {
    return hasNext;
  }

  public static void setHasNext(boolean hasNext) {
    QueryRecord.hasNext = hasNext;
  }

  public QueryRecord(Configuration conf) {
    this.conf = conf;
  }

  /**
   * Query {@link com.cloudera.bigdata.analysis.query.Query} object. The query
   * object represents an query request which tell back-end what kind of capture
   * records to return, i.e.: conditions, result limit, order and order-by
   * column.
   * 
   * @see com.cloudera.bigdata.analysis.query.Query
   * @param query
   *          the query object.
   * @return the results.
   */
  public List<StringBuffer> query(final Query query) {
    String queryTable = new String();
    queryTable = IndexUtil
        .convertByteArrayToString(query.getTable().getBytes());
    if (!IndexUtil.isIndexConfAvailableForQuery(queryTable)) {
      ETLException
          .handle("Failed to query data because there is index query configuration error!");
    }
    if (LOG.isDebugEnabled()) {
      LOG.debug("query : " + query.toString());
    }
    QueryResult queryResult = null;
    HTable hTable = null;
    try {
      hTable = new HTableNew(conf, queryTable);
      if (LOG.isDebugEnabled()) {
        LOG.debug("before query coprocessor table " + queryTable);
      }
      Map<byte[], QueryTypeProto.QueryResult> queryResults = hTable
          .coprocessorService(
              RecordServiceProto.RecordService.class,
              null,
              null,
              new Batch.Call<RecordServiceProto.RecordService, QueryTypeProto.QueryResult>() {
                @Override
                public QueryTypeProto.QueryResult call(
                    RecordServiceProto.RecordService instance)
                    throws IOException {
                  BlockingRpcCallback<QueryTypeProto.QueryResult> rpcCallback = new BlockingRpcCallback<QueryTypeProto.QueryResult>();
                  QueryTypeProto.Query qp = ProtoUtil.toQueryProto(query);
                  instance.query(null, qp, rpcCallback);
                  return rpcCallback.get();
                }
              });
      if (LOG.isDebugEnabled()) {
        LOG.debug("Returned QueryResult Count:" + queryResults.size());
        Set<Map.Entry<byte[], QueryTypeProto.QueryResult>> entries = queryResults
            .entrySet();
        for (Map.Entry<byte[], QueryTypeProto.QueryResult> entry : entries) {
          LOG.debug("KEY:" + Bytes.toString(entry.getKey()));
          LOG.debug("QueryResult:" + entry.getValue());
        }
      }
      queryResult = QueryResult.merge(queryResults.values());
      if (LOG.isDebugEnabled()) {
        LOG.debug("query merge : " + queryResult.toString());
      }
    } catch (Throwable throwable) {
      LOG.error(throwable.getMessage(), throwable);
      throwable.printStackTrace();
      throw new RuntimeException(throwable);
    } finally {
      if (hTable != null) {
        try {
          hTable.close();
        } catch (IOException e) {
          LOG.error("IO Exception! Can not close table " + queryTable, e);
          e.printStackTrace();
          throw new RuntimeException(e);
        }
      }
    }

    List<StringBuffer> records = new ArrayList<StringBuffer>();
    for (ClientProtos.Result record : queryResult.proto.getRecordsList()) {
      Result recordResult = ProtobufUtil.toResult(record);
      records.add(buildRecord(recordResult));
    }
    // No matter whether searched by index, all results have to be sorted!!
    // although the "Map<byte[],QueryResult> queryResults" are sorted by region
    // number,
    // but it does not mean any record from region 1 must sort before one from
    // region 2!
    // i.e. record 1: [0002,370102,1386901010,A00011] come from region 1,
    // record 2: [4001,370101,1386901006,A00005] come from region 2, sort-by:
    // locId, order: asc,
    // although record 2 come from region 2, but it should place before record
    // 1. so, sorting is required.
    // String orderBy = Bytes.toString(query.getOrderByQualifier().getBytes());
    if (query.isOrdered()) {
      ByteArray orderByQualifier = query.getOrderByQualifier();
      Order order = query.getOrder();
      String orderByField = Bytes.toString(orderByQualifier.getBytes());
      if (order == Order.ASC) {
        Collections.sort(records, new BeanComparator(orderByField));
      } else {
        Collections.sort(records, new ReverseComparator(new BeanComparator(
            orderByField)));
      }
      if (records.size() > query.getResultsLimit()) {
        records.subList(0, query.getResultsLimit());
      }
    }
    return records;
  }

  /**
   * Query {@link com.cloudera.bigdata.analysis.query.Query} object. The query
   * object represents an query request which tell back-end what kind of capture
   * records to return, i.e.: conditions, result limit, order and order-by
   * column.
   * 
   * @see com.cloudera.bigdata.analysis.query.Query
   * @param query
   *          the query object.
   * @return the results.
   */
  public QueryResult queryPagedHBase(final Query query) {
    String queryTable = new String();
    queryTable = IndexUtil
        .convertByteArrayToString(query.getTable().getBytes());
    if (!IndexUtil.isIndexConfAvailableForQuery(queryTable)) {
      ETLException
          .handle("Failed to query data because there is index query configuration error!");
    }
    if (LOG.isDebugEnabled()) {
      LOG.debug("query : " + query.toString());
    }

    HTableNew hTable = null;
    QueryTypeProto.QueryResult.Builder builder = null;
    try {
      hTable = new HTableNew(conf, queryTable);
      if (LOG.isDebugEnabled()) {
        LOG.debug("before query coprocessor table " + queryTable);
      }

      int resultsLimit = query.getResultsLimit();
      int count = 0;
      int regionsNum = hTable.getRegionLocations().size();

      ArrayList<String> regionStartKeys = IndexUtil
          .generateSplitKey(regionsNum);

      // start from the last queriedRegionsNum + 1
      boolean finishedThisPage = false;
      int i = QueryRecord.getQueriedRegionsNum();
      BlockingRpcCallback<QueryTypeProto.QueryResult> rpcCallback = new BlockingRpcCallback<QueryTypeProto.QueryResult>();
      QueryTypeProto.Query qp = ProtoUtil.toQueryProto(query);

      for (; i < regionsNum; i++) {
        finishedThisPage = false;
        // execute coprocessor on each region
        hTable.coprocessorProxy(RecordServiceProto.RecordService.class,
            regionStartKeys.get(i).getBytes()).query(null, qp, rpcCallback);
        QueryTypeProto.QueryResult queryResultPro = rpcCallback.get();
        if (LOG.isDebugEnabled()) {
          LOG.debug("Returned QueryResult Count:"
              + queryResultPro.getRecordsList().size());
          LOG.debug("QueryResult:" + queryResultPro.getRecordsList().toString());
        }

        if (queryResultPro.getRecordsList().size() == 0) {
          continue;
        } else {
          SearchMode searchMode = queryResultPro.getSearchMode();
          builder = QueryTypeProto.QueryResult.newBuilder();
          builder.setQuery(ProtoUtil.toQueryProto(query));
          builder.setSearchMode(searchMode);
          if (searchMode == QueryTypeProto.SearchMode.INDEX_BASED_SEARCH) {
            int j = 0;
            List<ClientProtos.Result> records = queryResultPro.getRecordsList();
            if (count < resultsLimit) {
              // start from the last queriedRecordsNumOnRegion + 1
              j = (i == QueryRecord.getQueriedRegionsNum() ? QueryRecord
                  .getQueriedRecordsNumOnRegion() : 0);
              for (; j < records.size(); j++) {
                if (count < resultsLimit) {
                  builder.addRecords(records.get(j));
                  count++;
                } else {
                  if (j < records.size() - 1
                      || (j == (records.size() - 1) && (i < regionsNum))) {
                    QueryRecord.setHasNext(true);
                    QueryRecord.setQueriedRegionsNum(i);
                    QueryRecord.setQueriedRecordsNumOnRegion(j);
                    finishedThisPage = true;
                    break;
                  }
                  // the last one
                  if (j == records.size() - 1 && i == regionsNum - 1) {
                    QueryRecord.setHasNext(false);
                    finishedThisPage = true;
                    break;
                  }
                }
              }
            }
            if (!finishedThisPage && j == records.size() && i < regionsNum - 1
                && count >= resultsLimit) {
              QueryRecord.setHasNext(true);
              QueryRecord.setQueriedRegionsNum(i + 1);
              QueryRecord.setQueriedRecordsNumOnRegion(0);
              finishedThisPage = true;
              break;
            }
            if (finishedThisPage) {
              break;
            }
            if (i == regionsNum - 1) {
              QueryRecord.setHasNext(false);
              break;
            }

          } else {// for full-table scan search, we have to all all results.
            builder.addAllRecords(queryResultPro.getRecordsList());
          }
        }
      }
      if (i == regionsNum) {
        QueryRecord.setHasNext(false);
      }

      if (LOG.isDebugEnabled()) {
        LOG.debug("query paged : " + builder.getRecordsList().toString());
      }
    } catch (Throwable throwable) {
      LOG.error(throwable.getMessage(), throwable);
      throwable.printStackTrace();
      throw new RuntimeException(throwable);
    } finally {
      if (hTable != null) {
        try {
          hTable.close();
        } catch (IOException e) {
          LOG.error("IO Exception! Can not close table " + queryTable, e);
          e.printStackTrace();
          throw new RuntimeException(e);
        }
      }
    }

    return new QueryResult(builder.build());
  }

  /**
   * Query {@link com.cloudera.bigdata.analysis.query.Query} object. The query
   * object represents an query request which tell back-end what kind of capture
   * records to return, i.e.: conditions, result limit, order and order-by
   * column.
   * 
   * @see com.cloudera.bigdata.analysis.query.Query
   * @param query
   *          the query object.
   * @return the results.
   */
  public QueryResult queryHBase(final Query query) {
    String queryTable = new String();
    queryTable = IndexUtil
        .convertByteArrayToString(query.getTable().getBytes());
    if (!IndexUtil.isIndexConfAvailableForQuery(queryTable)) {
      ETLException
          .handle("Failed to query data because there is index query configuration error!");
    }
    if (LOG.isDebugEnabled()) {
      LOG.debug("query : " + query.toString());
    }
    QueryResult queryResult = null;
    HTable hTable = null;
    try {
      // override HTable to fix HBase 0.94.1 TreeMap sync issue
      hTable = new HTableNew(conf, queryTable);
      if (LOG.isDebugEnabled()) {
        LOG.debug("before query coprocessor table " + queryTable);
      }
      Map<byte[], QueryTypeProto.QueryResult> queryResults = hTable
          .coprocessorService(
              RecordServiceProto.RecordService.class,
              null,
              null,
              new Batch.Call<RecordServiceProto.RecordService, QueryTypeProto.QueryResult>() {
                @Override
                public QueryTypeProto.QueryResult call(
                    RecordServiceProto.RecordService instance)
                    throws IOException {
                  BlockingRpcCallback<QueryTypeProto.QueryResult> rpcCallback = new BlockingRpcCallback<QueryTypeProto.QueryResult>();
                  QueryTypeProto.Query qp = ProtoUtil.toQueryProto(query);
                  instance.query(null, qp, rpcCallback);
                  return rpcCallback.get();
                }
              });
      if (LOG.isDebugEnabled()) {
        LOG.debug("Returned QueryResult Count:" + queryResults.size());
        Set<Map.Entry<byte[], QueryTypeProto.QueryResult>> entries = queryResults
            .entrySet();
        for (Map.Entry<byte[], QueryTypeProto.QueryResult> entry : entries) {
          LOG.debug("KEY:" + Bytes.toString(entry.getKey()));
          LOG.debug("QueryResult:" + entry.getValue());
        }
      }
      queryResult = QueryResult.merge(queryResults.values());
      if (LOG.isDebugEnabled()) {
        LOG.debug("query merge : " + queryResult.toString());
      }
    } catch (Throwable throwable) {
      LOG.error(throwable.getMessage(), throwable);
      throwable.printStackTrace();
      throw new RuntimeException(throwable);
    } finally {
      if (hTable != null) {
        try {
          hTable.close();
        } catch (IOException e) {
          LOG.error("IO Exception! Can not close table " + queryTable, e);
          e.printStackTrace();
          throw new RuntimeException(e);
        }
      }
    }

    return queryResult;
  }

  public static StringBuffer buildRecord(Result result) {
    StringBuffer record = new StringBuffer();
    String delimiter = "|";
    record.append(Bytes.toString(result.getRow()));
    record.append(delimiter);
    record.append(Bytes.toString(result.getValue(Constants.FAMILY_F,
        Bytes.toBytes("q1"))));
    record.append(delimiter);
    record.append(Bytes.toString(result.getValue(Constants.FAMILY_F,
        Bytes.toBytes("q2"))));
    return record;
  }

  public static void refreshIndexMap(Configuration conf) {
    conf.addResource(new Path(Constants.CORE_SITE_XML));
    try {
      FileSystem fs = FileSystem.get(conf);
      Path tablePath = new Path(Constants.INDEX_CONF_DIR_HDFS);
      FileStatus files[] = fs.listStatus(tablePath);
      if (files == null || files.length == 0) {
        ETLException.handle("The dir " + Constants.INDEX_CONF_DIR_HDFS
            + " is not existed in HDFS");
      }
      for (FileStatus file : files) {
        System.out.println(file.getPath().getName());
        if (!file.isDir()) {
          Path path = file.getPath();
          String p = path.getName();

          int pos = p.lastIndexOf("_index-conf.xml");
          if (pos != -1) {
            final String tableName = p.substring(0, pos);
            System.out.println(tableName);

            // check whether table is existed or not
            hbaseAdmin = new HBaseAdmin(conf);
            if (!hbaseAdmin.tableExists(tableName)) {
              LOG.warn("The table [" + tableName + "] is not existed in HBase!");
            } else {
              HTable hTable = null;
              try {
                hTable = new HTableNew(conf, tableName);
                if (LOG.isDebugEnabled()) {
                  LOG.debug("before query coprocessor table " + tableName);
                }

                Map<byte[], RecordServiceProto.RefreshResponse> refreshResults = hTable
                    .coprocessorService(
                        RecordServiceProto.RecordService.class,
                        null,
                        null,
                        new Batch.Call<RecordServiceProto.RecordService, RecordServiceProto.RefreshResponse>() {
                          @Override
                          public RecordServiceProto.RefreshResponse call(
                              RecordServiceProto.RecordService instance)
                              throws IOException {
                            if (LOG.isDebugEnabled()) {
                              LOG.debug("refresh call");
                              LOG.debug("Coprocessor  Thread: ["
                                  + Thread.currentThread().getName() + "]");
                            }
                            BlockingRpcCallback<RecordServiceProto.RefreshResponse> rpcCallback = new BlockingRpcCallback<RecordServiceProto.RefreshResponse>();
                            RecordServiceProto.RefreshRequest rf = ProtoUtil
                                .toRefreshRequestProto(tableName);
                            instance.refreshIndexMap(null, rf, rpcCallback);
                            return rpcCallback.get();
                          }
                        });

                if (LOG.isDebugEnabled()) {
                  LOG.debug("Returned RefreshResponse Count:"
                      + refreshResults.size());
                  Set<Map.Entry<byte[], RecordServiceProto.RefreshResponse>> entries = refreshResults
                      .entrySet();
                  for (Map.Entry<byte[], RecordServiceProto.RefreshResponse> entry : entries) {
                    LOG.debug("KEY:" + Bytes.toString(entry.getKey()));
                    LOG.debug("RefreshResponse:" + entry.getValue());
                  }
                }
              } catch (Throwable throwable) {
                LOG.error(throwable.getMessage(), throwable);
                throwable.printStackTrace();
                throw new RuntimeException(throwable);
              } finally {
                if (hTable != null) {
                  try {
                    hTable.close();
                  } catch (IOException e) {
                    LOG.error("IO Exception! Can not close table " + tableName,
                        e);
                    e.printStackTrace();
                    throw new RuntimeException(e);
                  }
                }
              }
            }
          }
        }
      }
    } catch (IOException e) {
      // TODO Auto-generated catch block
      e.printStackTrace();
    }
  }

  public static void main(String args[]) {
    Configuration conf = HBaseConfiguration.create();
    if (args[0].equals("refreshindex")) {
      refreshIndexMap(conf);
      System.out.println("refreshindex success");
      System.exit(0);
    }

    Options options = buildOptions();
    CommandLineParser parser = new BasicParser();

    CommandLine commandLine = null;
    try {
      commandLine = parser.parse(options, args);
    } catch (ParseException e) {
      e.printStackTrace();
    }

    Query query = new Query();
    String table = commandLine.getOptionValue("table");
    query.setTable(new ByteArray(Bytes.toBytes(table)));
    ConditionExpressionParser conditionExpressionParser = new ConditionExpressionParser();
    Condition condition = conditionExpressionParser.parse(table,
        commandLine.getOptionValue("condition"), false);
    query.setCondition(condition);
    if (LOG.isDebugEnabled()) {
      LOG.debug("set condition " + condition.toString());
    }
    String orderByColumnFamily = commandLine
        .getOptionValue("orderByColumnFamily");
    String orderByQualifier = commandLine.getOptionValue("orderByQualifier");
    String order = commandLine.getOptionValue("order");
    if (StringUtils.isNotEmpty(orderByColumnFamily)
        && StringUtils.isNotEmpty(orderByQualifier)
        && StringUtils.isNotEmpty(order)) {
      query.setOrderByColumnFamily(new ByteArray(Bytes
          .toBytes(orderByColumnFamily)));
      query.setOrderByQualifier(new ByteArray(Bytes.toBytes(orderByQualifier)));
      query.setOrder(Order.fromCode(Integer.parseInt(order)));
    }
    query.setResultsLimit(Integer.parseInt(commandLine
        .getOptionValue("resultsLimit")));
    query.setPaged(Boolean.parseBoolean(commandLine.getOptionValue("paged")));
    StopWatch stopWatch = new StopWatch();
    stopWatch.start();

    QueryRecord q = new QueryRecord(conf);
    QueryResult qr = new QueryResult();

    int page = 0;

    if (query.isPaged()) {
      while (true) {
        page++;
        // query next pages
        qr = q.queryPagedHBase(query);
        if (qr.proto.getRecordsList().size() != 0) {
          System.out.println("Page " + page + " Query Result:");
          System.out.println("Total Size: " + qr.proto.getRecordsList().size());
        }
        if (!QueryRecord.getHasNext()) {
          break;
        }
      }
      System.out.println("----Finished all query pages.----");
      System.out.println("Time Used: " + stopWatch.getTime() + "ms");
    } else {
      qr = q.queryHBase(query);
      System.out.println("Query Result:");
      System.out.println("Total Size: " + qr.proto.getRecordsList().size());
      System.out.println("Time Used: " + stopWatch.getTime() + "ms");
    }
    stopWatch.stop();
  }

  public static Options buildOptions() {
    Options options = new Options();
    options.addOption("create", false, "Create its tables.");
    options.addOption("recreate", false, "Recreate its tables.");
    options.addOption("regionQuantity", true, "Region quantity of its tables.");
    options.addOption("indexInMemory", true, "Load all indexes into memory.");
    options.addOption("drop", false, "Drop its tables.");
    options.addOption("search", false, "Query its tables.");
    options
        .addOption("put", true,
            "Put an entry of CaptureRecord with rowKeyPrefix,locationId,captureTime,plate.");
    options
        .addOption("get", true,
            "Get an entry of CaptureRecord with rowKeyPrefix,locationId,captureTime,plate.");
    options.addOption("query", false, "Query records.");
    options.addOption("table", true, "Query: table.");
    options.addOption("condition", true, "Query: condition");
    options.addOption("paged", true, "Query: paged.");
    options.addOption("orderByColumnFamily", true,
        "Query: orderByColumnFamily.");
    options.addOption("orderByQualifier", true, "Query: orderByQualifier.");
    options.addOption("order", true, "Query: order.");
    options.addOption("resultsLimit", true, "Query: resultsLimit.");
    options
        .addOption(
            "delete",
            true,
            "Delete an entry of CaptureRecord with rowKeyPrefix,locationId,captureTime,plate.");
    options
        .addOption(
            "import",
            true,
            "Import random generated records into its tables. "
                + "Format: -import threadCount,amountPerThread,batchSize,writeBufferSize(MB),startTime,endTime");
    return options;
  }

  private static void validateCommandLine(CommandLine commandLine) {

  }

}
