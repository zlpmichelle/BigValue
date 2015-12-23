package com.cloudera.bigdata.analysis.query;

import java.io.IOException;
import java.util.Collection;
import java.util.List;

import org.apache.hadoop.hbase.ipc.ServerRpcController;
import org.apache.hadoop.hbase.protobuf.generated.ClientProtos;
import org.apache.hadoop.util.StringUtils;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import com.cloudera.bigdata.analysis.index.protobuf.ProtoUtil;
import com.cloudera.bigdata.analysis.index.protobuf.generated.QueryTypeProto;
import com.google.protobuf.RpcController;

/**
 * The returned query result from database. It contains initial query object and
 * a set of {@link org.apache.hadoop.hbase.client.Result}.
 */
public class QueryResult {

  private static final Logger LOG = LoggerFactory.getLogger(QueryResult.class);

  public QueryTypeProto.QueryResult proto;

  public QueryResult() {

  }

  public QueryTypeProto.QueryResult getProto() {
    return proto;
  }

  public void setProto(QueryTypeProto.QueryResult proto) {
    this.proto = proto;
  }

  public QueryResult(QueryTypeProto.QueryResult proto) {
    this.proto = proto;
  }

  /**
   * Merge already ordered QueryResult collections
   * 
   * @return
   */
  public static QueryResult merge(
      Collection<QueryTypeProto.QueryResult> queryResults) {
    if (!queryResults.iterator().hasNext()) {
      return null;
    }
    QueryTypeProto.QueryResult first = queryResults.iterator().next();
    QueryTypeProto.Query query = first.getQuery();
    Query q = ProtoUtil.toQuery(query);
    int resultsLimit = q.getResultsLimit();
    QueryTypeProto.SearchMode searchMode = first.getSearchMode();
    // For index-based search, the results between collections are naturally
    // ordered
    // for example, if returned value 1,2,3,4 come from 2 regions,
    // the return QueryResult collection must be: [1,2]:region1,[3,4]:region2
    // so just add them one by one until result limit.
    // TreeSet<Result> mergedResults = new TreeSet<Result>(
    // new ResultComparator(query.getOrderByColumnFamily(),
    // query.getOrderByQualifier(), query.getOrder()));
    QueryTypeProto.QueryResult.Builder builder = QueryTypeProto.QueryResult
        .newBuilder();
    builder.setQuery(query);
    builder.setSearchMode(searchMode);
    if (searchMode == QueryTypeProto.SearchMode.INDEX_BASED_SEARCH) {
      int counter = 0;
      for (QueryTypeProto.QueryResult queryResult : queryResults) {
        if (counter < resultsLimit) {
          List<ClientProtos.Result> records = queryResult.getRecordsList();
          for (ClientProtos.Result resultRecord : records) {
            if (counter < resultsLimit) {
              builder.addRecords(resultRecord);
              counter++;
            } else {
              break;
            }
          }
        } else {
          break;
        }
      }
    } else {// for full-table scan search, we have to all all results.
      for (QueryTypeProto.QueryResult queryResult : queryResults) {
        builder.addAllRecords(queryResult.getRecordsList());
      }

      // for (QueryResult queryResult : queryResults) {
      // for (Result resultRecord : queryResult.getRecords()) {
      // mergedResults.add(resultRecord);
      // if (mergedResults.size() > resultsLimit) {
      // mergedResults.pollLast();
      // }
      // }
      // }
    }
    // mergedQueryResult.setRecords(mergedResults);
    QueryTypeProto.QueryResult mergedQueryResult = builder.build();

    return new QueryResult(mergedQueryResult);
  }

  /*--------------------------------------------     Common Methods    ---------------------------------------------*/
  /**
   * Stores an exception encountered during RPC invocation so it can be passed
   * back through to the client.
   * 
   * @param controller
   *          the controller instance provided by the client when calling the
   *          service
   * @param ioe
   *          the exception encountered
   */
  public static void setControllerException(RpcController controller,
      IOException ioe) {
    if (controller != null) {
      if (controller instanceof ServerRpcController) {
        ((ServerRpcController) controller).setFailedOn(ioe);
      } else {
        controller.setFailed(StringUtils.stringifyException(ioe));
      }
    }
  }

}
