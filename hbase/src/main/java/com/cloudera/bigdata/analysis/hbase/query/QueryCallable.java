package com.cloudera.bigdata.analysis.hbase.query;

import java.util.concurrent.Callable;

import org.apache.commons.lang.time.StopWatch;

import com.cloudera.bigdata.analysis.index.QueryRecord;
import com.cloudera.bigdata.analysis.query.Query;
import com.cloudera.bigdata.analysis.query.QueryResult;

public class QueryCallable implements Callable<Long> {
  private Query query;
  private String queryStr;
  private int threadNum;
  private QueryRecord q;
  private QueryResult qr;
  private int page = 0;
  private long oneQueryTime = 0;

  public QueryCallable(Query query, String queryStr, int threadNum,
      QueryRecord q) {
    this.query = query;
    this.queryStr = queryStr;
    this.threadNum = threadNum;
    this.q = q;
  }

  @Override
  public Long call() throws Exception {
    QueryRecord.queriedRegionsNum = 0;
    QueryRecord.queriedRecordsNumOnRegion = 0;
    oneQueryTime = 0;

    StopWatch stopWatch = new StopWatch();
    stopWatch.start();
    System.out.println("++++ Thread-" + threadNum + " is starting.");
    qr = new QueryResult();
    page = 0;
    if (query.isPaged()) {
      while (true) {
        page++;
        // query next pages
        qr = q.queryPagedHBase(query);
        if (qr.proto.getRecordsList().size() != 0) {
          System.out.println("Page " + page + " Query Result for " + queryStr
              + ":");
          System.out.println("Total Size: " + qr.proto.getRecordsList().size());
        }
        if (!QueryRecord.getHasNext()) {
          break;
        }
      }
      oneQueryTime = stopWatch.getTime();
      System.out.println("----Finished all query pages.----");
      System.out.println("Time Used: " + oneQueryTime + "ms");
    } else {
      qr = q.queryHBase(query);
      oneQueryTime = stopWatch.getTime();
      System.out.println("Query Result for Thread-" + threadNum + " : "
          + queryStr + ":");
      System.out.println("Total Size: " + qr.proto.getRecordsList().size());
      System.out.println("Time Used: " + oneQueryTime + "ms");
    }
    stopWatch.stop();
    return oneQueryTime;
  }
}
