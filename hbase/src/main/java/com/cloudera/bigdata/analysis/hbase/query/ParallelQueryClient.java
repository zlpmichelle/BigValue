package com.cloudera.bigdata.analysis.hbase.query;

import java.io.BufferedReader;
import java.io.File;
import java.io.FileNotFoundException;
import java.io.FileReader;
import java.io.IOException;
import java.util.ArrayList;
import java.util.List;
import java.util.Properties;
import java.util.concurrent.Callable;
import java.util.concurrent.ExecutionException;
import java.util.concurrent.Executors;
import java.util.concurrent.Future;
import java.util.concurrent.ThreadFactory;
import java.util.concurrent.ThreadPoolExecutor;

import org.apache.commons.cli.BasicParser;
import org.apache.commons.cli.CommandLine;
import org.apache.commons.cli.CommandLineParser;
import org.apache.commons.cli.Options;
import org.apache.commons.cli.ParseException;
import org.apache.commons.lang.StringUtils;
import org.apache.commons.logging.Log;
import org.apache.commons.logging.LogFactory;
import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.hbase.HBaseConfiguration;
import org.apache.hadoop.hbase.util.Bytes;
import org.apache.hadoop.hdfs.util.ByteArray;

import scala.util.Random;

import com.cloudera.bigdata.analysis.hbase.Constants;
import com.cloudera.bigdata.analysis.index.QueryRecord;
import com.cloudera.bigdata.analysis.index.util.ConditionExpressionParser;
import com.cloudera.bigdata.analysis.query.Condition;
import com.cloudera.bigdata.analysis.query.Order;
import com.cloudera.bigdata.analysis.query.Query;
import com.google.common.util.concurrent.ThreadFactoryBuilder;

public class ParallelQueryClient {

  static final Log LOG = LogFactory.getLog(ParallelQueryClient.class);
  private Configuration conf;

  private int threadNum;
  private String threadName;
  private String filePath;
  private Options options;
  private boolean isRandom = false;
  private CommandLineParser parser;
  private CommandLine commandLine;

  private Random random = new Random();

  public ParallelQueryClient() {
    options = QueryRecord.buildOptions();
    parser = new BasicParser();
  }

  public void parallelQuery() {
    ThreadFactoryBuilder builder = new ThreadFactoryBuilder();
    builder.setNameFormat(threadName);
    ThreadFactory factory = builder.build();
    ThreadPoolExecutor executor = (ThreadPoolExecutor) Executors
        .newFixedThreadPool(threadNum, factory);

    try {
      List<String> queryList = getArgs();
      List<Future<Long>> futures = new ArrayList<Future<Long>>();
      long starttime = System.currentTimeMillis();
      for (int i = 0; i < threadNum; i++) {
        QueryRecord client = new QueryRecord(conf);
        // randomly fetch a Query from the query pool
        int j = random.nextInt(queryList.size() - 1);
        // construct a Query
        Query query = constructQuery(queryList.get(j).split(" "));
        // context.setArgs(argsList.get(i));
        Callable<Long> callable = new QueryCallable(query, queryList.get(j), i,
            client);
        Future<Long> future = executor.submit(callable);
        futures.add(future);
      }
      long max = 0;
      long all = 0;
      for (Future<Long> future : futures) {
        long time = future.get();
        if (time > max)
          max = time;
        all += time;
      }
      // LOG.info("====throughput "+queryTime*threadNum*1000/max);
      LOG.info("======Average " + all / threadNum + "ms");
      while (true) {
        if (executor.getCompletedTaskCount() == threadNum) {
          executor.shutdown();
          break;
        }
      }

      long endtime = System.currentTimeMillis();
      LOG.info("All used time " + (endtime - starttime) + "ms");
    } catch (SecurityException e) {
      e.printStackTrace();
      LOG.info(e.getMessage());
      LOG.info(e.getCause());
    } catch (IllegalArgumentException e) {
      e.printStackTrace();
      LOG.info(e.getMessage());
      LOG.info(e.getCause());
    } catch (InterruptedException e) {
      e.printStackTrace();
      LOG.info(e.getMessage());
    } catch (ExecutionException e) {
      e.printStackTrace();
      LOG.info(e.getMessage());
    }

  }

  private Query constructQuery(String[] args) {
    commandLine = null;
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
        commandLine.getOptionValue("condition"), isRandom);
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

    return query;
  }

  public void init(String args) {
    Properties p = new Properties();
    try {
      p.load(new FileReader(args));
      this.conf = HBaseConfiguration.create();
      this.threadNum = Integer.parseInt(p.getProperty(Constants.THREADNUM));
      this.threadName = p.getProperty(Constants.THREADNAME, "query");
      File file = new File(args);
      String propertyFolder = file.getAbsoluteFile().getParent();
      this.filePath = propertyFolder + "/" + p.getProperty(Constants.FILEPATH);
      this.isRandom = Boolean.parseBoolean(p.getProperty(Constants.RANDOM));
      LOG.info("parallel query file path is " + filePath);
    } catch (FileNotFoundException e) {
      e.printStackTrace();
      LOG.info(e.getMessage());
    } catch (IOException e) {
      e.printStackTrace();
      LOG.info(e.getMessage());
    }
  }

  public List<String> getArgs() {
    BufferedReader br;
    try {
      br = new BufferedReader(new FileReader(filePath));

      List<String> queryList = new ArrayList<String>();
      String s;

      while ((s = br.readLine()) != null) {
        if (!s.equals("")) {
          queryList.add(s.trim());
        }
      }
      br.close();
      return queryList;
    } catch (FileNotFoundException e1) {
      e1.printStackTrace();
    } catch (IOException e) {
      e.printStackTrace();
    }
    return null;
  }

  public static void main(String[] args) {
    ParallelQueryClient client = new ParallelQueryClient();
    client.init(args[0]);
    client.parallelQuery();
  }

}