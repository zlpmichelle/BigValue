package com.cloudera.bigdata.analysis.dataload.client;

import java.util.HashMap;

import com.cloudera.bigdata.analysis.dataload.client.WorkerFactory.DriverType;
import com.cloudera.bigdata.analysis.dataload.client.WorkerFactory.SourceType;
import com.cloudera.bigdata.analysis.dataload.client.WorkerFactory.TargetType;

public class GeneralDataLoad {

  final static String NAME = "GeneralETLTool";
  private static HashMap<String, String> argsMap = new HashMap<String, String>();
  private static String driver, source, target;

  private static void help() {
    System.out.println("Usage: " + NAME
        + "Driver=[mapreduce hive] Source=[hdfs hbase] Target=[hdfs hbase]\n");
    System.exit(255);
  }

  public static void main(String[] args) throws Exception {
    if (args.length < 1) {
      help();
    }
    for (String arg : args) {
      String[] tokens = arg.split("=");
      if (tokens.length != 2) {
        continue;
      }
      argsMap.put(tokens[0], tokens[1]);
    }
    try {
      driver = argsMap.get("Driver");
      source = argsMap.get("Source");
      target = argsMap.get("Target");
    } catch (Exception e) {
      e.printStackTrace();
      help();
    }
    DriverType driverType = DriverType.valueOf(DriverType.class, driver);
    SourceType sourceType = SourceType.valueOf(SourceType.class, source);
    TargetType targetType = TargetType.valueOf(TargetType.class, target);
    Worker worker = WorkerFactory.createWorker(driverType, sourceType,
        targetType);
    worker.execute();
  }

}
