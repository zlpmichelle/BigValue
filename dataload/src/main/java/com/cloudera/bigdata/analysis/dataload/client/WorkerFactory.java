package com.cloudera.bigdata.analysis.dataload.client;

import org.apache.log4j.Logger;

public class WorkerFactory {

  private static final Logger LOGGER = Logger.getLogger(WorkerFactory.class);

  public static enum DriverType {
    MAPREDUCE, HIVE
  };

  public static enum SourceType {
    HDFS, HBASE
  };

  public static enum TargetType {
    HDFS, HBASE
  };

  public static Worker createWorker(DriverType driverType,
      SourceType sourceType, TargetType targetType) throws Exception {
    Worker worker = null;
    switch (driverType) {
    case MAPREDUCE:
      worker = new MapReduceDriver(sourceType, targetType)
          .createMapReduceWorker();
      break;
    case HIVE:
      worker = new HiveDriver("etl-hive-conf.properties");
      break;
    default:
      LOGGER.error("Unsupported ETLWorkerType: " + driverType);
    }
    return worker;
  }
}
