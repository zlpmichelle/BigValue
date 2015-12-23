package com.cloudera.bigdata.analysis.dataload.client;

import org.apache.log4j.Logger;

import com.cloudera.bigdata.analysis.dataload.client.WorkerFactory.SourceType;
import com.cloudera.bigdata.analysis.dataload.client.WorkerFactory.TargetType;
import com.cloudera.bigdata.analysis.dataload.mapreduce.HBase2HBaseWorker;
import com.cloudera.bigdata.analysis.dataload.mapreduce.HBase2HdfsWorker;
import com.cloudera.bigdata.analysis.dataload.mapreduce.Hdfs2HBaseWorker;
import com.cloudera.bigdata.analysis.dataload.mapreduce.Hdfs2HdfsWorker;

public class MapReduceDriver {
  private static final Logger LOGGER = Logger.getLogger(MapReduceDriver.class);
  private SourceType sourceType;
  private TargetType targetType;

  public MapReduceDriver(SourceType sourceType, TargetType targetType)
      throws Exception {
    this.sourceType = sourceType;
    this.targetType = targetType;
  }

  public Worker createMapReduceWorker() throws Exception {
    Worker worker = null;
    switch (sourceType) {
    case HDFS:
      switch (targetType) {
      case HDFS:
        worker = new Hdfs2HdfsWorker("etl-hdfs2hdfs-conf.properties");
        break;
      case HBASE:
        worker = new Hdfs2HBaseWorker("etl-hdfs2hbase-conf.properties");
        break;
      default:
        LOGGER.error("Unsupported TargetType: " + targetType);
      }
    case HBASE:
      switch (targetType) {
      case HDFS:
        worker = new HBase2HdfsWorker("etl-hbase2hdfs-conf.properties");
        break;
      case HBASE:
        worker = new HBase2HBaseWorker("etl-hbase2hbase-conf.properties");
        break;
      default:
        LOGGER.error("Unsupported TargetType: " + targetType);
      }
    default:
      LOGGER.error("Unsupported ETLSourceType: " + sourceType);
    }
    return worker;
  }
}
