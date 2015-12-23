package com.cloudera.bigdata.analysis.dataload.client;

import java.io.IOException;

import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.fs.FileSystem;
import org.apache.hadoop.hbase.HBaseConfiguration;
import org.apache.hadoop.hbase.client.HBaseAdmin;
import org.apache.log4j.Logger;

import com.cloudera.bigdata.analysis.dataload.io.ConfigReader;

public abstract class BaseWorker implements Worker {
  private static final Logger LOGGER = Logger.getLogger(BaseWorker.class);

  protected String configFile;
  protected Configuration conf;
  protected FileSystem fs;
  protected static HBaseAdmin hbaseAdmin;
  protected ConfigReader configReader;

  public BaseWorker(String config) {
    this.configFile = config;
    this.configReader = new ConfigReader(config);
    try {
      conf = HBaseConfiguration.create();
      fs = FileSystem.get(conf);
      hbaseAdmin = new HBaseAdmin(conf);
    } catch (IOException ioe) {
      LOGGER.error("ERROR: " + ioe);
      throw new RuntimeException(ioe);
    }

  }

  protected abstract void doWork() throws Exception;
}
