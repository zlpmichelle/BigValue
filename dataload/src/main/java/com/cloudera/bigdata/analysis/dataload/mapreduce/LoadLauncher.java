package com.cloudera.bigdata.analysis.dataload.mapreduce;

import java.io.IOException;
import java.util.List;
import java.util.Properties;

import org.apache.hadoop.mapreduce.Job;

public interface LoadLauncher {
  /**
   * Get all configuration from config file.
   * 
   * @param props
   *          - Configuration properties.
   * @return True if configuration is successful.
   */
  public boolean conf(Properties props);

  public List<Job> launch() throws IOException, InterruptedException;

  public void cleanup() throws IOException;

  public boolean isComplete() throws IOException, InterruptedException;;

  public void waitForComplete() throws IOException, InterruptedException;;
}
