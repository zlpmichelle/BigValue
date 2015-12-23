package com.cloudera.bigdata.analysis.dataload.mapreduce;

import org.apache.hadoop.hbase.HBaseConfiguration;
import org.apache.hadoop.hbase.mapreduce.LoadIncrementalHFiles;
import org.apache.hadoop.util.ToolRunner;

public class LoadHfile {
  private static final String USAGE_STR = "com.cloudera.bigdata.analysis.dataload.mapreduce.LoadHfile <hbaseGeneratedHfilesOutputPath> <tableName>";

  public static void loadIncrementalHFiles(
      String hbaseGeneratedHfilesOutputPath, String tableName, long startTime)
      throws Exception {
    // LoadIncrementalHFiles to bulk load data from hfile into hbase table
    if (hbaseGeneratedHfilesOutputPath != null) {
      String[] args = { hbaseGeneratedHfilesOutputPath, tableName };
      int ret = ToolRunner.run(
          new LoadIncrementalHFiles(HBaseConfiguration.create()), args);
      System.out.println("Successfully Bulk Load data into HBase table!");
      System.out.println("The Total Time of Bulk Load is: "
          + (System.currentTimeMillis() - startTime) + " millisecond!");
      System.exit(ret);
    }
  }

  public static void main(String args[]) throws Exception {
    if (args.length < 2) {
      System.out.println(USAGE_STR);
    }

    String hbaseGeneratedHfilesOutputPath = args[0];
    String tableName = args[1];
    long startTime = System.currentTimeMillis();

    // LoadIncrementalHFiles to bulk load data from hfile into hbase table
    loadIncrementalHFiles(hbaseGeneratedHfilesOutputPath, tableName, startTime);
  }
}
