package com.cloudera.bigdata.analysis.dataload.io;

import java.util.Map;

import com.cloudera.bigdata.analysis.dataload.util.DataLoaderUtils;

public class ConfigReader {

  protected Map<String, String> confMap = null;

  public ConfigReader(String config) {
    // read configuration file to construct confMap
    this.confMap = DataLoaderUtils.readConfig(config);
  }

  public Map<String, String> getConfMap() {
    return confMap;
  }

  @Override
  public String toString() {
    StringBuilder sb = new StringBuilder();
    sb.append("ConfMap:").append(confMap);
    return sb.toString();
  }

  public static void main(String[] args) {
    ConfigReader cr = new ConfigReader("etl-hbase2hbase-conf.properties");
    System.out.println(cr.toString());
  }
}
