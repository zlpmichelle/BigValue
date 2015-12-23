package com.cloudera.bigdata.analysis.dataload.hive;

public class HiveCommand {

  private String hiveExecutable;
  private String hiveOption;
  private String hiveSetting;
  private String hiveScript;

  public HiveCommand(String hiveExecutable, String hiveOption,
      String hiveSetting, String hiveScript) {
    this.hiveExecutable = hiveExecutable;
    this.hiveOption = hiveOption;
    this.hiveSetting = hiveSetting;
    this.hiveScript = hiveScript;
  }

  public String getHiveExecutable() {
    return hiveExecutable;
  }

  public String getHiveOption() {
    return hiveOption;
  }

  public String getHiveSetting() {
    return hiveSetting;
  }

  public String getHiveScript() {
    return hiveScript;
  }

  @Override
  public String toString() {
    return hiveSetting + "\n" + hiveScript;
  }
}
