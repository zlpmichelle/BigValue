package com.cloudera.bigdata.analysis.dataload.etl;

public class CatCommand {
  private String catExecutable;
  private String catSourceFile1;
  private String catSourceFile2;
  private String catTarget;

  public CatCommand(String catExecutable, String catSourceFile1,
      String catSourceFile2, String catTarget) {
    this.catExecutable = catExecutable;
    this.catSourceFile1 = catSourceFile1;
    this.catSourceFile2 = catSourceFile2;
    this.catTarget = catTarget;
  }

  public String getCatExecutable() {
    return catExecutable;
  }

  public String getSourceFile1() {
    return catSourceFile1;
  }

  public String getSourceFile2() {
    return catSourceFile2;
  }

  public String getCatTarget() {
    return catTarget;
  }

  @Override
  public String toString() {
    return catSourceFile1 + "\n" + catSourceFile2 + "\n" + catTarget;
  }

}
