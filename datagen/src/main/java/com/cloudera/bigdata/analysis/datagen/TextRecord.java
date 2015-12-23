package com.cloudera.bigdata.analysis.datagen;

public class TextRecord implements Record {
  private String line;
  private long lineLength;
  public TextRecord(String recordLine, long length){
    this.line = recordLine;
    this.lineLength = length;
  }
  
  @Override
  public String getAsString() {
    return line;
  }

  @Override
  public long getLength() {
    return lineLength;
  }

}
