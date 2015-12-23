package com.cloudera.bigdata.analysis.datagen;

import java.io.OutputStream;

public interface GeneratorSink {
  public void sink(Record record);
  
  public void setOutputFolder(String outputDir);
  
  public OutputStream getOutputStream(String fileName);
}
