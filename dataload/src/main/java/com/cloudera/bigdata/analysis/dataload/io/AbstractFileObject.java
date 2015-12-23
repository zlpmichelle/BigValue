package com.cloudera.bigdata.analysis.dataload.io;

public abstract class AbstractFileObject implements FileObject {

  public String getCanonicalPath() {
    return null;
  }

  public String getHostname() {
    return null;
  }
}
