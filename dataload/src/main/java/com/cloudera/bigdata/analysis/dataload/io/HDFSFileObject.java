package com.cloudera.bigdata.analysis.dataload.io;

import java.io.DataInput;
import java.io.DataOutput;
import java.io.IOException;

import org.apache.commons.logging.Log;
import org.apache.commons.logging.LogFactory;

public class HDFSFileObject implements FileObject {

  public static final Log LOG = LogFactory.getLog(HDFSFileObject.class);

  private String fileName; 

  private String path;

  private String hostName;
  
  private long size;

  public HDFSFileObject() {

  }

  public HDFSFileObject(String hostName, String path, String fileName) {
    this.hostName = hostName;
    this.path = path;
    this.fileName = fileName;
  }

  @Override
  public void readFields(DataInput arg0) throws IOException {
    this.path = arg0.readUTF();
    this.fileName = arg0.readUTF();
    this.hostName = arg0.readUTF();
    this.size = arg0.readLong();
  }

  @Override
  public void write(DataOutput arg0) throws IOException {
    arg0.writeUTF(this.path);
    arg0.writeUTF(this.fileName);
    arg0.writeUTF(this.hostName);
    arg0.writeLong(this.size);
  }

  @Override
  public int compareTo(FileObject arg0) {
    HDFSFileObject hdfsFileObject = (HDFSFileObject) arg0;
    if (this.path.equals(hdfsFileObject.getPath())
        && this.fileName.equals(hdfsFileObject.getFileName())
        && this.hostName.equals(hdfsFileObject.getHostname())) {
      return 0;
    }
    return -1;
  }

  @Override
  public String getName() {
    return fileName;
  }

  @Override
  public String getCanonicalPath() {
    return this.path + "/" + fileName;
  }

  @Override
  public long getSize() {
    return this.size;
  }

  public void setSize(long size) {
    this.size = size;
  }

  @Override
  public String getHostname() {
    return this.hostName;
  }

  public void setHostname(String hostname) {
    this.hostName = hostname;
  }

  public String getFileName() {
    return fileName;
  }

  public String getPath() {
    return this.path;
  }

  public void setPath(String path) {
    this.path = path;
  }

  public String toString() {
    return "This hdfsFileObject - [ " + ": " + path + "/" + fileName
        + " ]";
  }

}
