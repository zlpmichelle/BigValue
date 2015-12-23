package com.cloudera.bigdata.analysis.dataload.io;

import java.io.DataInput;
import java.io.DataOutput;
import java.io.IOException;

import org.apache.hadoop.io.WritableComparable;

public class FileRowWritable implements WritableComparable {
  private String fileName;
  private long row;

  public FileRowWritable() {
    this.fileName = "";
    this.row = 0;
  }

  public FileRowWritable(String fileName, long row) {
    this.fileName = fileName;
    this.row = row;
  }

  public String getFileName() {
    return fileName;
  }

  public void setFileName(String fileName) {
    this.fileName = fileName;
  }

  public long getRow() {
    return row;
  }

  public void setRow(long row) {
    this.row = row;
  }

  public String toString() {
    return fileName + "," + row;
  }

  @Override
  public void write(DataOutput out) throws IOException {
    // TODO Auto-generated method stub
    out.writeUTF(fileName);
    out.writeLong(row);
  }

  @Override
  public void readFields(DataInput in) throws IOException {
    // TODO Auto-generated method stub
    this.fileName = in.readUTF();
    this.row = in.readLong();
  }

  @Override
  public int compareTo(Object otherWritable) {
    // TODO Auto-generated method stub
    if (otherWritable instanceof FileRowWritable) {
      FileRowWritable other = (FileRowWritable) otherWritable;

      if (other.fileName.equals(this.fileName) && other.row == this.row) {
        return 0;
      } else {
        return 1;
      }
    } else {
      return 1;
    }
  }

}
