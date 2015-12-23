package com.cloudera.bigdata.analysis.dataload;

import java.util.concurrent.atomic.AtomicBoolean;
import java.util.concurrent.atomic.AtomicLong;

import com.cloudera.bigdata.analysis.dataload.io.FileObject;

/**
 * FileInfo only logs some processing info while reading
 * from the file.
 */
public class FileInfo {
  private FileObject fileObj;
  private AtomicBoolean hasError;
  private AtomicLong unprocessedRecord;
  private AtomicLong totalRecord;
  private AtomicBoolean isEOF;

  public FileInfo(FileObject fileObj) {
    this.fileObj = fileObj;
    hasError = new AtomicBoolean(false);
    unprocessedRecord = new AtomicLong(0);
    totalRecord = new AtomicLong(0);
    isEOF = new AtomicBoolean(false);
  }

  public FileObject getFileObject() {
    return fileObj;
  }

  public boolean hasError() {
    return hasError.get();
  }

  public void setError(Exception e) {
    hasError.set(true);
  }

  public void addRecord() {
    unprocessedRecord.incrementAndGet();
    totalRecord.incrementAndGet();
  }

  public void handleRecord() {
    unprocessedRecord.decrementAndGet();
  }

  public long getUnhandledRecordCount() {
    return unprocessedRecord.get();
  }

  public void setEOF(boolean value) {
    isEOF.getAndSet(value);
  }

  /**
   * Check if all the records in this file have been processed. We cannot merely
   * check the unprocessedRecord, as this could be zero if the process rate is
   * equal to read rate.
   */
  public boolean isRemovable() {
    return isEOF.get() && unprocessedRecord.get() <= 0;
  }

  @Override
  public String toString() {
    return fileObj + "," + "hasError:" + hasError + ", "
        + "unprocessedRecord: " + unprocessedRecord.get() + ", "
        + "totalRecord: " + totalRecord.get();
  }

}
