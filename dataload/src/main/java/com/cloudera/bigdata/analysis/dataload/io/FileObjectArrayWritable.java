package com.cloudera.bigdata.analysis.dataload.io;

import java.util.List;

import org.apache.hadoop.io.ArrayWritable;
import org.apache.hadoop.io.Writable;
import org.apache.hadoop.io.WritableComparable;

public class FileObjectArrayWritable extends ArrayWritable implements
    WritableComparable<FileObjectArrayWritable> {

  public FileObjectArrayWritable(Class<? extends FileObject> valueClass) {
    super(valueClass);
  }

  public FileObjectArrayWritable(FileObject[] files,
      Class<? extends FileObject> valueClass) {
    super(valueClass, files);
  }

  public FileObjectArrayWritable(List<FileObject> fileList,
      Class<? extends Writable> valueClass) {
    super(valueClass, fileList.toArray(new FileObject[] {}));
  }

  public void merge(FileObjectArrayWritable arrayWritable) {

  }

  @Override
  public int compareTo(FileObjectArrayWritable o) {
    FileObjectArrayWritable that = (FileObjectArrayWritable) o;
    Writable[] thisValues = get();
    Writable[] thatValues = that.get();

    if (thisValues == null) {
      if (thatValues == null) {
        return 0;
      } else {
        return -1;
      }
    } else {
      if (thatValues == null) {
        return 1;
      }
    }

    int compareLen = thisValues.length >= thatValues.length ? thatValues.length
        : thisValues.length;

    for (int i = 0; i < compareLen; i++) {
      FileObject thisValue = (FileObject) thisValues[i];
      FileObject thatValue = (FileObject) thatValues[i];

      int compareRes = thisValue.compareTo(thatValue);
      if (compareRes != 0) {
        return compareRes;
      }
    }

    if (thisValues.length > compareLen) {
      return 1;
    }

    if (thatValues.length > compareLen) {
      return -1;
    }

    return 0;
  }

}
