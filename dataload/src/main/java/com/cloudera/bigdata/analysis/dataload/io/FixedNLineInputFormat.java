package com.cloudera.bigdata.analysis.dataload.io;

import java.io.IOException;
import java.util.ArrayList;
import java.util.List;

import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.fs.FSDataInputStream;
import org.apache.hadoop.fs.FileStatus;
import org.apache.hadoop.fs.FileSystem;
import org.apache.hadoop.fs.Path;
import org.apache.hadoop.io.Text;
import org.apache.hadoop.mapreduce.lib.input.FileSplit;
import org.apache.hadoop.mapreduce.lib.input.NLineInputFormat;
import org.apache.hadoop.util.LineReader;

public class FixedNLineInputFormat extends NLineInputFormat {

  public static List<FileSplit> getSplitsForFile(FileStatus status,
      Configuration conf, int numLinesPerSplit) throws IOException {
    List<FileSplit> splits = new ArrayList<FileSplit>();
    Path fileName = status.getPath();
    if (status.isDir()) {
      throw new IOException("Not a file: " + fileName);
    }
    FileSystem fs = fileName.getFileSystem(conf);
    LineReader lr = null;
    try {
      FSDataInputStream in = fs.open(fileName);
      lr = new LineReader(in, conf);
      Text line = new Text();
      int numLines = 0;
      long begin = 0;
      long length = 0;
      int num = -1;
      while ((num = lr.readLine(line)) > 0) {
        numLines++;
        length += num;
        if (numLines == numLinesPerSplit) {
          // NLineInputFormat uses LineRecordReader, which always
          // reads
          // (and consumes) at least one character out of its upper
          // split
          // boundary. So to make sure that each mapper gets N lines,
          // we
          // move back the upper split limits of each split
          // by one character here.
          createFileSplit(fileName, begin, length);
          begin += length;
          length = 0;
          numLines = 0;
        }
      }
      if (numLines != 0) {
        splits.add(createFileSplit(fileName, begin, length));
      }
    } finally {
      if (lr != null) {
        lr.close();
      }
    }
    return splits;
  }

  protected static FileSplit createFileSplit(Path fileName, long begin,
      long length) {
    return (begin == 0) ? new FileSplit(fileName, begin, length - 1,
        new String[] {}) : new FileSplit(fileName, begin - 1, length,
        new String[] {});
  }
}
