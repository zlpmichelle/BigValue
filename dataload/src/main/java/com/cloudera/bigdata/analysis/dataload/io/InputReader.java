package com.cloudera.bigdata.analysis.dataload.io;

import java.io.IOException;
import java.util.ArrayList;
import java.util.List;

import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.fs.FSDataInputStream;
import org.apache.hadoop.fs.FileSystem;
import org.apache.hadoop.fs.Path;
import org.apache.hadoop.io.LongWritable;
import org.apache.hadoop.io.Writable;
import org.apache.hadoop.mapreduce.InputSplit;
import org.apache.hadoop.mapreduce.RecordReader;
import org.apache.hadoop.mapreduce.TaskAttemptContext;
import org.apache.hadoop.mapreduce.lib.input.FileSplit;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import com.cloudera.bigdata.analysis.dataload.Constants;

public class InputReader extends
    RecordReader<LongWritable, FileObjectArrayWritable> {
  private final static Logger LOG = LoggerFactory.getLogger(InputReader.class);

  private long start;
  private long length;
  private long pos;
  private Class<? extends FileObject> clazz;
  private List<FileObject> fileObjects;
  private FileObjectArrayWritable currentArrayWritable;

  public InputReader() {
  }

  @SuppressWarnings("unchecked")
  @Override
  public void initialize(InputSplit split, TaskAttemptContext context)
      throws IOException, InterruptedException {
    // TODO Auto-generated method stub
    FileSplit fileSplit = (FileSplit) split;
    start = fileSplit.getStart();
    pos = start;
    length = fileSplit.getLength();

    Path path = fileSplit.getPath();
    Configuration conf = context.getConfiguration();

    String className = conf.get(Constants.FILEOBJECT_CLASS_KEY);
    try {
      clazz = (Class<? extends FileObject>) Class.forName(className);
    } catch (ClassNotFoundException e) {
      LOG.error("", e);
    }

    FileSystem fileSystem = FileSystem.get(conf);
    FSDataInputStream inputStream = fileSystem.open(path);
    FileObjectArrayWritable arrayWritable = new FileObjectArrayWritable(clazz);
    arrayWritable.readFields(inputStream);

    Writable[] writables = arrayWritable.get();

    fileObjects = new ArrayList<FileObject>();
    long end = start + length;
    for (long i = start; i < end; i++) {
      fileObjects.add((FileObject) writables[(int) i]);
    }
    inputStream.close();
  }

  @Override
  public boolean nextKeyValue() throws IOException, InterruptedException {
    // TODO Auto-generated method stub
    if (pos < start + length) {
      currentArrayWritable = new FileObjectArrayWritable(fileObjects, clazz);
      pos = start + length;
      return true;
    } else {
      return false;
    }
  }

  @Override
  public LongWritable getCurrentKey() throws IOException, InterruptedException {
    // TODO Auto-generated method stub
    return new LongWritable(pos);
  }

  @Override
  public FileObjectArrayWritable getCurrentValue() throws IOException,
      InterruptedException {
    return currentArrayWritable;
  }

  @Override
  public float getProgress() throws IOException, InterruptedException {
    // TODO Auto-generated method stub
    long done = pos - start - 1;
    return (float) done / length;
  }

  @Override
  public void close() throws IOException {
    // TODO Auto-generated method stub

  }

}
