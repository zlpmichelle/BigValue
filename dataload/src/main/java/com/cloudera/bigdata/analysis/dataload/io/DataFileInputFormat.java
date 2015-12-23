package com.cloudera.bigdata.analysis.dataload.io;

import java.io.IOException;
import java.util.ArrayList;
import java.util.List;

import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.fs.FSDataInputStream;
import org.apache.hadoop.fs.FileSystem;
import org.apache.hadoop.fs.Path;
import org.apache.hadoop.io.LongWritable;
import org.apache.hadoop.mapreduce.InputSplit;
import org.apache.hadoop.mapreduce.JobContext;
import org.apache.hadoop.mapreduce.RecordReader;
import org.apache.hadoop.mapreduce.TaskAttemptContext;
import org.apache.hadoop.mapreduce.lib.input.FileInputFormat;
import org.apache.hadoop.mapreduce.lib.input.FileSplit;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import com.cloudera.bigdata.analysis.dataload.Constants;

public class DataFileInputFormat extends
    FileInputFormat<LongWritable, FileObjectArrayWritable> {

  private static final Logger LOG = LoggerFactory
      .getLogger(DataFileInputFormat.class);

  @Override
  public RecordReader<LongWritable, FileObjectArrayWritable> createRecordReader(
      InputSplit split, TaskAttemptContext context) throws IOException,
      InterruptedException {
    return new InputReader();
  }

  @SuppressWarnings("unchecked")
  public List<InputSplit> getSplits(JobContext job) throws IOException {
    List<InputSplit> splits = new ArrayList<InputSplit>();

    // assume there is only one input file
    Path[] paths = getInputPaths(job);
    Path path = paths[0];

    Configuration conf = job.getConfiguration();
    String className = conf.get(Constants.FILEOBJECT_CLASS_KEY);
    Class<? extends FileObject> fileObjectClass = null;
    try {
      fileObjectClass = (Class<? extends FileObject>) Class.forName(className);
    } catch (ClassNotFoundException e) {
      e.printStackTrace();
    }

    FileSystem fs = FileSystem.get(conf);
    FileObjectArrayWritable arrayWritable = new FileObjectArrayWritable(
        fileObjectClass);
    FSDataInputStream inputStream = fs.open(path);
    arrayWritable.readFields(inputStream);
    inputStream.close();

    // fetch parallel is the hint
    int mapperNumHint = conf.getInt(Constants.FETCH_PARALLEL_KEY, 1);
    int inputFileNum = arrayWritable.get().length;
    int mapperNum = mapperNumHint;
    if (mapperNumHint > inputFileNum) {
      mapperNum = inputFileNum;
    }

    int actualFilesPerMapper = arrayWritable.get().length / mapperNum;
    int remainder = arrayWritable.get().length % mapperNum;

    for (int i = 0; i < mapperNum; i++) {
      if (i < remainder) {
        splits.add(new FileSplit(path, i * (actualFilesPerMapper + 1),
            actualFilesPerMapper + 1, new String[] {}));
      } else {
        splits.add(new FileSplit(path, i * actualFilesPerMapper + remainder,
            actualFilesPerMapper, new String[] {}));
      }
    }
    return splits;
  }
}
