package com.cloudera.bigdata.analysis.dataload.io;

import java.io.IOException;

import org.apache.hadoop.io.Text;
import org.apache.hadoop.mapreduce.InputSplit;
import org.apache.hadoop.mapreduce.RecordReader;
import org.apache.hadoop.mapreduce.TaskAttemptContext;
import org.apache.hadoop.mapreduce.lib.input.CombineFileInputFormat;
import org.apache.hadoop.mapreduce.lib.input.CombineFileRecordReader;
import org.apache.hadoop.mapreduce.lib.input.CombineFileSplit;

public class CombineTextInputFormat extends
    CombineFileInputFormat<FileRowWritable, Text> {
  @Override
  public RecordReader<FileRowWritable, Text> createRecordReader(
      InputSplit split, TaskAttemptContext context) throws IOException {
    return new CombineFileRecordReader<FileRowWritable, Text>(
        (CombineFileSplit) split, context, FileRowRecordReader.class);
  }
}
