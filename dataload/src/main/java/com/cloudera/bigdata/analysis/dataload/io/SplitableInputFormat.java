/**
 * Licensed to the Apache Software Foundation (ASF) under one
 * or more contributor license agreements.  See the NOTICE file
 * distributed with this work for additional information
 * regarding copyright ownership.  The ASF licenses this file
 * to you under the Apache License, Version 2.0 (the
 * "License"); you may not use this file except in compliance
 * with the License.  You may obtain a copy of the License at
 *
 *     http://www.apache.org/licenses/LICENSE-2.0
 *
 * Unless required by applicable law or agreed to in writing, software
 * distributed under the License is distributed on an "AS IS" BASIS,
 * WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
 * See the License for the specific language governing permissions and
 * limitations under the License.
 */

package com.cloudera.bigdata.analysis.dataload.io;

import org.apache.commons.logging.Log;
import org.apache.commons.logging.LogFactory;
import org.apache.hadoop.fs.Path;
import org.apache.hadoop.io.LongWritable;
import org.apache.hadoop.io.Text;
import org.apache.hadoop.io.compress.CompressionCodec;
import org.apache.hadoop.io.compress.CompressionCodecFactory;
import org.apache.hadoop.io.compress.SplittableCompressionCodec;
import org.apache.hadoop.mapreduce.InputFormat;
import org.apache.hadoop.mapreduce.InputSplit;
import org.apache.hadoop.mapreduce.Job;
import org.apache.hadoop.mapreduce.JobContext;
import org.apache.hadoop.mapreduce.RecordReader;
import org.apache.hadoop.mapreduce.TaskAttemptContext;
import org.apache.hadoop.mapreduce.lib.input.FileInputFormat;
import org.apache.hadoop.mapreduce.lib.input.LineRecordReader;

import com.cloudera.bigdata.analysis.dataload.mapreduce.SplitableRecordReader;

/**
 * An {@link InputFormat} for plain text files. Files are broken into lines.
 * Either linefeed or carriage-return are used to signal end of line. Keys are
 * the position in the file, and values are the line of text..
 */
public class SplitableInputFormat extends FileInputFormat<LongWritable, Text> {
  private static final Log LOG = LogFactory.getLog(SplitableInputFormat.class);
  private static String DELIMITER = "mapreduce.input.splitableInputFormat.delimiter";
  private static String RECORD_READER = "mapreduce.input.splitableInputFormat.recordReader";
  private static String ENCODING = "mapreduce.input.splitableInputFormat.encoding";

  public static void setReaderClass(Job job, String className) {
    job.getConfiguration().set(RECORD_READER, className);
  }

  public static void setEncoding(Job job, String encoding) {
    job.getConfiguration().set(ENCODING, encoding);
  }

  public static void setDelimiter(Job job, String delimiter) {
    job.getConfiguration().set(DELIMITER, delimiter);
  }

  @Override
  public RecordReader<LongWritable, Text> createRecordReader(InputSplit split,
      TaskAttemptContext context) {
    String delimiter = context.getConfiguration().get(DELIMITER);
    String encoding = context.getConfiguration().get(ENCODING);
    String className = context.getConfiguration().get(RECORD_READER,
        "org.apache.hadoop.mapreduce.lib.input.LineRecordReader");

    byte[] recordDelimiterBytes = null;
    if (null != delimiter)
      recordDelimiterBytes = delimiter.getBytes();
    // Here is the reader to do the real split.

    SplitableRecordReader recorder;
    try {
      Class<? extends SplitableRecordReader> recordClass = (Class<? extends SplitableRecordReader>) Class
          .forName(className);
      recorder = recordClass.newInstance();
    } catch (Exception e) {
      LOG.error("Unable to create instance of class " + className.toString(), e);
      return new LineRecordReader(recordDelimiterBytes);
    }
    if (encoding != null) {
      recorder.setEncoding(encoding);
    }
    if (delimiter != null) {
      recorder.setRecordDelimiterBytes(recordDelimiterBytes);
    }
    return recorder;
  }

  @Override
  protected boolean isSplitable(JobContext context, Path file) {
    CompressionCodec codec = new CompressionCodecFactory(
        context.getConfiguration()).getCodec(file);
    if (null == codec) {
      return true;
    }
    return codec instanceof SplittableCompressionCodec;
  }

}
