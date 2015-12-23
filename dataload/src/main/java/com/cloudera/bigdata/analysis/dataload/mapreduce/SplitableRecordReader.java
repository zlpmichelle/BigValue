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

package com.cloudera.bigdata.analysis.dataload.mapreduce;

import java.io.IOException;

import org.apache.commons.logging.Log;
import org.apache.commons.logging.LogFactory;
import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.fs.FSDataInputStream;
import org.apache.hadoop.fs.FileSystem;
import org.apache.hadoop.fs.Path;
import org.apache.hadoop.fs.Seekable;
import org.apache.hadoop.io.LongWritable;
import org.apache.hadoop.io.Text;
import org.apache.hadoop.io.compress.CodecPool;
import org.apache.hadoop.io.compress.CompressionCodec;
import org.apache.hadoop.io.compress.CompressionCodecFactory;
import org.apache.hadoop.io.compress.Decompressor;
import org.apache.hadoop.io.compress.SplitCompressionInputStream;
import org.apache.hadoop.io.compress.SplittableCompressionCodec;
import org.apache.hadoop.mapreduce.InputSplit;
import org.apache.hadoop.mapreduce.RecordReader;
import org.apache.hadoop.mapreduce.TaskAttemptContext;
import org.apache.hadoop.mapreduce.lib.input.FileSplit;
import org.apache.hadoop.util.LineReader;

/**
 * Treats keys as offset in file and value as line.
 */
public abstract class SplitableRecordReader extends
    RecordReader<LongWritable, Text> {
  private static final Log LOG = LogFactory.getLog(SplitableRecordReader.class);
  public static final int PHONE_NUM_LENGTH = 24;
  public static final int START_TIME_LENGTH = 14;

  private CompressionCodecFactory compressionCodecs = null;
  private long start;
  private long pos;
  private long end;
  private LineReader in;
  private int maxLineLength;
  private LongWritable key = null;
  private Text value = null;
  private Seekable filePosition;
  private CompressionCodec codec;
  private Decompressor decompressor;
  private String encoding = "UTF-8";
  private boolean isContinue = true;
  private Text previous = new Text();

  private byte[] recordDelimiterBytes = null;

  public SplitableRecordReader() {

  }

  public SplitableRecordReader(byte[] recordDelimiter) {
    this.recordDelimiterBytes = recordDelimiter;
  }

  public byte[] getRecordDelimiterBytes() {
    return recordDelimiterBytes;
  }

  public void setRecordDelimiterBytes(byte[] recordDelimiterBytes) {
    this.recordDelimiterBytes = recordDelimiterBytes;
  }

  public void setEncoding(String encoding) {
    this.encoding = encoding;
  }

  public String getEncoding() {
    return encoding;
  }

  /**
   * Decide the start of the reader.
   */
  public void initialize(InputSplit genericSplit, TaskAttemptContext context)
      throws IOException {
    FileSplit split = (FileSplit) genericSplit;
    Configuration job = context.getConfiguration();
    this.maxLineLength = job.getInt("mapred.linerecordreader.maxlength",
        Integer.MAX_VALUE);
    start = split.getStart();
    end = start + split.getLength();
    final Path file = split.getPath();
    compressionCodecs = new CompressionCodecFactory(job);
    codec = compressionCodecs.getCodec(file);

    // if (codec instanceof CryptoCodec && job instanceof JobConf)
    // CryptoContextHelper.resetInputCryptoContext((CryptoCodec) codec,
    // (JobConf) job, file);

    // open the file and seek to the start of the split
    FileSystem fs = file.getFileSystem(job);
    FSDataInputStream fileIn = fs.open(split.getPath());

    if (isCompressedInput()) {
      decompressor = CodecPool.getDecompressor(codec);
      if (codec instanceof SplittableCompressionCodec) {
        final SplitCompressionInputStream cIn = ((SplittableCompressionCodec) codec)
            .createInputStream(fileIn, decompressor, start, end,
                SplittableCompressionCodec.READ_MODE.BYBLOCK);
        if (null == this.recordDelimiterBytes) {
          in = new LineReader(cIn, job);
        } else {
          in = new LineReader(cIn, job, this.recordDelimiterBytes);
        }
        start = cIn.getAdjustedStart();
        end = cIn.getAdjustedEnd();
        filePosition = cIn;
      } else {
        if (null == this.recordDelimiterBytes) {
          in = new LineReader(codec.createInputStream(fileIn), job);
        } else {
          in = new LineReader(codec.createInputStream(fileIn), job,
              this.recordDelimiterBytes);
        }
        filePosition = fileIn;
      }
    } else {
      fileIn.seek(start);
      if (null == this.recordDelimiterBytes) {
        in = new LineReader(fileIn, job);
      } else {
        in = new LineReader(fileIn, job, this.recordDelimiterBytes);
      }
      filePosition = fileIn;
    }
    LOG.info("Read from " + split.getPath().toString());
    // If this is not the first split, we always throw away first record
    // because we always (except the last split) read one extra line in
    // next() method.
    if (start != 0) {
      start += in.readLine(new Text(), 0, maxBytesToConsume(start));

      // Read another line as previous.

      Text current = new Text();

      int newSize = in.readLine(previous, maxLineLength,
          maxBytesToConsume(start));

      LOG.info("Skip line " + previous + " for last split.");

      start += newSize;

      // Keep reading until a splitable point is found.
      while (start <= end) {
        newSize = in.readLine(current, maxLineLength, maxBytesToConsume(start));
        if (canSplit(previous.getBytes(), current.getBytes())) {
          break;
        }
        start += newSize;
        previous.set(current.getBytes());
        LOG.info("Skip line " + previous + " for last split.");
      }

      // If exceed the end, still read one extra line.
      if (start > end) {
        if (isContinue) {
          newSize = in.readLine(current, maxLineLength,
              maxBytesToConsume(start));
          if (!canSplit(previous.getBytes(), current.getBytes())) {
            // Still not splitable. So skip the block.
            start += newSize;
            isContinue = false;
          }
        }
      }
      LOG.info("Split between: \n" + previous + "\n" + current);

      // Restart at the last read line.
      fileIn.seek(start);
      if (null == this.recordDelimiterBytes) {
        in = new LineReader(fileIn, job);
      } else {
        in = new LineReader(fileIn, job, this.recordDelimiterBytes);
      }

      this.pos = start;
    } else {
      Text skip = new Text();
      start += in.readLine(skip, maxLineLength, maxBytesToConsume(start));
      // start += in.readLine(skip, 0, maxBytesToConsume(start));
      LOG.info("Skip line " + skip + ". Start at " + start);
    }

    // Restart at the start index.
  }

  private boolean isCompressedInput() {
    return (codec != null);
  }

  private int maxBytesToConsume(long pos) {
    return isCompressedInput() ? Integer.MAX_VALUE : (int) Math.min(
        Integer.MAX_VALUE, end - pos);
  }

  private long getFilePosition() throws IOException {
    long retVal;
    if (isCompressedInput() && null != filePosition) {
      retVal = filePosition.getPos();
    } else {
      retVal = pos;
    }
    return retVal;
  }

  /**
   * Keep reading records until splitable point is found.
   */
  public boolean nextKeyValue() throws IOException {
    if (key == null) {
      key = new LongWritable();
    }
    key.set(pos);
    if (value == null) {
      value = new Text();
    }
    int newSize = 0;
    // We always read one extra line, which lies outside the upper
    // split limit i.e. (end - 1)
    while (getFilePosition() <= end) {
      newSize = in.readLine(value, maxLineLength,
          Math.max(maxBytesToConsume(pos), maxLineLength));
      if (newSize == 0) {
        break;
      }
      pos += newSize;
      if (newSize < maxLineLength) {
        break;
      }

      // line too long. try again
      LOG.info("Skipped line of size " + newSize + " at pos " + (pos - newSize));
    }

    if (newSize == 0) {
      // Read one extra line.
      if (getFilePosition() > end) {
        newSize = in.readLine(value, maxLineLength,
            Math.max(maxBytesToConsume(pos), maxLineLength));

        // A invalid line. Stop reading.
        if (newSize == 0 || newSize >= maxLineLength) {
          newSize = 0;
          isContinue = false;
          key = null;
          value = null;
          return false;
        }

        // The line should be read.
        if (isContinue) {
          isContinue = false;
          pos += newSize;
          // Record the line.
          previous.set(value.getBytes());
          return true;
        } else {
          // Read until the line is splitable.
          if (canSplit(previous.getBytes(), value.getBytes())) {
            newSize = 0;
            key = null;
            value = null;
            return false;
          } else {
            pos += newSize;
            previous.set(value.getBytes());
            return true;
          }
        }
      } else {
        newSize = 0;
        isContinue = false;
        key = null;
        value = null;
        return false;
      }
    }
    return true;
  }

  @Override
  public LongWritable getCurrentKey() {
    return key;
  }

  @Override
  public Text getCurrentValue() {
    return value;
  }

  /**
   * Get the progress within the split
   */
  public float getProgress() throws IOException {
    if (start == end) {
      return 0.0f;
    } else {
      return Math
          .min(1.0f, (getFilePosition() - start) / (float) (end - start));
    }
  }

  public synchronized void close() throws IOException {
    try {
      if (in != null) {
        in.close();
      }
    } finally {
      if (decompressor != null) {
        CodecPool.returnDecompressor(decompressor);
      }
    }
  }

  /**
   * Check if two lines ate splitable.
   * 
   * @param previous
   *          - Previous read line.
   * @param current
   *          - Current read line.
   * @return True if the two records are splitable.
   */
  public abstract boolean canSplit(byte[] previous, byte[] current);
}
