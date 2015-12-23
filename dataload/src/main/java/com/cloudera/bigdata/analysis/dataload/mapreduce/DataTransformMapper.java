package com.cloudera.bigdata.analysis.dataload.mapreduce;

import java.io.IOException;
import java.util.ArrayList;
import java.util.List;

import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.filecache.DistributedCache;
import org.apache.hadoop.fs.Path;
import org.apache.hadoop.io.LongWritable;
import org.apache.hadoop.io.Writable;
import org.apache.hadoop.mapreduce.Mapper;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import com.cloudera.bigdata.analysis.dataload.Constants;
import com.cloudera.bigdata.analysis.dataload.LoadTask;
import com.cloudera.bigdata.analysis.dataload.io.FileObject;
import com.cloudera.bigdata.analysis.dataload.io.FileObjectArrayWritable;

public class DataTransformMapper<KEYOUT, VALUEOUT> extends
    Mapper<LongWritable, FileObjectArrayWritable, KEYOUT, VALUEOUT> {
  private static final Logger LOG = LoggerFactory
      .getLogger(DataTransformMapper.class);

  private int workerThdNumber;

  protected void setup(Context context) throws IOException,
      InterruptedException {
    Configuration conf = context.getConfiguration();
    workerThdNumber = conf.getInt(Constants.THREADS_PER_MAPPER_KEY,
        Constants.DEFAULT_THREADS_PER_MAPPER);

    Path[] files = DistributedCache.getLocalCacheFiles(conf);
    for (Path path : files) {
      if (path.getName().endsWith(conf.get(Constants.INSTANCE_DOC_NAME_KEY))) {
        conf.set(Constants.INSTANCE_DOC_PATH_KEY, path.toUri().toString());
        break;
      }
    }
  }

  protected void map(LongWritable key, FileObjectArrayWritable value,
      Context context) throws IOException, InterruptedException {
    Writable[] writables = value.get();
    List<FileObject> fileList = new ArrayList<FileObject>();
    for (Writable writable : writables) {
      if (writable instanceof FileObject) {
        fileList.add((FileObject) writable);
      }
    }
    LoadTask task = new LoadTask(fileList, context.getConfiguration(), 1,
        workerThdNumber);
    task.submitJob();
  }
}
