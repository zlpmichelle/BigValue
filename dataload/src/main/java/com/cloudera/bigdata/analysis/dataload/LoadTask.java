package com.cloudera.bigdata.analysis.dataload;

import java.io.FileInputStream;
import java.io.IOException;
import java.util.List;
import java.util.Properties;
import java.util.concurrent.ExecutorService;
import java.util.concurrent.Executors;
import java.util.concurrent.Future;

import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.hbase.HBaseConfiguration;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import com.cloudera.bigdata.analysis.dataload.io.FileObject;
import com.cloudera.bigdata.analysis.dataload.util.Util;

/**
 * The LoadTask is the main entry for loading data to HBase. It can be used in
 * standalone mode or in mapreduce mode. In standalone mode, it's invoked by a
 * single client; while in mapred it's invoked in Mapper's map function.
 */
public class LoadTask {
  private static final Logger LOG = LoggerFactory.getLogger(LoadTask.class);
  private static final String USAGE_STR = "com.cloudera.bigdata.analysis.dataload.LoadTask <properties_file>";

  private List<FileObject> files;
  private Configuration conf;
  private UpdateWorker[] updateWorkers;
  private FetchThread[] fetchThreads;
  private int fetchParallel;
  private int numWorkers;
  private ExecutorService execService;
  private RuntimeContext context;

  public LoadTask() {

  }

  public LoadTask(Configuration conf, int fetchParallel, int updateThreadNum)
      throws IOException {
    this(Util.getFileList(conf), conf, fetchParallel, updateThreadNum);
  }

  public LoadTask(List<FileObject> fileList, Configuration conf,
      int fetchThreadNum, int updateThreadNum) throws IOException {
    this.files = fileList;
    this.conf = conf;
    fetchParallel = fetchThreadNum;
    numWorkers = updateThreadNum;
    if (LOG.isDebugEnabled()) {
      LOG.debug("files size " + files.size());
      LOG.debug("fetchParallel " + fetchParallel);
      LOG.debug("numWorkers " + numWorkers);
    }
    context = new RuntimeContext(files, this.conf, fetchParallel, numWorkers);
  }

  public void submitJob() throws IOException {
    fetchThreads = new FetchThread[fetchParallel];
    updateWorkers = new UpdateWorker[numWorkers];

    Future<?>[] futures = new Future<?>[numWorkers + fetchParallel];
    execService = Executors.newFixedThreadPool(numWorkers + fetchParallel);
    for (int i = 0; i < updateWorkers.length; i++) {
      futures[i] = execService.submit(new UpdateWorker(i, context));
    }
    for (int i = 0; i < fetchThreads.length; i++) {
      futures[i + numWorkers] = execService.submit(new FetchThread(i, context));
    }

    try {
      context.waitUnLatch();
    } catch (InterruptedException e) {
      LOG.error("", e);
    }

    execService.shutdown();
  }

  public static void main(String args[]) {
    try {
      if (args.length < 1) {
        System.out.println(USAGE_STR);
      }

      Properties props = new Properties();
      props.load(new FileInputStream(args[0]));
      Configuration conf = HBaseConfiguration.create();
      Util.mergeProperties(props, conf);

      int fetchParallel = conf.getInt(
      /* Constants.PROP_PREFIX + Constants.FETCH_PARALLEL_KEY, */
      Constants.FETCH_PARALLEL_KEY, Constants.DEFAULT_FETCH_PARALLEL);
      int workerThreadsPerFetch = conf.getInt(
      /* Constants.PROP_PREFIX + Constants.THREADS_PER_MAPPER_KEY, */
      Constants.THREADS_PER_MAPPER_KEY, Constants.DEFAULT_THREADS_PER_MAPPER);
      int numWorkers = fetchParallel * workerThreadsPerFetch;

      LoadTask task = new LoadTask(conf, fetchParallel, numWorkers);

      task.submitJob();
    } catch (IOException e) {

    }
  }
}
