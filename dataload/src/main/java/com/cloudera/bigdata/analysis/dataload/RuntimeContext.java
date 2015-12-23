package com.cloudera.bigdata.analysis.dataload;

import java.util.ArrayList;
import java.util.List;
import java.util.NoSuchElementException;
import java.util.SortedSet;
import java.util.TreeSet;
import java.util.concurrent.BlockingQueue;
import java.util.concurrent.CountDownLatch;
import java.util.concurrent.LinkedBlockingQueue;
import java.util.concurrent.atomic.AtomicBoolean;
import java.util.concurrent.atomic.AtomicInteger;

import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.conf.Configured;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import com.cloudera.bigdata.analysis.dataload.io.FileObject;

public class RuntimeContext extends Configured {
  private static final Logger LOG = LoggerFactory
      .getLogger(RuntimeContext.class);
  public static final FileInfo DUMMY_FILE = new FileInfo(null);

  public static enum ErrorCode {
    ERROR_NEW_DATA_SOURCE, 
    ERROR_OPEN_DATA_SOURCE, 
    ERROR_CREATE_FILE_PARSER, 
    ERROR_CLOSE_FILE, 
    ERROR_CLOSE_DATA_SOURCE
  }
  private BlockingQueue<RecordWrapper> queue;
  private SortedSet<FileObject> files;
  private List<FileInfo> workingFiles;
  private AtomicBoolean atomicFlag;
  private AtomicInteger atomicFetchNum;
  private int updateWorkerThdNum;
  private CountDownLatch countDownLatch;

  public RuntimeContext(List<FileObject> files, 
      Configuration conf, 
      int fetchThdNum, 
      int updateWorkerNum) {
    super(conf);
    int lenQueue = conf.getInt(Constants.QUEUE_LENGTH_KEY,
        Constants.DEFAULT_QUEUE_LENGTH);
    this.queue = new LinkedBlockingQueue<RecordWrapper>(lenQueue);
    this.files = new TreeSet<FileObject>(files);
    this.workingFiles = new ArrayList<FileInfo>();
    this.updateWorkerThdNum = updateWorkerNum;
    countDownLatch = new CountDownLatch(fetchThdNum + updateWorkerThdNum);
    this.atomicFetchNum = new AtomicInteger(fetchThdNum);
    this.atomicFlag = new AtomicBoolean(false);
  }
  
  public void releaseLatch(boolean isFetch){
    if(isFetch){
      atomicFetchNum.decrementAndGet();
      if(atomicFetchNum.compareAndSet(0, 0) && atomicFlag.compareAndSet(false, true)){
        for(int i=0;i<updateWorkerThdNum;i++){
          RecordWrapper dummy = new RecordWrapper(null, DUMMY_FILE);
          queue.offer(dummy);
        }
      }
    }
    LOG.info(Thread.currentThread().getName() + " is releasing latch");
    countDownLatch.countDown();
  }
  
  public void waitUnLatch() throws InterruptedException{
    countDownLatch.await();
  }

  public void setError(ErrorCode c, Exception e) {

  }

  public boolean hasFinished() {
    if(atomicFlag.get() && queue.isEmpty()){
      return true;
    }
    return false;
  }

  public BlockingQueue<RecordWrapper> getQueue() {
    return queue;
  }

  /**
   * Get next file from the file list and put it to the working queue.
   */
  public synchronized FileInfo getNextFileToFetch() {
    try {
      FileObject file = files.first();
      files.remove(file);
      FileInfo fi = new FileInfo(file);
      workingFiles.add(fi);
      return fi;
    } catch (NoSuchElementException e) {
      return null;
    }
  }
}
