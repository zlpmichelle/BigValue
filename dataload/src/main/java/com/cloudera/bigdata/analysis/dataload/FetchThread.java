package com.cloudera.bigdata.analysis.dataload;

import java.io.IOException;
import java.io.InputStream;

import org.apache.hadoop.conf.Configuration;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import com.cloudera.bigdata.analysis.dataload.exception.ParseException;
import com.cloudera.bigdata.analysis.dataload.io.FileObject;
import com.cloudera.bigdata.analysis.dataload.source.DataSource;
import com.cloudera.bigdata.analysis.dataload.source.FileParser;
import com.cloudera.bigdata.analysis.dataload.source.Record;
import com.cloudera.bigdata.analysis.dataload.util.Util;

/**
 * FetchThread is the entry to fetch data from various sources. Firstly, it will
 * iterate the file list assigned to this thread, and then read records from the
 * file.
 */
public class FetchThread implements Runnable {
  private static final Logger LOG = LoggerFactory.getLogger(FetchThread.class);
  private static final String FETCH_NAME_PREFIX = "FetchThread-";

  private int threadId;
  private RuntimeContext context;
  private String name;
  private Configuration conf;
  private RecordConsumer consumer = null;

  public FetchThread(int id, RuntimeContext context) {
    this.name = FETCH_NAME_PREFIX + id;
    this.threadId = id;
    this.context = context;
    this.conf = context.getConf();
    
    if(conf.getBoolean(Constants.STREAMING_FETCH_KEY, false)){
      consumer = new RecordConsumer();
    }
  }

  public String getName() {
    return name;
  }

  public int getThreadId() {
    return threadId;
  }

  public DataSource connectDataSource() {
    DataSource dataSource;
    try {
      dataSource = Util.newDataSource(conf);
    } catch (Exception e1) {
      context.setError(RuntimeContext.ErrorCode.ERROR_NEW_DATA_SOURCE, e1);

      return null;
    }

    try {
      dataSource.connect();
    } catch (IOException e1) {
      context.setError(RuntimeContext.ErrorCode.ERROR_OPEN_DATA_SOURCE, e1);

      return null;
    }

    return dataSource;
  }

  @Override
  public void run() {
    DataSource dataSource = connectDataSource();
    FileParser parser;
    try {
      parser = Util.getFileParser(conf);
    } catch (ParseException e) {
      context.setError(RuntimeContext.ErrorCode.ERROR_CREATE_FILE_PARSER, e);
      context.releaseLatch(true);
      return;
    }

    while (true) {
      LOG.info("%%%%% " + Thread.currentThread().getName() + " is trying to get file.");
      FileInfo file = null;
      if(consumer!=null){
        file = consumer.getNextFile();
      }else{
        file = context.getNextFileToFetch();
      }
      
      if (file == null) {
        LOG.info(getName() + " has no more file to fetch from. Exiting...");
        break;
      }else{
        LOG.info(Thread.currentThread().getName() + " Processing: " + file.toString());
      }
      FileObject fo = file.getFileObject();
      InputStream is = null;
      Exception exception = null;
      DataSource.FileHandleStatus status = DataSource.FileHandleStatus.HANDLED_SUCCESSFUL;
      try {
        is = dataSource.readFile(fo);
        parser.init(is, fo, context.getConf());
        Record record = null;
        while ((record = parser.getNext()) != null) {
          RecordWrapper recordWrapper = new RecordWrapper(record, file);
          context.getQueue().put(recordWrapper);
          file.addRecord();
        } // end of record iteration in a single file
        file.setEOF(true);
      } catch (Exception e) {
        LOG.error("", e);
        exception = e;
        String errorMsg = null;
        status = null;
        if (e instanceof IOException) {
          errorMsg = getName() + " IOException happened when parse record: "
              + e.getMessage();
          LOG.error(errorMsg);
          status = DataSource.FileHandleStatus.IO_ERROR;
        } else if (e instanceof InterruptedException) {
          errorMsg = getName()
              + " InterruptedException happened when parse record: "
              + e.getMessage();
          LOG.error(errorMsg);
          status = DataSource.FileHandleStatus.INTERRUPTED;
        }
        file.setError(e); // should not skip the whole file
      } finally {
        try {
          dataSource.closeFile(fo, is, status, exception);
        } catch (IOException e) {
          LOG.error("Error occurred while closing file", e);
          context.setError(RuntimeContext.ErrorCode.ERROR_CLOSE_FILE, e);
          break;
        }
      }
    } // end of file iteration

    try {
      dataSource.close();
    } catch (IOException e) {
      context.setError(RuntimeContext.ErrorCode.ERROR_CLOSE_DATA_SOURCE, e);
    }
    
    context.releaseLatch(true);
  }
}
