package com.cloudera.bigdata.analysis.dataload;

import java.io.IOException;
import java.util.Map;
import java.util.concurrent.ConcurrentHashMap;
import java.util.concurrent.ConcurrentMap;
import java.util.concurrent.atomic.AtomicLong;

import org.apache.hadoop.hbase.client.Row;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import com.cloudera.bigdata.analysis.dataload.exception.TableDisabledException;
import com.cloudera.bigdata.analysis.dataload.source.HTableDefinition;
import com.cloudera.bigdata.analysis.dataload.source.Record;

/**
 * UpdateWorker will be responsible to fetch record from parsed queue and invoke
 * HBaseUpdater to insert record to HBase.
 */
public class UpdateWorker implements Runnable {
  private static final Logger LOG = LoggerFactory.getLogger(UpdateWorker.class);
  private static final String WORKER_NAME_PREFIX = "UpdateWorker-";
  private static ConcurrentMap<HTableDefinition, Boolean> openedTbls = new ConcurrentHashMap<HTableDefinition, Boolean>();
  private static AtomicLong processedCount = new AtomicLong(0);
  private static final int MAX_RETRY = 3;

  private boolean enableCounter = true;

  private String name = null;

  private int id;
  /**
   * Each thread will have its own HBaseUpdate object
   */
  private HBaseUpdater updateClient;

  private RuntimeContext context;

  private boolean hasRetryRecord = false;

  private int retryTimes = 0;

  public UpdateWorker(int id, RuntimeContext context) {
    this.name = WORKER_NAME_PREFIX + id;
    this.id = id;
    this.context = context;
    this.updateClient = new HBaseUpdater(context.getConf());
  }

  public String getName() {
    return name;
  }

  public String getThreadId() {
    return "" + id;
  }

  @Override
  public void run() {
    FileInfo file = null;
    while (!context.hasFinished()) {
      RecordWrapper record;
      RecordWrapper previousRecord = null;
      try {
        if (!hasRetryRecord) {
          record = context.getQueue().take();
          previousRecord = record;
          retryTimes = 0;
        } else {
          hasRetryRecord = false;
          if (retryTimes <= MAX_RETRY) {
            record = previousRecord;
          } else {
            retryTimes = 0;
            continue;
          }
        }

        if (record != null) {
          file = record.getFile();
          if (file == RuntimeContext.DUMMY_FILE) {
            break;
          }
          if (file != null && !file.hasError() && record.getRecord() != null) {
            if (LOG.isDebugEnabled()) {
              LOG.debug("before convertToHBaseRecord");
            }
            Record r = record.getRecord();
            r.preConvert(this.id);
            Map<HTableDefinition, Row[]> ops = null;
            try {
              ops = r.convertToHBaseRecord();
              if (LOG.isDebugEnabled()) {
                LOG.debug("after convertToHBaseRecord");
              }
            } catch (IOException e) {
              file.setError(e);
              e.getStackTrace();
            }

            if (ops == null) {
              throw new Exception(
                  "Failed to get Map<HTableDefinition, Row[]>, ops is null");
            } else {
              for (HTableDefinition tbl : ops.keySet()) {
                if (!openedTbls.containsKey(tbl)) {
                  openedTbls.put(tbl, true);
                  if (LOG.isDebugEnabled()) {
                    LOG.debug("add table to openTbls");
                  }
                }
              }
              if (LOG.isDebugEnabled()) {
                LOG.debug("before HBaseUpdater update");
              }
              updateClient.update(r, ops);
              if (LOG.isDebugEnabled()) {
                LOG.debug("after HBaseUpdater update");
              }
            }

            if (enableCounter) {
              long count = processedCount.incrementAndGet();
              if (count % 1000000 == 0) {
                LOG.info("===== Processed " + count + " rows.");
              }
            }

            file.handleRecord();
          }
        }
      } catch (TableDisabledException e) {

        if (file != null) {
          file.setError(e);
        }
      } catch (InterruptedException e) {
        e.getStackTrace();
      } catch (IOException e) {
        hasRetryRecord = true;
        retryTimes++;
      } catch (Exception e) {
        hasRetryRecord = true;
        retryTimes++;
      }
    }

    try {
      updateClient.flush();
      LOG.info(Thread.currentThread().getName() + " is closing");
      updateClient.close();
    } catch (IOException e) {
      LOG.error("updateClient flush Error", e);
    }

    context.releaseLatch(false);
  }
}