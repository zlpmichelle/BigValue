package com.cloudera.bigdata.analysis.dataload;

import java.io.IOException;
import java.util.ArrayList;
import java.util.Arrays;
import java.util.List;
import java.util.Map;
import java.util.concurrent.atomic.AtomicInteger;
import java.util.concurrent.locks.Lock;
import java.util.concurrent.locks.ReentrantLock;

import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.hbase.client.HTableInterface;
import org.apache.hadoop.hbase.client.HTableInterfaceFactory;
import org.apache.hadoop.hbase.client.HTablePool;
import org.apache.hadoop.hbase.client.Put;
import org.apache.hadoop.hbase.client.Row;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import com.cloudera.bigdata.analysis.dataload.exception.TableDisabledException;
import com.cloudera.bigdata.analysis.dataload.source.HTableDefinition;
import com.cloudera.bigdata.analysis.dataload.source.Record;

/**
 * HBaseUpdater is responsible for the real data injection. HBaseUpdater is not
 * thread-safe, so make sure each thread will has its own HBaseUpdater instance.
 * The HTablePool is thread-safe, as it's backed by ConcurrentHashMap.
 */
public class HBaseUpdater {
  private static final Logger LOG = LoggerFactory.getLogger(HBaseUpdater.class);

  private static Lock lock = new ReentrantLock();
  private static HTablePool pool = null;
  private static TableManagementThread managementThread = null;
  private static AtomicInteger counter = new AtomicInteger(0);

  private boolean createTableIfNotExist;
  private HTableInterface cachedTable = null;
  private HTableDefinition cachedTableDefinition = null;

  public HBaseUpdater(Configuration conf) {
    initialize(conf);
    counter.incrementAndGet();
    createTableIfNotExist = conf.getBoolean(Constants.CREATE_TABLE_KEY,
        Constants.DEFAULT_CREATE_TABLE);
  }

  private void initialize(Configuration conf) {
    lock.lock();
    if (pool == null) {
      int writeBufferSize = conf.getInt(Constants.WRITE_BUFFER_SIZE_KEY,
          Constants.DEFAULT_WRITE_BUFFER_SIZE);
      boolean autoFlush = conf.getBoolean(Constants.AUTO_FLUSH_KEY,
          Constants.DEFAULT_AUTO_FLUSH);
      HTableInterfaceFactory factory = new HTableFactoryImpl(writeBufferSize,
          autoFlush);
      pool = new HTablePool(conf, Integer.MAX_VALUE, factory);
      managementThread = new TableManagementThread(conf);
      managementThread.start();
    }
    lock.unlock();
  }


  public void update(Record record, Map<HTableDefinition, Row[]> updates)
      throws Exception {
    if (LOG.isDebugEnabled()) {
      LOG.debug("enter update");
    }
    for (HTableDefinition tableDefinition : updates.keySet()) {
      if (!tableDefinition.equals(cachedTableDefinition)) {
        if (cachedTable != null) {
          cachedTable.close();
        }

        if (createTableIfNotExist) {
          managementThread.createTable(tableDefinition);
          if (LOG.isDebugEnabled()) {
            LOG.debug("after create");
          }
        }
        cachedTable = pool.getTable(tableDefinition.getTableName());
        cachedTableDefinition = tableDefinition;
      }

      Row[] updatesPerTable = updates.get(tableDefinition);
      List<Put> puts = new ArrayList<Put>();
      boolean allPuts = true;
      for (Row r : updatesPerTable) {
        if (r instanceof Put) {
          puts.add((Put) r);
        } else {
          allPuts = false;
          continue;
        }
      }
      if (allPuts) {
        cachedTable.put(puts);
      } else {
        cachedTable.batch(Arrays.asList(updatesPerTable));
      }
    }
    record.finishUpdate();
  }

  /**
   * Explicitly flush the table updates
   */
  public void flush() throws IOException {
    if (cachedTable != null) {
      cachedTable.flushCommits();
    }
  }

  public void close() {
    if (counter.decrementAndGet() == 0) {
      managementThread.close();
      try {
        LOG.info(Thread.currentThread().getName() + " close the pool");
        pool.close();
      } catch (IOException e) {
        LOG.error("Fail to close HTable Pool", e);
      }
    }
  }
}
