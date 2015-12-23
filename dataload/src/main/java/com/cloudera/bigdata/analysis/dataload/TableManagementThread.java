package com.cloudera.bigdata.analysis.dataload;

import java.io.IOException;
import java.util.concurrent.ConcurrentHashMap;
import java.util.concurrent.atomic.AtomicBoolean;

import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.fs.Path;
import org.apache.hadoop.hbase.HColumnDescriptor;
import org.apache.hadoop.hbase.HTableDescriptor;
import org.apache.hadoop.hbase.client.HBaseAdmin;
import org.apache.hadoop.hbase.regionserver.ConstantSizeRegionSplitPolicy;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import com.cloudera.bigdata.analysis.dataload.source.HTableDefinition;
import com.cloudera.bigdata.analysis.index.IndexCoprocessor;
import com.cloudera.bigdata.analysis.query.RecordServiceEndpoint;
import com.google.common.collect.ConcurrentHashMultiset;

/**
 * TableManagementThread is a utility class to check/create HBase table
 * considering the multi-thread environment and distributed environment.
 */
public class TableManagementThread extends Thread {
  private static final Logger LOG = LoggerFactory
      .getLogger(TableManagementThread.class);

  private static final int SLEEP_TIME = 5000;
  private HBaseAdmin hBaseAdmin;

  private Configuration conf;

  private ConcurrentHashMultiset<String> tablesMultiset = ConcurrentHashMultiset
      .create();

  private ConcurrentHashMultiset<String> disabledTablesMultiset = ConcurrentHashMultiset
      .create();

  private AtomicBoolean exit = new AtomicBoolean(false);

  private ConcurrentHashMap<String, AtomicBoolean> availableMap = new ConcurrentHashMap<String, AtomicBoolean>();

  public TableManagementThread(Configuration configuration) {
    try {
      conf = configuration;
      hBaseAdmin = new HBaseAdmin(conf);
      updateTables();
      updateDisableTables();
    } catch (IOException e) {
      LOG.error("IOException happened while constructing HBaseAdmin", e);
      System.exit(1);
    }
  }

  public boolean isTableDisabled(String tableName) {
    boolean found = disabledTablesMultiset.contains(tableName);
    return found;
  }

  public void createTable(HTableDefinition tableDefinition) throws Exception {
    createTableInner(tableDefinition);
  }

  /**
   * We should always be aware that those methods will be invoked in multiple
   * distributed processes. Synchronization only guarantees the invocation
   * sequence in a single process.
   */
  protected synchronized void createTableInner(HTableDefinition tableDefinition)
      throws Exception {
    String tableName = tableDefinition.getTableName();
    if (LOG.isDebugEnabled()) {
      LOG.debug("tablename" + tableName);
    }
    if (isAvailable(tableName)) {
      return;
    }

    if (!isTableExist(tableName)) {
      // multiple process will enter this scope, but don't worry
      // HBase master will handle the creation request in synchronized manner
      HTableDescriptor ht = new HTableDescriptor(tableName);

      ht.setMemStoreFlushSize(tableDefinition.getMemStoreFlushSize());
      if (LOG.isDebugEnabled()) {
        LOG.debug("column family number: "
            + tableDefinition.getColumnFamilies().length);
      }

      for (HColumnDescriptor family : tableDefinition.getColumnFamilies()) {
        ht.addFamily(family);
      }

      byte[][] splits = tableDefinition.getSplitKeys();

      int numRegions = splits == null ? 1 : splits.length + 1;

      // set index corprocessor
      if (conf.getBoolean(Constants.BUILD_INDEX, false)) {
        if (LOG.isDebugEnabled()) {
          LOG.debug("Create table " + tableName + " with " + numRegions
              + " regions");
          LOG.debug("build index ");
        }

        try {
          ht.setValue(HTableDescriptor.SPLIT_POLICY,
              ConstantSizeRegionSplitPolicy.class.getName());
          ht.addCoprocessor(IndexCoprocessor.class.getName(),
              new Path(conf.get(Constants.HBASE_COPROCESSOR_LOCATION)), 1001,
              null);
          ht.addCoprocessor(RecordServiceEndpoint.class.getName(), new Path(
              conf.get(Constants.HBASE_COPROCESSOR_LOCATION)), 1001, null);
          if (LOG.isDebugEnabled()) {
            LOG.debug("added coprocessor"
                + conf.get(Constants.HBASE_COPROCESSOR_LOCATION));
          }
        } catch (IOException e1) {
          // TODO Auto-generated catch block e1.printStackTrace(); }
        }
      }

      try {
        hBaseAdmin.createTable(ht, splits);
        if (LOG.isDebugEnabled()) {
          LOG.debug("Create table done");
        }
      } catch (IOException e) {
        LOG.warn("This table [" + tableName + "] has been created before.",
            e.getStackTrace());
        updateTables();
      }
    }

    waitAvailable(tableName);
  }

  /**
   * Check if the HBase table is available with a light-weight manner.
   */
  private boolean isAvailable(String tableName) {
    AtomicBoolean available = availableMap.get(tableName);
    if (available == null) {
      available = new AtomicBoolean(false);
      availableMap.put(tableName, available);
    }
    if (available.get()) {
      return true;
    }
    return false;
  }

  /**
   * Check the table with RPC call.
   */
  private boolean isTableExist(String tableName) {
    try {
      if (hBaseAdmin.tableExists(tableName)) {
        return true;
      }
      return false;
    } catch (IOException e) {
      return false;
    }
  }

  /**
   * Wait the table to be available
   */
  private void waitAvailable(String tableName) {
    AtomicBoolean available = availableMap.get(tableName);
    while (!available.get()) {
      try {
        if (hBaseAdmin.isTableAvailable(tableName)) {
          available.getAndSet(true);
          return;
        }
      } catch (IOException e) {
        continue;
      }
    }
    return;
  }

  /**
   * Get the table list in HBase cluster.
   */
  private void updateTables() {
    try {
      HTableDescriptor[] tbls = hBaseAdmin.listTables();
      for (HTableDescriptor tbl : tbls) {
        String name = tbl.getNameAsString();
        tablesMultiset.add(name);
      }
    } catch (IOException e) {
      LOG.warn("IOException happened when list hbase tables", e.getStackTrace());
    }
  }

  /**
   * Update disabled tables
   */
  private void updateDisableTables() {
    for (String tableName : tablesMultiset.toArray(new String[0])) {
      try {
        if (hBaseAdmin.isTableDisabled(tableName)) {
          disabledTablesMultiset.add(tableName);
        } else {
          if (disabledTablesMultiset.contains(tableName)) {
            disabledTablesMultiset.remove(tableName);
          }
        }
      } catch (IOException e) {
        LOG.warn("IOException happened when judge is table disabled");
      }
    }
  }

  public void close() {
    exit.set(true);
  }

  @Override
  public void run() {
    while (!exit.get()) {
      try {
        updateDisableTables();
        sleep(SLEEP_TIME);
      } catch (InterruptedException e) {
      }
    }

    if (hBaseAdmin != null)
      try {
        hBaseAdmin.close();
      } catch (IOException e) {
        // TODO Auto-generated catch block
        e.printStackTrace();
      }
  }
}
