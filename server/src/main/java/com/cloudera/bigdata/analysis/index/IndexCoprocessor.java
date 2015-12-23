package com.cloudera.bigdata.analysis.index;

import java.io.IOException;

import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.hbase.CoprocessorEnvironment;
import org.apache.hadoop.hbase.client.Delete;
import org.apache.hadoop.hbase.client.Durability;
import org.apache.hadoop.hbase.client.Put;
import org.apache.hadoop.hbase.coprocessor.BaseRegionObserver;
import org.apache.hadoop.hbase.coprocessor.ObserverContext;
import org.apache.hadoop.hbase.coprocessor.RegionCoprocessorEnvironment;
import org.apache.hadoop.hbase.regionserver.HRegion;
import org.apache.hadoop.hbase.regionserver.wal.WALEdit;
import org.apache.hadoop.hbase.util.Bytes;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import com.cloudera.bigdata.analysis.index.search.SearchStrategyDecider;
import com.cloudera.bigdata.analysis.index.util.IndexUtil;
import com.cloudera.bigdata.analysis.index.util.RegionUtil;
import com.cloudera.bigdata.analysis.index.util.RowKeyUtil;

@SuppressWarnings("deprecation")
public class IndexCoprocessor extends BaseRegionObserver {

  private static final Logger LOG = LoggerFactory
      .getLogger(IndexCoprocessor.class);

  private static final long PROTOCOL_VERSION = 1L;

  // defined only for Endpoint implementation, so it can have way to
  // access region services.
  private RegionCoprocessorEnvironment regionEnv;

  private HRegion region;

  private byte[] regionStartKey;

  private Configuration conf;

  private IndexEntryBuilderGroup indexEntryBuilderGroup;

  private SearchStrategyDecider searchStrategyDecider;

  private String currentTable;

  /*-------------------------------------------   Coprocessor Methods   --------------------------------------------*/

  @Override
  public void start(CoprocessorEnvironment e) throws IOException {
    // this coprocessor is only running on region.
    if (e instanceof RegionCoprocessorEnvironment) {
      regionEnv = (RegionCoprocessorEnvironment) e;
      region = regionEnv.getRegion();
      conf = regionEnv.getConfiguration();
      regionStartKey = RegionUtil.getRegionStartKey(region);
      if (LOG.isDebugEnabled()) {
        LOG.debug("region id: " + region.getRegionId() + " toString:"
            + region.toString());
        LOG.debug("regionStartKey: "
            + IndexUtil.convertByteArrayToString(regionStartKey));
        LOG.debug("table name: " + Constants.CURRENT_TABLE);
      }
      // fix bug here
      indexEntryBuilderGroup = IndexEntryBuilderGroup.getInstance(IndexUtil
          .convertStringToByteArray(Constants.CURRENT_TABLE));
      if (LOG.isDebugEnabled()) {
        LOG.debug("indexEntryBuilderGroup: " + Constants.CURRENT_TABLE);
      }
      searchStrategyDecider = new SearchStrategyDecider();
      LOG.info("Coprocessor: [" + getClass().getName() + "] has started!");
    } else {
      throw new RuntimeException(IndexCoprocessor.class.getName()
          + "is only running on region!");
    }
  }

  /*-----------------------------------------   Region-Observer Methods   ------------------------------------------*/

  /**
   * Intercept put request, if it's a record put, build relative index entries
   * and put them into database at the same. otherwise, i.e. an index entry put,
   * ignore directly.
   */
  @Override
  public void postPut(ObserverContext<RegionCoprocessorEnvironment> e, Put put,
      WALEdit edit, final Durability durability) throws IOException {
    currentTable = IndexUtil.getCurrentTableName(Constants.CURRENT_PRO_HDFS);
    // refresh
    if (LOG.isDebugEnabled()) {
      LOG.debug("currentTable is " + currentTable);
    }
    indexEntryBuilderGroup = IndexEntryBuilderGroup.getInstance(IndexUtil
        .convertStringToByteArray(currentTable));

    byte[] rowKey = put.getRow();
    if (LOG.isDebugEnabled()) {
      LOG.debug("Intercepted Put: " + Bytes.toStringBinary(rowKey));
    }
    // only if a record put...
    if (RowKeyUtil.isRecord(rowKey)) {
      if (LOG.isDebugEnabled()) {
        LOG.debug("rowkey: " + IndexUtil.convertByteArrayToString(rowKey));
      }
      Put[] indexPuts = indexEntryBuilderGroup.build(regionStartKey, put);
      region.batchMutate(indexPuts);
    }
  }

  /**
   * TBD, this method might remove in the feature. we might use TTL to delete
   * records automatically.
   */
  @Override
  public void postDelete(ObserverContext<RegionCoprocessorEnvironment> e,
      Delete delete, WALEdit edit, final Durability durability)
      throws IOException {
    byte[] rowKey = delete.getRow();
    if (LOG.isDebugEnabled()) {
      LOG.debug("Intercepted Delete: " + Bytes.toStringBinary(rowKey));
    }
    // only if a record delete...
    if (RowKeyUtil.isRecord(rowKey)) {
    }
  }
}