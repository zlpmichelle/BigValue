package com.cloudera.bigdata.analysis.index;

import java.util.HashMap;
import java.util.LinkedHashSet;
import java.util.Map;
import java.util.Set;

import org.apache.hadoop.hbase.client.Put;
import org.apache.hadoop.hbase.util.Bytes;
import org.apache.hadoop.hdfs.util.ByteArray;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import com.cloudera.bigdata.analysis.index.util.IndexUtil;

/**
 * A group of {@link IndexEntryBuilder}, one group for one table, Each group
 * creates all kinds of indexes of a table.
 */
public class IndexEntryBuilderGroup {

  private static final Logger LOG = LoggerFactory
      .getLogger(IndexEntryBuilderGroup.class);

  private static Map<ByteArray, IndexEntryBuilderGroup> indexEntryBuilderGroupMap;

  static {
    indexEntryBuilderGroupMap = new HashMap<ByteArray, IndexEntryBuilderGroup>();
    // all tables with all indexes
    Map<ByteArray, Set<Index>> allIndexes = Index.getAll();

    for (ByteArray table : allIndexes.keySet()) {
      // all indexes of a table for a indexEntryBuilders
      Set<IndexEntryBuilder> indexEntryBuilders = new LinkedHashSet<IndexEntryBuilder>();
      Set<Index> indexes = Index.get(table);
      for (Index index : indexes) {
        // one index of a table for a indexEntryBuilder
        IndexEntryBuilder indexEntryBuilder = new IndexEntryBuilder(index);
        if (LOG.isDebugEnabled()) {
          LOG.debug("Create: " + indexEntryBuilder);
        }
        indexEntryBuilders.add(indexEntryBuilder);
      }
      // all indexes of a table for a indexEntryBuilders for a
      // IndexEntryBuilderGroup
      indexEntryBuilderGroupMap.put(table, new IndexEntryBuilderGroup(
          indexEntryBuilders));
    }
  }

  private Set<IndexEntryBuilder> builders;

  private IndexEntryBuilderGroup(Set<IndexEntryBuilder> builders) {
    this.builders = builders;
  }

  public static IndexEntryBuilderGroup getInstance(ByteArray table) {
    return indexEntryBuilderGroupMap.get(table);
  }

  /**
   * This should be a Template Method!
   */
  public Put[] build(byte[] regionStartKey, Put put) {
    Put[] puts = new Put[builders.size()];
    int i = 0;
    for (IndexEntryBuilder builder : builders) {
      Put indexEntryPut = builder.build(regionStartKey, put);
      puts[i++] = indexEntryPut;
      if (LOG.isDebugEnabled()) {
        LOG.debug("Create Index Entry Put:"
            + Bytes.toStringBinary(indexEntryPut.getRow()));
      }
    }
    return puts;
  }

  public static void main(String args[]) {
    LOG.info("table name: " + args[0]);
    // fix bug here
    IndexEntryBuilderGroup indexEntryBuilderGroup = IndexEntryBuilderGroup
        .getInstance(IndexUtil.convertStringToByteArray(args[0]));
    LOG.info("indexEntryBuilderGroup: "
        + indexEntryBuilderGroup.builders.toString());
  }
}
