package com.cloudera.bigdata.analysis.index;

import java.io.IOException;
import java.util.concurrent.ExecutorService;

import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.hbase.TableName;
import org.apache.hadoop.hbase.client.HTable;
import org.apache.hadoop.hbase.ipc.RegionCoprocessorRpcChannel;
import org.apache.hadoop.hbase.protobuf.ProtobufUtil;

import com.google.protobuf.Service;

public class HTableNew extends HTable {
  private ExecutorService pool;

  public HTableNew(Configuration conf, String tableName) throws IOException {
    super(conf, tableName);
    pool = getDefaultExecutor(conf);
  }

  public <T extends Service> T coprocessorProxy(final Class<T> service,
      final byte[] row) {
    final RegionCoprocessorRpcChannel channel = new RegionCoprocessorRpcChannel(
        connection, TableName.valueOf(getTableName()), row, null, null);
    T instance = null;
    try {
      instance = ProtobufUtil.newServiceStub(service, channel);
    } catch (Exception e) {
      e.printStackTrace();
    }
    return instance;
  }
}
