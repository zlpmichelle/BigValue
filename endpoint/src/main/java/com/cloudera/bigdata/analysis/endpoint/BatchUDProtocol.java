package com.cloudera.bigdata.analysis.endpoint;

import java.io.IOException;
import java.util.List;

import org.apache.hadoop.hbase.KeyValue;
import org.apache.hadoop.hbase.client.Scan;
import org.apache.hadoop.hbase.ipc.CoprocessorProtocol;

public interface BatchUDProtocol extends CoprocessorProtocol{
	<T,S> long batch_update(String tableName,List<KeyValue> kvs,String batchSize,Scan scan) throws IOException;
	
	<T,S> long batch_delete(String tableName,String batchSize,Scan scan) throws IOException;
	
}
