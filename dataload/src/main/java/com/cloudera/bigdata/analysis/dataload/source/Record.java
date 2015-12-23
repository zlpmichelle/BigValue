package com.cloudera.bigdata.analysis.dataload.source;

import java.io.IOException;
import java.util.Map;

import org.apache.hadoop.hbase.client.Row;

import com.cloudera.bigdata.analysis.dataload.UpdateWorker;

public abstract class Record {
  /** Previous process before put the record into the hbase */
  public abstract void preConvert(int workerId) throws IOException;

  /** Convert the record to hbase's put */
  public abstract Map<HTableDefinition, Row[]> convertToHBaseRecord()
      throws IOException;

  /** Post process after put the record into the hbase */
  public abstract void finishUpdate() throws IOException;

  /** Configure the Record with the UpdateWorker */
  public void setUp(UpdateWorker workThread) {
  }

}
