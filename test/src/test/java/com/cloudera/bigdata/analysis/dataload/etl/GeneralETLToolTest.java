package com.cloudera.bigdata.analysis.dataload.etl;

import org.junit.After;
import org.junit.AfterClass;
import org.junit.Before;
import org.junit.BeforeClass;
import org.junit.Test;

import com.cloudera.bigdata.analysis.dataload.mapreduce.HBase2HBaseWorker;
import com.cloudera.bigdata.analysis.dataload.mapreduce.Hdfs2HBaseWorker;

public class GeneralETLToolTest {

  @BeforeClass
  public static void setUpBeforeClass() throws Exception {
  }

  @AfterClass
  public static void tearDownAfterClass() throws Exception {
  }

  @Before
  public void setUp() throws Exception {
  }

  @After
  public void tearDown() throws Exception {
  }

  @Test
  public void testHBaseSourceWorker_testCase_1() throws Exception {
    HBase2HBaseWorker worker = new HBase2HBaseWorker(
        "etl-hbase2hbase-conf.properties");
    worker.execute();
  }

  @Test
  public void testHdfsSourceWorker_testCase_2() throws Exception {
    Hdfs2HBaseWorker worker = new Hdfs2HBaseWorker(
        "etl-hdfs2hbase-conf.properties");
    worker.execute();
  }
}
