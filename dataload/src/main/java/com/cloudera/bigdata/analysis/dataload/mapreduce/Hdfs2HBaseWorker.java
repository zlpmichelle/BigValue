package com.cloudera.bigdata.analysis.dataload.mapreduce;

import java.io.FileInputStream;
import java.io.FileNotFoundException;
import java.io.IOException;
import java.util.Arrays;
import java.util.List;
import java.util.Map;
import java.util.Properties;

import org.apache.commons.lang.StringUtils;
import org.apache.hadoop.fs.FileSystem;
import org.apache.hadoop.hbase.client.HBaseAdmin;
import org.apache.hadoop.hbase.util.Bytes;
import org.apache.hadoop.mapreduce.Job;
import org.apache.log4j.Logger;

import com.cloudera.bigdata.analysis.dataload.Constants;
import com.cloudera.bigdata.analysis.dataload.client.BaseWorker;
import com.cloudera.bigdata.analysis.dataload.client.Worker;
import com.cloudera.bigdata.analysis.dataload.exception.ETLException;
import com.cloudera.bigdata.analysis.dataload.extract.HBaseTargetFieldSpec;
import com.cloudera.bigdata.analysis.dataload.extract.HdfsSourceFieldSpec;
import com.cloudera.bigdata.analysis.dataload.transform.BulkLoadProcessFieldSpec;
import com.cloudera.bigdata.analysis.dataload.util.BulkLoadUtils;
import com.cloudera.bigdata.analysis.dataload.util.CommonUtils;

public class Hdfs2HBaseWorker extends BaseWorker implements Worker {
  private final static Logger LOGGER = Logger.getLogger(Hdfs2HBaseWorker.class);
  private HdfsSourceFieldSpec hdfsSourceFieldSpec;;
  private HBaseTargetFieldSpec hbaseTargetFieldSpec;
  private BulkLoadProcessFieldSpec bulkLoadProcessFieldSpec;
  Map<String, String> targetTableCellMap = null;
  private StringBuffer textRecordSpec = new StringBuffer();
  private static final String USAGE_STR = "com.cloudera.bigdata.analysis.dataload.etl.Hdfs2HBaseWorker <properties_file>";
  final static String EXCEPTION_LOG_TABLE_NAME = "exception_log";
  final static String EXCEPTION_LOG_TABLE_COLUMN_QUALIFIER_RAW_LINE = "line";
  final static String EXCEPTION_LOG_TABLE_COLUMN_QUALIFIER_ERROR_INFO = "info";

  final static byte[] EXCEPTION_LOG_TABLE_NAME_BYTES = Bytes
      .toBytes(EXCEPTION_LOG_TABLE_NAME);
  final static byte[] EXCEPTION_LOG_TABLE_COLUMN_FAMILY_BYTES = Bytes
      .toBytes("f");
  final static byte[] EXCEPTION_LOG_TABLE_COLUMN_QUALIFIER_RAW_LINE_BYTES = Bytes
      .toBytes(EXCEPTION_LOG_TABLE_COLUMN_QUALIFIER_RAW_LINE);
  final static byte[] EXCEPTION_LOG_TABLE_COLUMN_QUALIFIER_ERROR_INFO_BYTES = Bytes
      .toBytes(EXCEPTION_LOG_TABLE_COLUMN_QUALIFIER_ERROR_INFO);

  public Hdfs2HBaseWorker(String configFile) {
    super(configFile);
    // read configuration file to construct confMap
    this.hdfsSourceFieldSpec = new HdfsSourceFieldSpec(
        configReader.getConfMap());
    this.hbaseTargetFieldSpec = new HBaseTargetFieldSpec(
        configReader.getConfMap());
    this.bulkLoadProcessFieldSpec = new BulkLoadProcessFieldSpec(
        configReader.getConfMap());
  }

  @Override
  public void execute() throws Exception {
    doWork();
  }

  @Override
  protected void doWork() throws Exception {
    LOGGER.info("Parse HDFS source field spec..." + "\n"
        + hdfsSourceFieldSpec.toString());
    LOGGER.info("Parse HBase target field spec..." + "\n"
        + hbaseTargetFieldSpec.toString());
    LOGGER.info("Parse bulkload process field spec..." + "\n"
        + bulkLoadProcessFieldSpec.toString());

    // set conf from configuration properties files
    // for source table definition
    BulkLoadUtils.setConfFromSourceDefinition(hdfsSourceFieldSpec, conf);

    String fieldDelimiter = conf.get("hdfsSourceFileRecordFieldsDelimiter");
    String fieldsNumber = conf.get("hdfsSourceFileRecordFieldsNumber");

    // Type int
    String intFieldsNumber = conf.get("hdfsSourceFileRecordFieldTypeInt");
    List<String> intFields = Arrays.asList(StringUtils
        .splitByWholeSeparatorPreserveAllTokens(intFieldsNumber,
            Constants.DEFAULT_FIELD_DELIMITER));

    // convert input data text specification into text record spec
    // [f1:STRING, f2:STRING, f3:INT]

    long length = Long.parseLong(fieldsNumber);
    if (textRecordSpec == null || textRecordSpec.length() == 0) {
      for (int i = 1; i < length; i++) {
        if (intFields.contains(i + "")) {
          textRecordSpec
              .append(Constants.HDFS_SOURCE_FILE_RECORD_FIELD_NAME_PREFIX + i
                  + Constants.DEFAULT_FIELD_NAME_TYPE_VALUE_DELIMITER
                  + Constants.HDFS_SOURCE_FILE_RECORD_INT_FIELD_TYPE);
          textRecordSpec.append(fieldDelimiter);
        } else {
          textRecordSpec
              .append(Constants.HDFS_SOURCE_FILE_RECORD_FIELD_NAME_PREFIX + i
                  + Constants.DEFAULT_FIELD_NAME_TYPE_VALUE_DELIMITER
                  + Constants.HDFS_SOURCE_FILE_RECORD_DEFAULT_FIELD_TYPE);
          textRecordSpec.append(fieldDelimiter);
        }
      }

      // last field
      if (intFields.contains(length)) {
        textRecordSpec
            .append(Constants.HDFS_SOURCE_FILE_RECORD_FIELD_NAME_PREFIX
                + length + Constants.DEFAULT_FIELD_NAME_TYPE_VALUE_DELIMITER
                + Constants.HDFS_SOURCE_FILE_RECORD_INT_FIELD_TYPE);
      } else {
        textRecordSpec
            .append(Constants.HDFS_SOURCE_FILE_RECORD_FIELD_NAME_PREFIX
                + length + Constants.DEFAULT_FIELD_NAME_TYPE_VALUE_DELIMITER
                + Constants.HDFS_SOURCE_FILE_RECORD_DEFAULT_FIELD_TYPE);
      }
    }

    LOGGER.info("textRecordSpec:" + textRecordSpec.toString());
    conf.set("textRecordSpec", textRecordSpec.toString());

    // for target table definition
    targetTableCellMap = BulkLoadUtils.setConfFromTargetDefinition(
        hbaseTargetFieldSpec, conf, targetTableCellMap);

    // for bulkload process definition
    BulkLoadUtils.setConfFromBulkLoadStageDefinition(hbaseTargetFieldSpec,
        bulkLoadProcessFieldSpec, conf);
    if (conf.getBoolean("onlyGenerateSplitKeySpec", false)) {
      // only generate SplitKeySpec
    } else {
      // run bulkload
      if (targetTableCellMap == null) {
        throw new ETLException(
            "Failed to get targetTableCellMap from target definition.");
      }

      // create hbase table and generate hfile output
      long startTime = System.currentTimeMillis();
      fs = FileSystem.get(conf);
      hbaseAdmin = new HBaseAdmin(conf);
      Job job = BulkLoadUtils.createHBaseTableAndGenerateHfile(fs, hbaseAdmin,
          conf, targetTableCellMap);

      // LoadIncrementalHFiles to bulk load data from hfile into hbase table
      BulkLoadUtils.loadIncrementalHFiles(conf, job, startTime);
    }
  }

  public static void main(String[] args) throws Exception {
    Properties props = new Properties();
    if (args.length < 1) {
      System.out.println(USAGE_STR);
    }

    try {
      props.load(new FileInputStream(args[0]));
    } catch (FileNotFoundException e) {
      LOGGER.error("", e);
    } catch (IOException e) {
      LOGGER.error("", e);
    }

    // set the CDH version
    CommonUtils.setCdhVersion(props.getProperty("cdh.version"));

    Hdfs2HBaseWorker worker = new Hdfs2HBaseWorker(args[0]);
    worker.execute();
  }
}
