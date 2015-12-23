package com.cloudera.bigdata.analysis.dataload.util;

import java.io.BufferedReader;
import java.io.IOException;
import java.io.InputStream;
import java.io.InputStreamReader;
import java.util.Map;

import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.fs.FileSystem;
import org.apache.hadoop.fs.Path;
import org.apache.hadoop.hbase.HBaseConfiguration;
import org.apache.hadoop.hbase.client.HBaseAdmin;
import org.apache.hadoop.hbase.client.HTable;
import org.apache.hadoop.hbase.client.Put;
import org.apache.hadoop.hbase.io.ImmutableBytesWritable;
import org.apache.hadoop.hbase.mapreduce.HFileOutputFormat2;
import org.apache.hadoop.hbase.mapreduce.LoadIncrementalHFiles;
import org.apache.hadoop.hbase.mapreduce.PutSortReducer;
import org.apache.hadoop.hbase.mapreduce.TableMapReduceUtil;
import org.apache.hadoop.mapreduce.Job;
import org.apache.hadoop.mapreduce.lib.input.FileInputFormat;
import org.apache.hadoop.mapreduce.lib.input.TextInputFormat;
import org.apache.hadoop.mapreduce.lib.output.FileOutputFormat;
import org.apache.hadoop.util.ToolRunner;
import org.apache.log4j.Logger;

import com.cloudera.bigdata.analysis.dataload.Constants;
import com.cloudera.bigdata.analysis.dataload.exception.ETLException;
import com.cloudera.bigdata.analysis.dataload.extract.HBaseTargetFieldSpec;
import com.cloudera.bigdata.analysis.dataload.extract.HdfsSourceFieldSpec;
import com.cloudera.bigdata.analysis.dataload.io.CombineTextInputFormat;
import com.cloudera.bigdata.analysis.dataload.mapreduce.CustomizedCombineHBaseRowMapper;
import com.cloudera.bigdata.analysis.dataload.mapreduce.CustomizedHBaseRowMapper;
import com.cloudera.bigdata.analysis.dataload.mapreduce.ExtendedHBaseRowMapper;
import com.cloudera.bigdata.analysis.dataload.transform.BulkLoadProcessFieldSpec;
import com.cloudera.bigdata.analysis.dataload.transform.TargetTableSpec;
import com.cloudera.bigdata.analysis.index.util.IndexUtil;

public class BulkLoadUtils {
  private final static Logger LOGGER = Logger.getLogger(BulkLoadUtils.class);
  final static String EXCEPTION_LOG_TABLE_NAME = "exception_log";

  public static void setConfFromSourceDefinition(
      HdfsSourceFieldSpec hdfsSourceFieldSpec, Configuration conf) {
    // Source Definition, all are required
    String cdhHBaseMasterIpaddress = hdfsSourceFieldSpec
        .getCdhHBaseMasterIpaddress();
    String hdfsSourceFileInputPath = hdfsSourceFieldSpec
        .getHdfsSourceFileInputPath();
    String hdfsSourceFileEncoding = hdfsSourceFieldSpec
        .getHdfsSourceFileEncoding();
    String hdfsSourceFileRecordFieldsDelimiter = hdfsSourceFieldSpec
        .getHdfsSourceFileRecordFieldsDelimiter();
    String hdfsSourceFileRecordFieldsNumber = hdfsSourceFieldSpec
        .getHdfsSourceFileRecordFieldsNumber();
    String hdfsSourceFileRecordFieldTypeInt = hdfsSourceFieldSpec
        .getHdfsSourceFileRecordFieldTypeInt();

    if (cdhHBaseMasterIpaddress == null || cdhHBaseMasterIpaddress.isEmpty()
        || hdfsSourceFileInputPath == null || hdfsSourceFileInputPath.isEmpty()
        || hdfsSourceFileEncoding == null || hdfsSourceFileEncoding.isEmpty()
        || hdfsSourceFileRecordFieldsDelimiter == null
        || hdfsSourceFileRecordFieldsDelimiter.isEmpty()
        || hdfsSourceFileRecordFieldsNumber == null
        || hdfsSourceFileRecordFieldsNumber.isEmpty()
        || hdfsSourceFileRecordFieldTypeInt == null
        || hdfsSourceFileRecordFieldTypeInt.isEmpty()) {
      ETLException.handle("Failed to run bulkload, need to specify value of \""
          + Constants.CDH_HBASE_MASTER_IPADDRESS + "\" and \""
          + Constants.HDFS_SOURCE_FILE_INPUT_PATH + "\" and \""
          + Constants.HDFS_SOURCE_FILE_ENCODING + "\" and \""
          + Constants.HDFS_SOURCE_FILE_RECORD_FIELDS_DELIMITER + "\" and \""
          + Constants.HDFS_SOURCE_FILE_RECORD_FIELDS_TYPE_INT + "\" and \""
          + Constants.HDFS_SOURCE_FILE_RECORD_FIELDS_NUMBER + "\"");
    } else {
      conf.set("cdhHBaseMasterIpaddress", cdhHBaseMasterIpaddress);
      conf.set("hdfsSourceFileInputPath", hdfsSourceFileInputPath);
      conf.set("hdfsSourceFileEncoding", hdfsSourceFileEncoding);
      conf.set("hdfsSourceFileRecordFieldsDelimiter",
          hdfsSourceFileRecordFieldsDelimiter);
      conf.set("hdfsSourceFileRecordFieldsNumber",
          hdfsSourceFileRecordFieldsNumber);
      conf.set("hdfsSourceFileRecordFieldTypeInt",
          hdfsSourceFileRecordFieldTypeInt);
    }
  }

  public static Map<String, String> setConfFromTargetDefinition(
      HBaseTargetFieldSpec hbaseTargetFieldSpec, Configuration conf,
      Map<String, String> targetTableCellMap) {
    // Target HBase Definition
    // target hbase table, all are required
    String hbaseGeneratedHfilesOutputPath = hbaseTargetFieldSpec
        .getHbaseGeneratedHfilesOutputPath();
    String hbaseTargetTableName = hbaseTargetFieldSpec
        .getHbaseTargetTableName();
    boolean hbaseTargetWriteToWAL = hbaseTargetFieldSpec
        .isHbaseTargetWriteToWAL();
    if (hbaseGeneratedHfilesOutputPath == null
        || hbaseGeneratedHfilesOutputPath.isEmpty()
        || hbaseTargetTableName == null || hbaseTargetTableName.isEmpty()
        || String.valueOf(hbaseTargetWriteToWAL).isEmpty()) {
      ETLException.handle("Failed to run bulkload, need to specify value of \""
          + Constants.HBASE_GENERATED_HFILES_OUTPUT_PATH + "\" and \""
          + Constants.HBASE_TARGET_TABLE_NAME + "\" and \""
          + Constants.HBASE_TARGET_WRITE_TO_WAL_FLAG + "\"");
    }
    // ETL for hbase rowkey, column families and column, all are required for
    // CustomizedHBaseRowMapper
    conf.set("hbaseGeneratedHfilesOutputPath", hbaseGeneratedHfilesOutputPath);
    conf.set("hbaseTargetTableName", hbaseTargetTableName);
    conf.setBoolean("hbaseTargetWriteToWAL", hbaseTargetWriteToWAL);

    targetTableCellMap = hbaseTargetFieldSpec.getTargetTableCellMap();
    if (targetTableCellMap == null) {
      try {
        throw new ETLException(
            "Failed to get targetTableCellMap from target definition.");
      } catch (ETLException e) {
        e.printStackTrace();
      }
    } else {
      conf.set("hbaseTargetTableCellMapString", CommonUtils.getStringFromMap(
          targetTableCellMap, Constants.COLUMN_ENTRY_SEPARATOR,
          Constants.COLUMN_KEY_VALUE_SEPARATOR));
      LOGGER.info("hbaseTargetTableCellMapString is "
          + conf.get("hbaseTargetTableCellMapString"));
    }

    return targetTableCellMap;
  }

  public static void setConfFromBulkLoadStageDefinition(
      HBaseTargetFieldSpec hbaseTargetFieldSpec,
      BulkLoadProcessFieldSpec bulkLoadProcessFieldSpec, Configuration conf)
      throws Exception {
    // BulkLoad Stage Definition, all are optional

    boolean buildIndex = bulkLoadProcessFieldSpec.isBuildIndex();
    conf.setBoolean("buildIndex", buildIndex);

    boolean onlyGenerateSplitKeySpec = bulkLoadProcessFieldSpec
        .isOnlyGenerateSplitKeySpec();
    conf.setBoolean("onlyGenerateSplitKeySpec", onlyGenerateSplitKeySpec);

    boolean nativeTaskEnabled = bulkLoadProcessFieldSpec.isNativeTaskEnabled();
    conf.setBoolean("nativeTaskEnabled", nativeTaskEnabled);
    // enable native task
    if (nativeTaskEnabled) {
      conf.set("mapreduce.map.output.collector.delegator.class",
          "org.apache.hadoop.mapred.nativetask.NativeMapOutputCollectorDelegator");
    }

    if (conf.getBoolean("buildIndex", false)) {
      // make sure there is a index configuration for this current bulkload
      // table
      if (!IndexUtil.isIndexConfAvailableForLoad(conf
          .get("hbaseTargetTableName"))) {
        ETLException
            .handle("Failed to load data because there is index configuration error!");
      }

      String regionQuantity = bulkLoadProcessFieldSpec.getRegionQuantity();
      String indexConfFileName = bulkLoadProcessFieldSpec
          .getIndexConfFileName();
      String hbaseCoprocessorLocation = bulkLoadProcessFieldSpec
          .getHBaseCoprocessorLocation();
      if (regionQuantity == null || regionQuantity.isEmpty()
          || indexConfFileName == null || indexConfFileName.isEmpty()) {
        ETLException.handle("Need to specify value for "
            + Constants.REGION_QUANTITY + " and "
            + Constants.INDEX_CONF_FILE_NAME + " when buildIndex is true.");
      } else {
        conf.set("regionQuantity", regionQuantity);
        conf.set("indexConfFileName", indexConfFileName);
        conf.set("hbaseCoprocessorLocation", hbaseCoprocessorLocation);

        // generate split key spec for quick index build
        // split according to the regionQuantity
        StringBuffer keys = new StringBuffer();
        byte[][] splitKeys = IndexUtil.calcSplitKeys(Integer.parseInt(conf
            .get("regionQuantity")));
        for (int i = 0; i < splitKeys.length - 1; i++) {
          keys.append(CommonUtils.convertByteArrayToString(splitKeys[i]));
          keys.append(",");
        }
        keys.append(CommonUtils
            .convertByteArrayToString(splitKeys[splitKeys.length - 1]));

        String hbaseTargetTableSplitKeySpec = keys.toString();
        conf.set("hbaseTargetTableSplitKeySpec", hbaseTargetTableSplitKeySpec);

        System.out.println(Constants.HBASE_TARGET_TABLE_SPLIT_KEY_SPEC + "="
            + conf.get("hbaseTargetTableSplitKeySpec"));
      }
    } else {
      generateHbaseTargetTableSplitKeySpec(hbaseTargetFieldSpec,
          bulkLoadProcessFieldSpec, conf);
    }

    // use ExtensibleHBaseRowConverter or not
    boolean extendedHbaseRowConverter = false;
    String extendedHBaseRowConverterClass = bulkLoadProcessFieldSpec
        .getExtendedHBaseRowConverterClass();
    if (extendedHBaseRowConverterClass == null
        || extendedHBaseRowConverterClass.isEmpty()) {
      LOGGER.info("Use CustomizedHBaseRowConverter");
    } else {
      LOGGER.info("Use ExtensibleHBaseRowConverter, Class name is: "
          + extendedHBaseRowConverterClass);
      extendedHbaseRowConverter = true;
      conf.set("extendedHBaseRowConverterClass", extendedHBaseRowConverterClass);
    }
    conf.setBoolean("extendedHbaseRowConverter", extendedHbaseRowConverter);

    // specify the input Split Size for combineInputFiles
    boolean combineInputFiles = false;
    String inputSplitSize = null;
    inputSplitSize = bulkLoadProcessFieldSpec.getInputSplitSize();
    if (inputSplitSize == null || inputSplitSize.isEmpty()) {
      LOGGER
          .info("Not use input files combination before mapper, specify value of \"inputSplitSize\"  if you want to use it");
    } else {
      LOGGER
          .info("Use input files combination before mapper, combination of inputSplitSize is "
              + inputSplitSize);
      combineInputFiles = true;
      // set input files combination in mapreduce
      conf.set("inputSplitSize", inputSplitSize);
      conf.set("mapreduce.input.fileinputformat.split.minsize", inputSplitSize);
      conf.set("mapreduce.input.fileinputformat.split.maxsize", inputSplitSize);
      conf.set("mapreduce.input.fileinputformat.split.minsize.per.rack",
          inputSplitSize);
      conf.set("mapreduce.input.fileinputformat.split.minsize.per.node",
          inputSplitSize);
    }
    conf.setBoolean("combineInputFiles", combineInputFiles);

    // 307M 70 mapper
    conf.set("inputSplitSize", "419439400");
    conf.set("mapreduce.input.fileinputformat.split.minsize", "419439400");
    conf.set("mapreduce.input.fileinputformat.split.maxsize", "419439400");
    conf.set("mapreduce.input.fileinputformat.split.minsize.per.rack",
        "419439400");
    conf.set("mapreduce.input.fileinputformat.split.minsize.per.node",
        "419439400");

    if (conf.getBoolean("extendedHbaseRowConverter", false)
        && (conf.getBoolean("combineInputFiles", false))) {
      LOGGER
          .warn("Use extensibleHbaseRowConverter to run bulkload, if want to use input files combination before mapper, should set keep "
              + "\"extendedHBaseRowConverterClass\" empty");
    }

    String importDate = bulkLoadProcessFieldSpec.getImportDate();
    String validatorClass = bulkLoadProcessFieldSpec.getValidatorClass();
    boolean createMalformedTable = bulkLoadProcessFieldSpec
        .isCreateMalformedTable();

    if (conf.get("importDate") != null && !conf.get("importDate").isEmpty()) {
      conf.set("importDate", importDate);
    }
    conf.set("validatorClass", validatorClass);
    conf.setBoolean("createMalformedTable", createMalformedTable);
  }

  public static void generateHbaseTargetTableSplitKeySpec(
      HBaseTargetFieldSpec hbaseTargetFieldSpec,
      BulkLoadProcessFieldSpec bulkLoadProcessFieldSpec, Configuration conf)
      throws Exception {
    boolean preCreateRegions = bulkLoadProcessFieldSpec.isPreCreateRegions();
    String rowkeyPrefix = null;
    String recordCountFile = null;
    String recordsNumPerRegion = null;
    String hbaseTargetTableSplitKeySpec = hbaseTargetFieldSpec
        .getHbaseTargetTableSplitKeySpec();
    if (preCreateRegions) {
      conf.setBoolean("preCreateRegions", preCreateRegions);
      // get rowkey prefix
      rowkeyPrefix = bulkLoadProcessFieldSpec.getRowkeyPrefix();
      // get records number per region
      recordsNumPerRegion = bulkLoadProcessFieldSpec.getRecordsNumPerRegion();

      if (hbaseTargetTableSplitKeySpec != null
          && !hbaseTargetTableSplitKeySpec.isEmpty()) {
        if ((rowkeyPrefix == null || rowkeyPrefix.isEmpty())
            && (recordsNumPerRegion == null || recordsNumPerRegion.isEmpty())) {
          System.out.println(Constants.HBASE_TARGET_TABLE_SPLIT_KEY_SPEC + "="
              + hbaseTargetTableSplitKeySpec);
          conf.set("hbaseTargetTableSplitKeySpec", hbaseTargetTableSplitKeySpec);
          System.out.println("Successfully set "
              + Constants.HBASE_TARGET_TABLE_SPLIT_KEY_SPEC);
        } else {
          ETLException.handle("No need to specify value for "
              + Constants.ROWKEY_PREFIX + " and "
              + Constants.RECORDS_NUM_PER_REGION
              + " when hbase.target.table.split.key.spec is specified.");
        }
      } else {
        if (rowkeyPrefix == null || rowkeyPrefix.isEmpty()) {
          ETLException.handle("Failed to run " + Constants.PRE_CREATE_REGIONS
              + ", need to specify value of \"rowkeyPrefix\"");
        }
        conf.set("rowkeyPrefix", rowkeyPrefix);

        if (recordsNumPerRegion == null || recordsNumPerRegion.isEmpty()) {
          // generate hive script and record count file
          recordCountFile = RecordCountHiveShellUtils.getRecordCountFile(conf);
          RecordCountHiveShellUtils.getTotalRecordNum(conf);

          // calculate records number per region
          recordsNumPerRegion = RecordCountHiveShellUtils
              .calculateRecordNumPerRegion(conf);
        } else {
          // generate hive script and record count file
          recordCountFile = RecordCountHiveShellUtils.getRecordCountFile(conf);
          RecordCountHiveShellUtils.getTotalRecordNum(conf);
          if (Long.parseLong(recordsNumPerRegion) > Long.parseLong(conf
              .get("totalRecordNum"))) {
            recordsNumPerRegion = conf.get("totalRecordNum");
          }
        }

        // generate splitKeySpec string
        if (hbaseTargetTableSplitKeySpec != null
            && !hbaseTargetTableSplitKeySpec.isEmpty()) {
          LOGGER
              .warn("No need to specify hbase.target.table.split.key.spec when preCreateRegions is true");
        }
        hbaseTargetTableSplitKeySpec = RegionSplitKeyUtils
            .genSplitKeysFromFile(recordCountFile,
                Long.parseLong(recordsNumPerRegion));
        System.out.println(Constants.HBASE_TARGET_TABLE_SPLIT_KEY_SPEC + "="
            + hbaseTargetTableSplitKeySpec);
        conf.set("hbaseTargetTableSplitKeySpec", hbaseTargetTableSplitKeySpec);
      }
    } else {
      // not pre-create regions
      conf.set("hbaseTargetTableSplitKeySpec", "");
    }
  }

  public static Job createHBaseTableAndGenerateHfile(FileSystem fs,
      HBaseAdmin hbaseAdmin, Configuration conf,
      Map<String, String> targetTableCellMap) throws Exception {
    TargetTableSpec targetTableSpec = new TargetTableSpec(
        conf.get("hbaseTargetTableName"), CommonUtils.getStringFromMap(
            targetTableCellMap, Constants.COLUMN_ENTRY_SEPARATOR,
            Constants.COLUMN_KEY_VALUE_SEPARATOR),
        conf.get("hbaseTargetTableSplitKeySpec"));

    // create hbase table
    if (!CommonUtils.doesTableExist(hbaseAdmin,
        conf.get("hbaseTargetTableName"))) {
      CommonUtils.createTable(conf, hbaseAdmin, targetTableSpec);
    } else {
      int existedRegionQuantity = CommonUtils.getExistedTableRegionsNum(
          hbaseAdmin, conf.get("hbaseTargetTableName"));
      System.out.println("Table " + conf.get("hbaseTargetTableName")
          + " is existed. The existed region number is "
          + existedRegionQuantity);

      // update the existedRegionQuantity and split key spec
      conf.set("regionQuantity", String.valueOf(existedRegionQuantity));
      
      if (conf.getBoolean("buildIndex", false)) {
        // generate split key spec for quick index build
        // split according to the regionQuantity
        StringBuffer keys = new StringBuffer();
        byte[][] splitKeys = IndexUtil.calcSplitKeys(Integer.parseInt(conf
            .get("regionQuantity")));
        for (int i = 0; i < splitKeys.length - 1; i++) {
          keys.append(CommonUtils.convertByteArrayToString(splitKeys[i]));
          keys.append(",");
        }
        keys.append(CommonUtils
            .convertByteArrayToString(splitKeys[splitKeys.length - 1]));

        String hbaseTargetTableSplitKeySpec = keys.toString();
        conf.set("hbaseTargetTableSplitKeySpec", hbaseTargetTableSplitKeySpec);
        System.out.println("Updated "
            + Constants.HBASE_TARGET_TABLE_SPLIT_KEY_SPEC + "="
            + conf.get("hbaseTargetTableSplitKeySpec"));
      }
    }

    // create malformed table
    if (conf.getBoolean("createMalformedTable", false)) {
      if (!CommonUtils.doesTableExist(hbaseAdmin, EXCEPTION_LOG_TABLE_NAME)) {
        CommonUtils.createMalformedLineLogTable(hbaseAdmin,
            EXCEPTION_LOG_TABLE_NAME);
      }
    }

    // hbase hfile output path
    String hbaseGeneratedHfilesOutputPath = conf
        .get("hbaseGeneratedHfilesOutputPath");
    if (hbaseGeneratedHfilesOutputPath != null
        && !hbaseGeneratedHfilesOutputPath.isEmpty()) {
      Path outputDir = new Path(hbaseGeneratedHfilesOutputPath);
      if (fs.exists(outputDir)) {
        fs.delete(outputDir, true);
        LOGGER.info("Delete hfile output path.");
      }
    }

    // run mapreduce to generate hfile
    Job job = createSubmittableJob(conf, targetTableSpec);
    job.waitForCompletion(true);
    return job;
  }

  public static void loadIncrementalHFiles(Configuration conf, Job job,
      long startTime) throws Exception {
    String hbaseGeneratedHfilesOutputPath = conf
        .get("hbaseGeneratedHfilesOutputPath");
    String cdhHBaseMasterIpaddress = conf.get("cdhHBaseMasterIpaddress");
    if (job.isSuccessful()) {
      if (hbaseGeneratedHfilesOutputPath != null) {
        try {
          // chmod 777 for hfile
          LOGGER.info("Executing Shell:\n" + "/bin/sh -c ssh "
              + cdhHBaseMasterIpaddress
              + " sudo -u hdfs hadoop fs -chmod -R 777 "
              + hbaseGeneratedHfilesOutputPath);
          Process prs = Runtime.getRuntime().exec(
              new String[] {
                  "/bin/sh",
                  "-c",
                  "ssh " + cdhHBaseMasterIpaddress
                      + " sudo -u hdfs hadoop fs -chmod -R 777 "
                      + hbaseGeneratedHfilesOutputPath });

          String line;
          InputStream es = prs.getErrorStream();
          BufferedReader br = new BufferedReader(new InputStreamReader(es));
          while ((line = br.readLine()) != null) {
            LOGGER.info(line);
            if (line.contains("FAILED") || line.contains("ERROR")
                || line.contains("Failed") || line.contains("Error")) {
              LOGGER.error(line);
              throw new RuntimeException(line);
            }
          }

          // give hbase bulkload hfile output permission
          LOGGER.info("Executing Shell:\n" + "/bin/sh -c ssh "
              + cdhHBaseMasterIpaddress
              + " sudo -u hdfs hadoop fs -chown -R hbase "
              + hbaseGeneratedHfilesOutputPath);
          prs = Runtime.getRuntime().exec(
              new String[] {
                  "/bin/sh",
                  "-c",
                  "ssh " + cdhHBaseMasterIpaddress
                      + " sudo -u hdfs hadoop fs -chown -R hbase "
                      + hbaseGeneratedHfilesOutputPath });

          es = prs.getErrorStream();
          br = new BufferedReader(new InputStreamReader(es));
          while ((line = br.readLine()) != null) {
            LOGGER.info(line);
            if (line.contains("FAILED") || line.contains("ERROR")
                || line.contains("Failed") || line.contains("Error")) {
              LOGGER.error(line);
              throw new RuntimeException(line);
            }
          }

          br.close();
        } catch (IOException e) {
          e.printStackTrace();
        }

        // LoadIncrementalHFiles to bulk load data from hfile into hbase table
        if (hbaseGeneratedHfilesOutputPath != null) {
          String[] args = { hbaseGeneratedHfilesOutputPath,
              conf.get("hbaseTargetTableName") };
          int ret = ToolRunner.run(
              new LoadIncrementalHFiles(HBaseConfiguration.create()), args);
          LOGGER.info("Successfully Bulk Load data into HBase table!");
          System.out.println("The Total Time of Bulk Load is: "
              + (System.currentTimeMillis() - startTime) + " millisecond!");
          System.exit(ret);
        }
      } else {
        LOGGER.info("Successfully Direct Load data into HBase table!");
        System.out.println("The Total Time of Direct Load is: "
            + (System.currentTimeMillis() - startTime) + " millisecond!");
      }
    } else {
      throw new Exception("Failed to Load data into HBase table");
    }
  }

  private static Job createSubmittableJob(Configuration conf,
      TargetTableSpec tableSpec) throws Exception {
    String tableName = tableSpec.getTableName();
    String jobPrefixName = null;
    if (conf.getBoolean("extendedHbaseRowConverter", false)) {
      jobPrefixName = "ExtendedBulkLoad";
    } else {
      if (conf.getBoolean("combineInputFiles", false)) {
        jobPrefixName = "CustomizedBulkLoadWithCombineInputFiles";
      } else {
        jobPrefixName = "CustomizedBulkLoad";
      }
    }
    Job job = new Job(conf, jobPrefixName + "_" + tableName);
    FileInputFormat.setInputPaths(job, conf.get("hdfsSourceFileInputPath"));

    if (conf.getBoolean("extendedHbaseRowConverter", false)) {
      job.setJarByClass(ExtendedHBaseRowMapper.class);
      job.setMapperClass(ExtendedHBaseRowMapper.class);
      job.setInputFormatClass(TextInputFormat.class);
    } else {
      if (conf.getBoolean("combineInputFiles", false)) {
        job.setJarByClass(CustomizedCombineHBaseRowMapper.class);
        job.setMapperClass(CustomizedCombineHBaseRowMapper.class);
        job.setInputFormatClass(CombineTextInputFormat.class);
      } else {
        job.setJarByClass(CustomizedHBaseRowMapper.class);
        job.setMapperClass(CustomizedHBaseRowMapper.class);
        job.setInputFormatClass(TextInputFormat.class);
      }
    }

    String hfileOutPath = conf.get("hbaseGeneratedHfilesOutputPath");
    if (hfileOutPath != null) {
      HTable table = new HTable(conf, tableName);
      Path outputDir = new Path(hfileOutPath);
      FileOutputFormat.setOutputPath(job, outputDir);

      job.setReducerClass(PutSortReducer.class);
      job.setMapOutputKeyClass(ImmutableBytesWritable.class);
      job.setMapOutputValueClass(Put.class);
      HFileOutputFormat2.configureIncrementalLoad(job, table);
    } else {
      // No reducers. Just write straight to table. Call
      // initTableReducerJob
      // to set up the TableOutputFormat.
      TableMapReduceUtil.initTableReducerJob(tableName, null, job);
      job.setNumReduceTasks(0);
    }

    TableMapReduceUtil.addDependencyJars(job);
    // TableMapReduceUtil.addDependencyJars(job.getConfiguration(),
    // com.google.common.base.Function.class);
    addDependencyValidatorClass(conf, job);
    return job;
  }

  private static void addDependencyValidatorClass(Configuration conf, Job job) {
    // handle validator class
    String validatorClassName = conf.get("validatorClass");
    if (validatorClassName != null) {
      try {
        Class<?> validatorClass = Class.forName(validatorClassName);
        LOGGER.info("Load " + validatorClassName);
        TableMapReduceUtil.addDependencyJars(job.getConfiguration(),
            validatorClass);
        LOGGER.info("Add dependency Jar By class " + validatorClass.getName());
      } catch (Exception e) {
        LOGGER.error("Error in setting up bulkload's validator", e);
        throw new RuntimeException(e);
      }
    }
  }

}
