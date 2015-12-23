package com.cloudera.bigdata.analysis.dataload.util;

import java.io.BufferedReader;
import java.io.BufferedWriter;
import java.io.File;
import java.io.FileWriter;
import java.io.IOException;
import java.io.InputStream;
import java.io.InputStreamReader;
import java.util.ArrayList;

import org.apache.commons.lang.StringUtils;
import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.mapred.JobClient;
import org.apache.hadoop.mapred.JobConf;
import org.apache.log4j.Logger;

import com.cloudera.bigdata.analysis.dataload.Constants;
import com.cloudera.bigdata.analysis.dataload.exception.ETLException;

public class RecordCountHiveShellUtils {
  private final static Logger LOGGER = Logger
      .getLogger(RecordCountHiveShellUtils.class);

  // generate hive script from conf properties file. The hive script will create
  // external table in hive(.hql file) to generate the record count file
  public static void generateRecordCountHiveShell(Configuration conf)
      throws Exception {
    // generate the hive table definition
    String projectHome = System.getProperty("user.dir");
    File generatedRecordCountHiveDir = new File(projectHome, "/tmpbl");
    File generatedRecordCountHiveShellFile = new File(projectHome,
        "/tmpbl/gen_record_count.hql");
    LOGGER.info("generatedRecordCountHiveShellFile:"
        + generatedRecordCountHiveShellFile);
    try {
      if (generatedRecordCountHiveDir.exists()) {
        LOGGER.warn("Dir: " + generatedRecordCountHiveDir.getCanonicalPath()
            + " is already existed.");
        generatedRecordCountHiveDir.delete();
        LOGGER.info("Delete old dir: "
            + generatedRecordCountHiveDir.getCanonicalPath());
      }
      generatedRecordCountHiveDir.mkdirs();
      if (generatedRecordCountHiveShellFile.exists()) {
        LOGGER.warn("File: "
            + generatedRecordCountHiveShellFile.getCanonicalPath()
            + " is already existed.");
        generatedRecordCountHiveShellFile.delete();
        LOGGER.info("Delete old file: "
            + generatedRecordCountHiveShellFile.getCanonicalPath());
      }
      if (generatedRecordCountHiveShellFile.createNewFile()) {
        LOGGER.info("Empty file: "
            + generatedRecordCountHiveShellFile.getCanonicalPath()
            + " is created successfully.");
      } else {
        ETLException.handle("Failed to create file: "
            + generatedRecordCountHiveShellFile.getCanonicalPath());
      }

      BufferedWriter out = new BufferedWriter(new FileWriter(
          generatedRecordCountHiveShellFile.getCanonicalPath()));
      // TODO: need to use upper hive version
      out.write("add jar /opt/cloudera/parcels/CDH/lib/hive/lib/hive-contrib.jar;");
      out.newLine();
      out.write("SET hive.exec.reducers.max=1;");
      out.newLine();
      out.write("SET hive.exec.compress.output=false;");
      out.newLine();
      // native task enable or not
      if (conf.getBoolean("nativeTaskEnabled", true)) {
        out.write("SET mapreduce.map.output.collector.delegator.class= org.apache.hadoop.mapred.nativetask.NativeMapOutputCollectorDelegator;");
        out.newLine();
      }

      // get the hbase target table name for hive external table name
      out.write("create external table "
          + conf.get("hbaseTargetTableName", "bl_test") + " (");
      out.newLine();

      // fields
      String[] fields = null;
      String fieldDelimiter = conf.get("hdfsSourceFileRecordFieldsDelimiter");
      String textRecordSpec = conf.get("textRecordSpec");

      // handle '\t' delimiter, if there is a '\t' in file delimiter, will
      // construct escaped delimiter string which appends with "\\", because
      // split() needs to handle escaped character
      if (fieldDelimiter.contains("\\t") || fieldDelimiter.contains("\\n")
          || fieldDelimiter.contains("\\b") || fieldDelimiter.contains("\\f")
          || fieldDelimiter.contains("\\r")) {
        fields = textRecordSpec.split(ExpressionUtils
            .getEscapedDelimiter(fieldDelimiter));
      } else {
        // splitByWholeSeparatorPreserveAllTokens() can not handling '\t', but
        // it is good at handling other escaped delimiter and null field split
        fields = StringUtils.splitByWholeSeparatorPreserveAllTokens(
            textRecordSpec, fieldDelimiter);
      }

      for (int i = 0; i < fields.length - 1; i++) {
        String[] ss = StringUtils.splitByWholeSeparatorPreserveAllTokens(
            fields[i], Constants.DEFAULT_FIELD_NAME_TYPE_VALUE_DELIMITER);
        out.write(ss[0] + " string,");
        out.newLine();
      }

      // the source file line spec shema is like [f1:STRING, f2:STRING, f3:INT]
      String[] ss = StringUtils.splitByWholeSeparatorPreserveAllTokens(
          fields[fields.length - 1],
          Constants.DEFAULT_FIELD_NAME_TYPE_VALUE_DELIMITER);
      out.write(ss[0] + " string");
      out.newLine();
      out.write(")");
      out.newLine();

      // TODO: hive-0.12.0 seems do not have
      // "org.apache.hadoop.hive.contrib.serde2.MultiDelimitSerDe"
      // instead, can use "ROW FORMAT DELIMITED FIELDS TERMINATED BY ","

      // out.write("ROW FORMAT SERDE");
      // out.newLine();
      // out.write("\'org.apache.hadoop.hive.contrib.serde2.MultiDelimitSerDe\'");
      // out.newLine();
      // // get the input file delimiter
      // out.write("WITH SERDEPROPERTIES (\'input.delimited\'=\'" +
      // fieldDelimiter
      // + "\')");

      out.write("ROW FORMAT DELIMITED FIELDS TERMINATED BY \"" + fieldDelimiter
          + "\"");

      out.newLine();

      // get the input file location
      out.write("location \'" + conf.get("hdfsSourceFileInputPath") + "\';");
      out.newLine();
      out.newLine();

      // generate the count SQL
      out.write("INSERT OVERWRITE LOCAL DIRECTORY \'tmpbl/count\'");
      out.newLine();
      out.write("     select c1 from");
      out.newLine();
      // get the split RowKey SubString
      String rowkeyPrefix = conf.get("rowkeyPrefix");
      out.write("     (select concat(" + rowkeyPrefix
          + ", \',\', count(*)) as c1, " + rowkeyPrefix + " as c2 from "
          + conf.get("hbaseTargetTableName"));
      out.newLine();
      out.write("     group by " + rowkeyPrefix + " order by c2) t;");
      out.newLine();

      out.close();

      // set the conf
      conf.set("generatedRecordCountHiveShell",
          generatedRecordCountHiveShellFile.getCanonicalPath());

      LOGGER.info("File: "
          + generatedRecordCountHiveShellFile.getCanonicalPath()
          + " is generated successfully.");
    } catch (IOException e) {
      e.printStackTrace();
    }
  }

  // get the record count file absolute path which is generated from running
  // hive script
  public static String getRecordCountFile(Configuration conf) throws Exception {
    // generate hive script and set conf with "generatedRecordCountHiveShell"
    generateRecordCountHiveShell(conf);

    String generateRecordCountHiveShell = conf
        .get("generatedRecordCountHiveShell");

    String recordCountFile = System.getProperty("user.dir")
        + "/tmpbl/count/000000_0";
    if (recordCountFile == null || recordCountFile.isEmpty()) {
      ETLException.handle("Failed to run pre creating regions, because "
          + recordCountFile + " is null or empty.");
    }
    File recordCountFilePath = new File(recordCountFile);
    // run the hive script to generate record count file
    if (generateRecordCountHiveShell == null) {
      throw new Exception("Failed to get generatedRecordCountHiveShell path: "
          + generateRecordCountHiveShell + " in local FS.");
    } else {
      try {
        String line = null;
        BufferedReader br = null;

        // execute hive shell to count records
        LOGGER.info("Executing Hive Script:\n" + "/usr/bin/hive" + " -f "
            + generateRecordCountHiveShell);
        Process prs = Runtime.getRuntime()
            .exec(
                new String[] { "/usr/bin/hive", "-f",
                    generateRecordCountHiveShell });

        InputStream es = prs.getErrorStream();
        br = new BufferedReader(new InputStreamReader(es));
        while ((line = br.readLine()) != null) {
          LOGGER.info(line);
          if (line.contains("FAILED: Error in semantic analysis")
              || line.contains("FAILED: Execution Error")) {
            LOGGER.error(line);
            throw new RuntimeException(line);
          }
        }

        br.close();

        // get record count File absolute path
        if (recordCountFilePath.exists()) {
          LOGGER.info("Dir: " + recordCountFile
              + " is created successfully in local FS.");
          // set the conf
          conf.set("recordCountFile", recordCountFile);
        } else {
          ETLException.handle("File: " + recordCountFile + " is not existed.");
        }
      } catch (IOException e) {
        e.printStackTrace();
      }
    }
    return recordCountFile;
  }

  public static String getTotalRecordNum(Configuration conf) throws Exception {
    String totalRecordNum = null;
    // generate hive script and set conf with "generatedRecordCountHiveShell"
    generateTotalHiveShell(conf);

    String generateTotalHiveShell = conf.get("generateTotalHiveShell");

    String totalCountFile = System.getProperty("user.dir")
        + "/tmpbl_total/000000_0";
    if (totalCountFile == null || totalCountFile.isEmpty()) {
      ETLException.handle("Failed to run pre creating regions, because "
          + totalCountFile + " is null or empty.");
    }
    File totalCountFilePath = new File(totalCountFile);
    // run the hive script to generate total count file
    if (generateTotalHiveShell == null) {
      throw new Exception("Failed to get generateTotalHiveShell path: "
          + generateTotalHiveShell + " in local FS.");
    } else {
      try {
        String line = null;
        BufferedReader br = null;

        // execute hive shell to count total records
        LOGGER.info("Executing Hive Script:\n" + "/usr/bin/hive" + " -f "
            + generateTotalHiveShell);
        Process prs = Runtime.getRuntime().exec(
            new String[] { "/usr/bin/hive", "-f", generateTotalHiveShell });

        InputStream es = prs.getErrorStream();
        br = new BufferedReader(new InputStreamReader(es));
        while ((line = br.readLine()) != null) {
          LOGGER.info(line);
          if (line.contains("FAILED: Error in semantic analysis")
              || line.contains("FAILED: Execution Error")) {
            LOGGER.error(line);
            throw new RuntimeException(line);
          }
        }

        br.close();

        // get record count File absolute path
        if (totalCountFilePath.exists()) {
          LOGGER.info("Dir: " + totalCountFile
              + " is created successfully in local FS.");
          // set the conf
          conf.set("totalCountFile", totalCountFile);
        } else {
          ETLException.handle("File: " + totalCountFile + " is not existed.");
        }
      } catch (IOException e) {
        e.printStackTrace();
      }
    }

    // get the value
    try {
      String line = null;
      BufferedReader br = null;

      // execute hive shell to count total records
      LOGGER.info("cat " + totalCountFile);
      Process prs = Runtime.getRuntime().exec(
          new String[] { "cat", totalCountFile });

      InputStream is = prs.getInputStream();
      br = new BufferedReader(new InputStreamReader(is));
      // in fact it only fetch the first line
      if ((line = br.readLine()) != null) {
        totalRecordNum = line.trim();
      }
      // set the conf
      if (totalRecordNum == null || totalRecordNum.isEmpty()) {
        ETLException.handle("totalRecordNum is NULL");
      } else {
        LOGGER.info("totalRecordNum: " + totalRecordNum);
        conf.set("totalRecordNum", totalRecordNum);
      }
      br.close();
    } catch (IOException e) {
      e.printStackTrace();
    }

    return totalRecordNum;
  }

  public static void generateTotalHiveShell(Configuration conf) {
    // generate the hive table definition
    String projectHome = System.getProperty("user.dir");
    File generatedTotalHiveDir = new File(projectHome, "/tmpbl_total");
    File generatedTotalHiveShellFile = new File(projectHome,
        "/tmpbl/gen_total_count.hql");
    LOGGER.info("generatedTotalHiveShellFile:" + generatedTotalHiveShellFile);
    File recordCountFile = new File(conf.get("recordCountFile"));
    try {

      if (!recordCountFile.exists()) {
        ETLException.handle("Failed to count total record number, because : "
            + recordCountFile.getCanonicalPath() + " doesn't exist");
      }

      if (generatedTotalHiveDir.exists()) {
        LOGGER.warn("File: " + generatedTotalHiveDir.getCanonicalPath()
            + " is already existed.");
        generatedTotalHiveDir.delete();
        LOGGER.info("Delete old file: "
            + generatedTotalHiveDir.getCanonicalPath());
      }
      generatedTotalHiveDir.mkdirs();
      if (generatedTotalHiveShellFile.exists()) {
        LOGGER.warn("File: " + generatedTotalHiveShellFile.getCanonicalPath()
            + " is already existed.");
        generatedTotalHiveShellFile.delete();
        LOGGER.info("Delete old file: "
            + generatedTotalHiveShellFile.getCanonicalPath());
      }
      if (generatedTotalHiveShellFile.createNewFile()) {
        LOGGER.info("Empty file: "
            + generatedTotalHiveShellFile.getCanonicalPath()
            + " is created successfully.");
      } else {
        ETLException.handle("Failed to create file: "
            + generatedTotalHiveShellFile.getCanonicalPath());
      }

      BufferedWriter output = new BufferedWriter(new FileWriter(
          generatedTotalHiveShellFile.getCanonicalPath()));
      // TODO: need to use upper hive version
      output
          .write("add jar /opt/cloudera/parcels/CDH/lib/hive/lib/hive-contrib.jar;");
      output.newLine();
      output.write("SET hive.exec.reducers.max=1;");
      output.newLine();
      output.write("SET hive.exec.compress.output=false;");
      output.newLine();
      // native task enable or not
      if (conf.getBoolean("nativeTaskEnabled", true)) {
        output
            .write("SET mapreduce.map.output.collector.delegator.class= org.apache.hadoop.mapred.nativetask.NativeMapOutputCollectorDelegator;");
        output.newLine();
      }
      String totalTableName = conf.get("hbaseTargetTableName", "bl_test")
          + "_total";

      // get the hbase target table name for hive table name
      output.write("create table " + totalTableName + " (");
      output.newLine();

      output.write("rowkeyprefix string,");
      output.newLine();

      output.write("num int");
      output.newLine();

      output.write(")");
      output.newLine();

      output.write("ROW FORMAT DELIMITED FIELDS TERMINATED BY \',\'");
      output.newLine();

      output.write("STORED AS TEXTFILE;");
      output.newLine();
      output.newLine();

      output.write("load data local inpath " + "\'"
          + recordCountFile.getCanonicalPath() + "\' into table "
          + totalTableName + ";");
      output.newLine();
      output.newLine();

      // generate the count SQL
      output.write("INSERT OVERWRITE LOCAL DIRECTORY \'tmpbl_total/\'");
      output.newLine();
      output.write("     select sum(num) from " + totalTableName + ";");
      output.newLine();

      output.close();

      // set the conf
      conf.set("generateTotalHiveShell",
          generatedTotalHiveShellFile.getCanonicalPath());

      LOGGER.info("File: " + generatedTotalHiveShellFile.getCanonicalPath()
          + " is generated successfully.");
    } catch (IOException e) {
      e.printStackTrace();
    }
  }

  // calculate the record number properly according to total record and core
  // number
  public static String calculateRecordNumPerRegion(Configuration conf) {
    String recordsNumPerRegionString = null;
    // get total record number
    long totalRecordNum = Long.parseLong(conf.get("totalRecordNum"));

    // read regionservers and get the core number of each RS
    long regionNumByRS = 0;
    long regionNumByGlobalFlush = 0;
    long minTotalRegionNum = 0;
    long maxReduceTaskCapacity = 0;

    try {
      // parse the alive regionservers list
      String cdhHBaseMasterIpaddress = conf.get("cdhHBaseMasterIpaddress");
      Process prs = Runtime.getRuntime()
          .exec(
              new String[] {
                  "/bin/sh",
                  "-c",
                  "ssh " + cdhHBaseMasterIpaddress
                      + " hbase zkcli get /hbase/rs" });

      String line = null;
      BufferedReader br = null;

      // handling error
      InputStream es = prs.getErrorStream();
      br = new BufferedReader(new InputStreamReader(es));
      while ((line = br.readLine()) != null) {
        LOGGER.info(line);
        if (line.contains("FAILED: Error in semantic analysis")
            || line.contains("FAILED: Execution Error")) {
          LOGGER.error(line);
          throw new RuntimeException(line);
        }
      }

      // get alive regionservers
      InputStream is = prs.getInputStream();
      br = new BufferedReader(new InputStreamReader(is));
      // in fact it only fetch the first line
      ArrayList<String> rsList = new ArrayList<String>();
      String rsHost = null;

      if ((line = br.readLine()) != null) {
        String[] tmp = line.split(":");
        if (tmp.length > 2) {
          String[] rs = line.split(",");
          for (int i = 0; i < rs.length; i++) {
            if (i == 0) {
              rsHost = rs[0].substring("Connecting to ".length(),
                  line.indexOf(":"));
              rsList.add(rsHost);
            } else {
              rsHost = rs[i].substring(0, rs[i].indexOf(":"));
              rsList.add(rsHost);
            }
          }
        } else if (tmp.length == 2) {
          String rsHostsStr = line.substring("Connecting to ".length(),
              line.indexOf(":"));
          String[] rs2 = rsHostsStr.split(",");
          for (int i = 0; i < rs2.length; i++) {
            rsList.add(rs2[i]);
          }
        } else {
          // incorrect logic
        }
      }

      br.close();

      // get the core number from every RS
      for (String rsHostName : rsList) {
        regionNumByRS = getCoreNumPerRS(cdhHBaseMasterIpaddress, rsHostName) * 4;
        // calculate the max region number according to memory size and hbase
        // flush size(64MB)
        regionNumByGlobalFlush = (long) (getMemNumPerRS(
            cdhHBaseMasterIpaddress, rsHostName) * 0.5 * 0.35 / (64 * 1024));
        // get the minimal total region number of hbase cluster
        minTotalRegionNum += Math.min(regionNumByRS, regionNumByGlobalFlush);
      }

      // get the maximal reduce task capacity number
      maxReduceTaskCapacity = getMaxReduceTaskCapacity(conf);

      // calculate the record number per region
      long recordsNumPerRegion = 0;
      long rsNum = rsList.size();
      if (maxReduceTaskCapacity < minTotalRegionNum) {
        minTotalRegionNum = maxReduceTaskCapacity;
      }
      minTotalRegionNum = minTotalRegionNum + 1;
      if (rsNum < minTotalRegionNum) {
        if (totalRecordNum > minTotalRegionNum) {
          LOGGER.info("Suggested pre-assign: " + minTotalRegionNum
              + " regions all of the cluster.");
          recordsNumPerRegion = totalRecordNum / minTotalRegionNum;
        } else if (totalRecordNum > rsNum && totalRecordNum < minTotalRegionNum) {
          LOGGER
              .warn("Total record number is less than minimal suggested total regions number.");
          LOGGER.info("Suggested pre-assign: " + rsNum
              + " regions all of the cluster.");
          recordsNumPerRegion = totalRecordNum / rsNum;
        } else {
          LOGGER.warn("Total record number is less than regionservers number.");
          LOGGER.info("Suggested pre-assign: " + 1
              + " regions all of the cluster.");
          recordsNumPerRegion = 1;
        }
      } else {
        if (totalRecordNum > rsNum) {
          LOGGER
              .info("The minimal suggested total regions number is less than Regionservers number.\nSuggested pre-assign: "
                  + minTotalRegionNum + " regions all of the cluster.");
          recordsNumPerRegion = totalRecordNum / rsNum;
        } else if (totalRecordNum > minTotalRegionNum && totalRecordNum < rsNum) {
          LOGGER
              .warn("The minimal suggested total regions number is less than Regionservers number.\nTotal record number is less than regionservers number.");
          LOGGER.info("Suggested pre-assign: " + minTotalRegionNum
              + " regions all of the cluster.");
          recordsNumPerRegion = totalRecordNum / minTotalRegionNum;
        }
        LOGGER
            .warn("The minimal suggested total regions number is less than Regionservers number.\nTotal record number is less than minimal suggested total regions number.");
        LOGGER.info("Suggested pre-assign: " + 1
            + " regions all of the cluster.");
        recordsNumPerRegion = 1;
      }

      // set the conf
      conf.set("recordsNumPerRegion", String.valueOf(recordsNumPerRegion));
      LOGGER.info("recordsNumPerRegion: " + recordsNumPerRegion);
      LOGGER.info("totalRecordNum: " + totalRecordNum);
      recordsNumPerRegionString = String.valueOf(recordsNumPerRegion);
    } catch (Exception e) {
      e.printStackTrace();
    }
    return recordsNumPerRegionString;
  }

  // get the core number of each RS
  public static long getCoreNumPerRS(String cdhHBaseMasterIpaddress,
      String RegionServerHost) {
    Process prs;
    long coreNum = 0;
    try {
      prs = Runtime.getRuntime().exec(
          new String[] {
              "/bin/sh",
              "-c",
              "ssh " + cdhHBaseMasterIpaddress + " ssh " + RegionServerHost
                  + " cat /proc/cpuinfo |grep \"processor\"|wc -l" });

      String line = null;
      BufferedReader br = null;

      // handling error
      InputStream es = prs.getErrorStream();
      br = new BufferedReader(new InputStreamReader(es));
      while ((line = br.readLine()) != null) {
        LOGGER.info(line);
        if (line.contains("FAILED: Error in semantic analysis")
            || line.contains("FAILED: Execution Error")) {
          LOGGER.error(line);
          throw new RuntimeException(line);
        }
      }

      // get the core number from InputStream
      InputStream is = prs.getInputStream();
      br = new BufferedReader(new InputStreamReader(is));
      // in fact it only fetch the first line
      if ((line = br.readLine()) != null) {
        coreNum = Long.parseLong(line);
      }
      br.close();
    } catch (IOException e) {
      e.printStackTrace();
    }
    return coreNum;
  }

  // get the memory number of each RS
  public static long getMemNumPerRS(String cdhHBaseMasterIpaddress,
      String RegionServerHost) {
    Process prs;
    long MemNum = 0;
    try {
      prs = Runtime
          .getRuntime()
          .exec(
              new String[] {
                  "/bin/sh",
                  "-c",
                  "ssh "
                      + cdhHBaseMasterIpaddress
                      + " ssh "
                      + RegionServerHost
                      + " cat /proc/meminfo |grep \"MemTotal\"|awk \'$1~\"MemTotal\"{print $2}\'" });

      String line = null;
      BufferedReader br = null;

      // handling error
      InputStream es = prs.getErrorStream();
      br = new BufferedReader(new InputStreamReader(es));
      while ((line = br.readLine()) != null) {
        LOGGER.info(line);
        if (line.contains("FAILED: Error in semantic analysis")
            || line.contains("FAILED: Execution Error")) {
          LOGGER.error(line);
          throw new RuntimeException(line);
        }
      }

      // get the core number from InputStream
      InputStream is = prs.getInputStream();
      br = new BufferedReader(new InputStreamReader(is));
      // in fact it only fetch the first line
      if ((line = br.readLine()) != null) {
        MemNum = Long.parseLong(line);
      }
      br.close();
    } catch (IOException e) {
      e.printStackTrace();
    }
    return MemNum;
  }

  public static int getMaxReduceTaskCapacity(Configuration conf) {
    JobConf jobConf = new JobConf(conf);
    int maxReduceTaskCapacity = 1;
    JobClient jc;
    try {
      jc = new JobClient(jobConf);
      maxReduceTaskCapacity = jc.getClusterStatus().getMaxReduceTasks();
      LOGGER.info("the max reduce slot: " + maxReduceTaskCapacity);
    } catch (IOException e) {
      // TODO Auto-generated catch block
      e.printStackTrace();
    }
    return maxReduceTaskCapacity;
  }
}
