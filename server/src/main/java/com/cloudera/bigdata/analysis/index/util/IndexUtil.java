package com.cloudera.bigdata.analysis.index.util;

import java.io.BufferedInputStream;
import java.io.FileNotFoundException;
import java.io.IOException;
import java.util.ArrayList;
import java.util.HashSet;
import java.util.Properties;
import java.util.Set;

import javax.xml.parsers.DocumentBuilder;
import javax.xml.parsers.DocumentBuilderFactory;
import javax.xml.parsers.ParserConfigurationException;

import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.fs.FSDataInputStream;
import org.apache.hadoop.fs.FileStatus;
import org.apache.hadoop.fs.FileSystem;
import org.apache.hadoop.fs.Path;
import org.apache.hadoop.hbase.HBaseConfiguration;
import org.apache.hadoop.hbase.util.Bytes;
import org.apache.hadoop.hdfs.util.ByteArray;
import org.apache.log4j.Logger;
import org.w3c.dom.Document;
import org.w3c.dom.Element;
import org.w3c.dom.NodeList;
import org.xml.sax.SAXException;

import com.cloudera.bigdata.analysis.exception.ETLException;
import com.cloudera.bigdata.analysis.index.Constants;

public class IndexUtil {

  public final static Logger LOGGER = Logger.getLogger(IndexUtil.class);
  private static Configuration conf;

  private static FileSystem fs;

  public static String getCurrentTableName(String currentPro) {
    Properties props = new Properties();
    try {
      conf = new Configuration();
      conf.addResource(new Path(Constants.CORE_SITE_XML));

      fs = FileSystem.get(conf);
      Path currentProPath = new Path(currentPro);
      if (!fs.exists(currentProPath)) {
        LOGGER.warn("File " + currentPro + " does not exists in HDFS");
      }
      FSDataInputStream currentProStream = fs.open(currentProPath);

      props.load(currentProStream);
    } catch (FileNotFoundException e) {
      LOGGER.error("", e);
    } catch (IOException e) {
      LOGGER.error("", e);
    }
    
    if(props.getProperty(Constants.BUILD_INDEX).equals("false")){
    	return "build_no_index";
    }else{
    return props.getProperty(Constants.HBASE_TARGET_TABLE_NAME);
    }
  }

  public static String getCurrentIndexConfNameForLoad(String currentPro) {
    Properties props = new Properties();
    try {
      conf = HBaseConfiguration.create();
      conf.addResource(new Path(Constants.CORE_SITE_XML));

      fs = FileSystem.get(conf);
      Path currentProPath = new Path(currentPro);
      if (!fs.exists(currentProPath)) {
        LOGGER.warn("File " + currentPro + " does not exists in HDFS");
      }
      FSDataInputStream currentProStream = fs.open(currentProPath);

      props.load(currentProStream);
    } catch (FileNotFoundException e) {
      LOGGER.error("", e);
    } catch (IOException e) {
      LOGGER.error("", e);
    }
    return props.getProperty(Constants.INDEX_CONF_FILE_NAME);
  }

  public static String getCurrentIndexConfNameForQuery(String queryTable,
      Path indexConfDirHdfs) {
    if(queryTable.equals("build_no_index")){
    	return "build_no_index";
    }  
	  
    String fn = null;
    boolean existed = false;
    conf = HBaseConfiguration.create();
    conf.addResource(new Path(Constants.CORE_SITE_XML));
    try {
      fs = FileSystem.get(conf);
      FileStatus files[] = fs.listStatus(indexConfDirHdfs);
      if (files == null || files.length == 0) {
        ETLException
            .handle("The dir in HDFS " + indexConfDirHdfs + " is empty");
      }
      for (FileStatus file : files) {
        if (!file.isDir()) {
          Path path = file.getPath();
          String p = path.getName();
          fn = p.substring(p.lastIndexOf("/") + 1);
          if (fn.equals(queryTable + "_index-conf.xml")) {
            existed = true;
            break;
          }
        }
      }
    } catch (IOException e) {
      // TODO Auto-generated catch block
      e.printStackTrace();
    }
    if (existed) {
      return fn;
    } else {
      ETLException.handle("There is no matched " + queryTable
          + "_index-conf.xml under " + indexConfDirHdfs
          + " in HDFS for query table [" + queryTable + "]");
      return null;
    }
  }

  public static boolean isIndexConfAvailableForQuery(String queryTable) {
    // check hbase.target.table.name and index-conf.xml file name in
    // current.properties
    String indexConfName = getCurrentIndexConfNameForQuery(queryTable,
        new Path(Constants.INDEX_CONF_DIR_HDFS));
    if (!indexConfName.equals(queryTable + "_index-conf.xml")) {
      ETLException.handle("The index configuration file name under "
          + Constants.CURRENT_PRO_HDFS + " doesn't match the value of "
          + queryTable + " when query");
    } else {
      // check table elements consistency index-conf.xml file
      if ((getIndexTablesName(Constants.INDEX_CONF_DIR_HDFS + "/"
          + indexConfName).size()) != 1) {
        ETLException.handle("The table elements are not consistent in "
            + Constants.INDEX_CONF_DIR_HDFS
            + "/"
            + indexConfName
            + " , the size of tables names is "
            + getIndexTablesName(
                Constants.INDEX_CONF_DIR_HDFS + "/" + indexConfName).size());
      } else {
        if (getIndexTablesName(
            Constants.INDEX_CONF_DIR_HDFS + "/" + indexConfName).contains(
            queryTable)) {
          return true;
        } else {
          ETLException.handle("There is no search table [" + queryTable
              + "] in " + Constants.INDEX_CONF_DIR_HDFS + "/" + indexConfName
              + " or there is index table configuration error in "
              + Constants.INDEX_CONF_DIR_HDFS + "/" + indexConfName);
        }
      }
    }
    // check current pro
    return false;
  }

  public static boolean isIndexConfAvailableForLoad(String tableName) {
    // check hbase.target.table.name and index-conf.xml file name in
    // current.properties
    // ensure running current.properties is the same with it in HDFS
    if (tableName.equals(Constants.CURRENT_TABLE)
        && (tableName + "_index-conf.xml")
            .equals(Constants.CURRENT_INDEX_CONF_FILE_NAME_FOR_LOAD)) {
      // ensure the indexConfFileName is matched with indexConfFileName in HDFS
      if (!Constants.CURRENT_INDEX_CONF_FILE_NAME_FOR_LOAD
          .equals(Constants.CURRENT_TABLE + "_index-conf.xml")) {
        ETLException.handle("The value of " + Constants.INDEX_CONF_FILE_NAME
            + " doesn't match the value of "
            + Constants.HBASE_TARGET_TABLE_NAME + " in "
            + Constants.CURRENT_PRO_HDFS + " when load");
      } else {
        // check table elements consistency index-conf.xml file
        if ((getIndexTablesName(Constants.INDEX_CONF_DIR_HDFS + "/"
            + Constants.CURRENT_INDEX_CONF_FILE_NAME_FOR_LOAD).size()) != 1) {
          ETLException
              .handle("The table elements are not consistent in "
                  + Constants.INDEX_CONF_DIR_HDFS
                  + "/"
                  + Constants.CURRENT_INDEX_CONF_FILE_NAME_FOR_LOAD
                  + " , the size of tables names is "
                  + getIndexTablesName(
                      Constants.INDEX_CONF_DIR_HDFS + "/"
                          + Constants.CURRENT_INDEX_CONF_FILE_NAME_FOR_LOAD)
                      .size());
        } else {
          if (getIndexTablesName(
              Constants.INDEX_CONF_DIR_HDFS + "/"
                  + Constants.CURRENT_INDEX_CONF_FILE_NAME_FOR_LOAD).contains(
              Constants.CURRENT_TABLE)) {
            return true;
          }
        }
      }
    } else {
      ETLException
          .handle("It is not consistent in current.properties between local and HDFS ");
    }
    return false;
  }

  public static Set<String> getIndexTablesName(String xmlFilePath) {
    DocumentBuilderFactory dbf = DocumentBuilderFactory.newInstance();
    Set<String> tables = new HashSet<String>();
    Element index = null;
    NodeList name = null;
    String tableName = null;
    try {
      DocumentBuilder builder = dbf.newDocumentBuilder();
      conf = new Configuration();
      conf.addResource(new Path(Constants.CORE_SITE_XML));
      fs = FileSystem.get(conf);
      Path xmlPath = new Path(xmlFilePath);
      if (!fs.exists(xmlPath)) {
        LOGGER.warn("Failed to get Table Name from index conf, because file : "
            + xmlFilePath + " does not exists in HDFS");
      }
      FSDataInputStream inXml = fs.open(xmlPath);
      Document doc = builder.parse(new BufferedInputStream(inXml));
      Element root = doc.getDocumentElement();
      NodeList indexs = root.getElementsByTagName("index");
      for (int i = 0; i < indexs.getLength(); i++) {
        index = (Element) indexs.item(i);
        name = index.getElementsByTagName("table");
        tableName = ((Element) name.item(0)).getFirstChild().getNodeValue();
        if (tableName != null && !tables.contains(tableName)) {
          tables.add(tableName);
        }
      }
    } catch (SAXException e) {
      // TODO Auto-generated catch block
      e.printStackTrace();
    } catch (IOException e) {
      // TODO Auto-generated catch block
      e.printStackTrace();
    } catch (ParserConfigurationException e) {
      // TODO Auto-generated catch block
      e.printStackTrace();
    }

    // tablesNames list all tables' name
    return tables;
  }

  public static byte[][] getSplitKeys(String splitKeyStr) {
    String[] keys = splitKeyStr.split(",");
    byte[][] results = new byte[keys.length][];
    for (int i = 0; i < keys.length; i++) {
      String s = keys[i];
      results[i] = Bytes.toBytes(s);
    }

    return results;
  }

  /**
   * region splits must balance! it's not good enough to just divide region
   * count. for example: 11 prefixes, 3 regions, each region has 3 prefixes, but
   * last region will take 5 prefixes, it's not balance, we should re-balance
   * the rest 2 prefixes.
   */
  public static byte[][] calcSplitKeys(int regionQuantity) {
    int avgPrefixCount = Constants.ROWKEY_PREFIX_MAX_VALUE / regionQuantity;
    int residualPrefixCount = Constants.ROWKEY_PREFIX_MAX_VALUE
        % regionQuantity;
    byte[][] splitKeys = new byte[regionQuantity - 1][];
    for (int i = 1; i <= regionQuantity - 1; i++) {
      if (i <= residualPrefixCount) {
        splitKeys[i - 1] = Bytes.toBytes(RowKeyUtil.formatToRowKeyPrefix(i
            * avgPrefixCount + i));
      } else {
        splitKeys[i - 1] = Bytes.toBytes(RowKeyUtil.formatToRowKeyPrefix(i
            * avgPrefixCount + residualPrefixCount));
      }
    }
    return splitKeys;
  }

  public static ArrayList<String> generateSplitKey(int regionQuantity) {
    // generate split key spec for quick index build
    // split according to the regionQuantity
    ArrayList<String> regionStartKeys = new ArrayList<String>();
    byte[][] splitKeys = calcSplitKeys(regionQuantity);
    regionStartKeys.add("0000");
    for (int i = 0; i < splitKeys.length; i++) {
      regionStartKeys.add(convertByteArrayToString(splitKeys[i]));
    }
    return regionStartKeys;
  }

  public static String convertByteArrayToString(byte[] rowkey) {
    String strRead = new String(rowkey);
    strRead = String.copyValueOf(strRead.toCharArray(), 0, rowkey.length);
    return strRead;
  }

  public static ByteArray convertStringToByteArray(String str) {
    byte[] StrBytes = Bytes.toBytes(str);
    return new ByteArray(StrBytes);
  }

}
