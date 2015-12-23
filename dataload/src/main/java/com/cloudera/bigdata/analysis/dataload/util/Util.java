package com.cloudera.bigdata.analysis.dataload.util;

import java.io.File;
import java.io.FileNotFoundException;
import java.io.IOException;
import java.net.InetAddress;
import java.net.URI;
import java.net.UnknownHostException;
import java.util.Arrays;
import java.util.Date;
import java.util.Iterator;
import java.util.List;
import java.util.Map.Entry;
import java.util.Properties;
import java.util.Random;
import java.util.Set;

import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.fs.FileSystem;
import org.apache.hadoop.fs.Path;
import org.apache.hadoop.hbase.util.Bytes;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import com.cloudera.bigdata.analysis.dataload.Constants;
import com.cloudera.bigdata.analysis.dataload.exception.ParseException;
import com.cloudera.bigdata.analysis.dataload.exception.TableDisabledException;
import com.cloudera.bigdata.analysis.dataload.io.FileObject;
import com.cloudera.bigdata.analysis.dataload.source.DataSource;
import com.cloudera.bigdata.analysis.dataload.source.FileParser;
import com.cloudera.bigdata.analysis.dataload.transform.ExtensibleHBaseRowConverter;

public class Util {
  private static final Logger LOG = LoggerFactory.getLogger(Util.class);

  private static Random random = new Random(new Date().getTime());

  private static ThreadLocal<Random> localRand = new ThreadLocal<Random>();

  public static Random getRandom() {
    Random tRand = localRand.get();
    if (tRand == null) {
      tRand = new Random(random.nextLong());
      localRand.set(tRand);
    }
    return tRand;
  }

  /**
   * Create an instance of DataSource
   * 
   * @throws IllegalAccessException
   * @throws InstantiationException
   * @throws ClassNotFoundException
   * @throws IOException
   */
  public static DataSource newDataSource(Configuration conf)
      throws InstantiationException, IllegalAccessException,
      ClassNotFoundException, IOException {
    String dataSourceClass = conf.get(Constants.DATASOURCE_CLASS_KEY);
    if (LOG.isDebugEnabled()) {
      LOG.info("dataSourceClass is " + dataSourceClass);
    }
    DataSource ds = DataSource.getDataSource(dataSourceClass);
    ds.init(conf);
    return ds;
  }

  /**
   * Create an instance of ExtendedHBaseRowConverter
   * 
   * @param conf
   *          configuration file
   * @return
   */
  public static ExtensibleHBaseRowConverter newExtendedHBaseRowConverter(
      Configuration conf) {
    String extendedHBaseRowConverterClass = conf
        .get(Constants.EXTENDEDHBASEROWCONVERTER_CLASS_KEY);
    ExtensibleHBaseRowConverter ec = ExtensibleHBaseRowConverter
        .getExtendedHBaseRowConverter(extendedHBaseRowConverterClass);
    return ec;

  }

  public static Class<?> forceInit(String className) {
    try {
      return Class.forName(className);
    } catch (ClassNotFoundException e) {
      throw new IllegalArgumentException("Cannot Initialize: ", e);
    }
  }

  /**
   * Create an instance of FileParser
   * 
   * @throws IllegalAccessException
   * @throws InstantiationException
   * @throws ClassNotFoundException
   */
  @SuppressWarnings("unchecked")
  public static FileParser newFileParser(Configuration conf)
      throws InstantiationException, IllegalAccessException,
      ClassNotFoundException {
    String fileParserClass = conf.get(Constants.FILEPARSER_CLASS_KEY);
    Class<? extends FileParser> fpClass = (Class<? extends FileParser>) Class
        .forName(fileParserClass);
    return fpClass.newInstance();
  }

  public static FileParser getFileParser(Configuration conf)
      throws ParseException {
    String parserTypeString = conf.get(Constants.PARSER_TYPE_KEY);
    return FileParser.getFileParser(FileParser.ParserType
        .formValue(parserTypeString));
  }

  public static String getServerIPorHostname() {
    String ip = null;
    try {
      ip = InetAddress.getLocalHost().getHostName();
    } catch (UnknownHostException e) {
      try {
        ip = InetAddress.getLocalHost().getHostAddress();
      } catch (UnknownHostException e1) {
        ip = "unknow";
      }
    }
    return ip;
  }

  public static List<FileObject> getFileList(Configuration conf)
      throws IOException {
    DataSource dataSource = null;
    try {
      dataSource = Util.newDataSource(conf);
    } catch (Exception e) {
      LOG.error("", e);
      System.exit(1);
    }

    if (dataSource != null) {
      dataSource.connect();
      List<FileObject> fileList = dataSource.getFileList();
      dataSource.close();

      return fileList;
    }
    return null;
  }

  public static List<FileObject> getFileList(DataSource dataSource)
      throws IOException {
    if (dataSource != null) {
      dataSource.connect();
      List<FileObject> fileList = dataSource.getFileList();
      dataSource.close();

      return fileList;
    }
    return null;
  }

  public static String makeQualified(String localFile, Configuration conf)
      throws IOException {
    URI pathURI = new File(localFile).toURI();
    String finalPath = null;

    Path path = new Path(pathURI);
    FileSystem localFs = FileSystem.getLocal(conf);
    if (pathURI.getScheme() == null) {
      if (!localFs.exists(path)) {
        throw new FileNotFoundException("File " + localFile
            + " does not exist.");
      }
      finalPath = path.makeQualified(localFs).toString();
    } else {
      FileSystem fs = path.getFileSystem(conf);
      if (!fs.exists(path)) {
        throw new FileNotFoundException("File " + localFile
            + " does not exist.");
      }
      // finalPath = path.makeQualified(fs).toString();
      // finalPath = path.makeQualified(fs.getUri(), fs.getWorkingDirectory())
      // .toString();
      finalPath = fs.makeQualified(path).toUri().toString();
    }
    return finalPath;
  }

  public static void mergeProperties(Properties props, Configuration conf) {
    Set<Entry<Object, Object>> entrySet = props.entrySet();

    Iterator<Entry<Object, Object>> iter = entrySet.iterator();
    while (iter.hasNext()) {
      Entry<Object, Object> entry = iter.next();
      String property = (String) entry.getKey();
      String value = (String) entry.getValue();

      conf.set(property, value);
    }
  }

  /**
   * Generate the split keys bytes array based on the prefixes and split size
   * which is configured in the conf.properties.
   * 
   * This generation method assumes the prefix string is in decimal format.
   * 
   * The split size should be a number in below format: 2*10^n, 5*10^n or 1*10^n
   * number of regions for even key space.
   * 
   * @param prefixesStr
   * @param splitSize
   * @return
   */
  public static byte[][] genSplitKeys(String prefixesStr, int splitSize)
      throws Exception {
    if (prefixesStr == null) {
      prefixesStr = "";
    }
    String[] prefixes = prefixesStr.split(",");
    int round = 1;
    int num = 1;
    if (!checkSplitSize(splitSize)) {
      throw new TableDisabledException(
          "The split size should be a number with 1*10^n, 2*10^n or 5*10^n.");
    }
    if (prefixes == null || prefixes.length == 0) {
      prefixes = new String[1];
      prefixes[0] = "";
    }
    prefixes = trimStrings(prefixes);

    while ((round *= 10) < splitSize) {
      num++;
    }
    float step = (float) round / (float) splitSize;
    byte[][] splitKeys = new byte[splitSize * prefixes.length][];

    char[] digits = new char[num];
    String[] suffixes = new String[splitSize];
    for (int i = 0; i < splitSize; i++) {
      int digit = (int) (step * i);
      String strd = Integer.toString(digit);
      int index = 0;
      while (index < num - strd.length()) {
        digits[index] = '0';
        index++;
      }
      while (index < num) {
        digits[index] = strd.charAt(index - num + strd.length());
        index++;
      }
      String suffix = new String(digits);
      suffixes[i] = suffix;
    }

    Arrays.sort(prefixes);
    StringBuilder sb = new StringBuilder();
    for (int i = 0; i < prefixes.length; i++) {
      for (int j = 0; j < suffixes.length; j++) {
        splitKeys[i * suffixes.length + j] = Bytes.toBytes(prefixes[i]
            + suffixes[j]);
        sb.append(prefixes[i] + suffixes[j] + "|");
      }
    }
    LOG.info("SplitKey: " + sb);
    return splitKeys;
  }

  private static String[] trimStrings(String[] orignal) {
    String[] result = new String[orignal.length];

    for (int i = 0; i < orignal.length; i++) {
      result[i] = orignal[i].trim();
    }

    return result;
  }

  private static boolean checkSplitSize(int splitSize) {
    if (splitSize < 1) {
      return false;
    }

    while (true) {
      if (splitSize < 10) {
        if (splitSize == 1 || splitSize == 2 || splitSize == 5) {
          return true;
        } else {
          return false;
        }
      } else {
        if (splitSize % 10 != 0) {
          return false;
        }
        splitSize /= 10;
      }
    }
  }

  public static void main(String[] args) throws Exception {
    genSplitKeys("", 15);
  }
}
