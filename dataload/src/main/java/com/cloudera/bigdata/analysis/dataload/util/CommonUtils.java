package com.cloudera.bigdata.analysis.dataload.util;

import java.io.IOException;
import java.io.UnsupportedEncodingException;
import java.util.ArrayList;
import java.util.HashMap;
import java.util.List;
import java.util.Map;

import org.apache.commons.lang.StringUtils;
import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.fs.Path;
import org.apache.hadoop.hbase.HColumnDescriptor;
import org.apache.hadoop.hbase.HTableDescriptor;
import org.apache.hadoop.hbase.TableName;
import org.apache.hadoop.hbase.client.HBaseAdmin;
import org.apache.hadoop.hbase.client.HTable;
import org.apache.hadoop.hbase.io.compress.Compression.Algorithm;
import org.apache.hadoop.hbase.mapreduce.LoadIncrementalHFiles;
import org.apache.hadoop.hbase.regionserver.BloomType;
import org.apache.hadoop.hbase.regionserver.ConstantSizeRegionSplitPolicy;
import org.apache.hadoop.hbase.util.Bytes;
import org.apache.hadoop.hdfs.util.ByteArray;
import org.apache.hadoop.mapred.JobClient;
import org.apache.log4j.Logger;

import com.cloudera.bigdata.analysis.dataload.exception.ETLException;
import com.cloudera.bigdata.analysis.dataload.transform.HBaseTableSpec;
import com.cloudera.bigdata.analysis.dataload.transform.TargetTableSpec;
import com.cloudera.bigdata.analysis.dataload.transform.HBaseTableSpec.ColumnSpec;
import com.cloudera.bigdata.analysis.index.Constants;
import com.cloudera.bigdata.analysis.index.util.IndexUtil;
import com.cloudera.bigdata.analysis.index.util.RowKeyUtil;
import com.cloudera.bigdata.analysis.query.RecordServiceEndpoint;

public class CommonUtils {

  private final static Logger LOGGER = Logger.getLogger(CommonUtils.class);

  final static HashMap<Integer, byte[]> substrings = new HashMap<Integer, byte[]>();

  private static String cdhVersion = new String();

  public static String getCdhVersion() {
    return cdhVersion;
  }

  public static void setCdhVersion(String cdhVersion) {
    CommonUtils.cdhVersion = cdhVersion;
  }

  public static boolean isAtomicExpression(String columnSpec) {
    if (StringUtils.indexOf(columnSpec, Constants.OPEN_PARENTHESIS) < 0
        && StringUtils.indexOf(columnSpec, Constants.CLOSE_PARENTHESIS) < 0
        && StringUtils.indexOf(columnSpec, Constants.COMMA) < 0) {
      return true;
    }

    return false;
  }

  public static boolean isSingleQuotationMarksWrapedExpression(String columnSpec) {
    if (columnSpec.startsWith(Constants.SINGLE_QUOTATION_MARK)
        && columnSpec.endsWith(Constants.SINGLE_QUOTATION_MARK)) {
      return true;
    }

    return false;
  }

  public static String getSingleQuotationMarksWrapedExpressionValue(
      String singleQuotationMarksWrapedExpression) {
    return StringUtils.substring(singleQuotationMarksWrapedExpression, 1,
        singleQuotationMarksWrapedExpression.length() - 1);
  }

  public static String[] getExpressions(String commaSeperatedExpressions)
      throws ETLException {

    List<String> expressions = new ArrayList<String>();

    spawnExpressions(commaSeperatedExpressions, expressions);

    return expressions.toArray(new String[expressions.size()]);
  }

  // return the position of the top closing parenthesis in the first expression
  // in a common separated expressions
  public static int getTopParenthesisClosingPositionOfFirstExpression(
      String commaSeperatedExpressions) throws ETLException {

    int openPostion = StringUtils.indexOf(commaSeperatedExpressions,
        Constants.OPEN_PARENTHESIS);
    if (openPostion < 0)
      return -1; // no parenthesis

    int commaPostion = StringUtils.indexOf(commaSeperatedExpressions,
        Constants.COMMA);

    if (commaPostion < openPostion)
      return -1;// no parenthesis in the first expression

    int index = openPostion + 1;
    int netCount = 1;
    while (index < commaSeperatedExpressions.length()) {
      char c = commaSeperatedExpressions.charAt(index);
      if (c == '(') {
        netCount++;
      } else if (c == ')') {
        netCount--;
        if (netCount == 0)
          return index;// arrive at the closing parenthesis of the first open
                       // parenthesis
      }

      index++;
    }

    throw new ETLException("Parenthesis is not closed properly: "
        + commaSeperatedExpressions);
  }

  // return the first comma postion in the top level in a common seperated
  // expressions
  public static int getFirstCommaPostionOnTopLevel(
      String commaSeperatedExpressions) throws ETLException {

    int openPostion = StringUtils.indexOf(commaSeperatedExpressions,
        Constants.OPEN_PARENTHESIS);
    if (openPostion < 0)
      return -1; // no parenthesis

    int commaPostion = StringUtils.indexOf(commaSeperatedExpressions,
        Constants.COMMA);

    if (commaPostion < openPostion)
      return -1;// no parenthesis in the first expression

    int index = openPostion + 1;
    int netCount = 1;
    while (index < commaSeperatedExpressions.length()) {
      char c = commaSeperatedExpressions.charAt(index);
      if (c == '(') {
        netCount++;
      } else if (c == ')') {
        netCount--;
        if (netCount == 0)
          return index;// arrive at the closing parenthesis of the first open
                       // parenthesis
      }

      index++;
    }
    throw new ETLException("Parenthesis is not closed properly: "
        + commaSeperatedExpressions);
  }

  private static void spawnExpressions(String commaSeperatedExpressions,
      List<String> expressions) throws ETLException {

    int closingParenthesisPosition = getTopParenthesisClosingPositionOfFirstExpression(commaSeperatedExpressions);

    if (closingParenthesisPosition < 0) {// no parenthesis in first expression

      int firstCommaPostionOnTopLevel = StringUtils.indexOf(
          commaSeperatedExpressions, Constants.COMMA);

      if (firstCommaPostionOnTopLevel < 0) {// only one expression
        String firstExpression = commaSeperatedExpressions;
        expressions.add(firstExpression);
        return;
      }

      String firstExpression = commaSeperatedExpressions.substring(0,
          firstCommaPostionOnTopLevel);
      expressions.add(firstExpression);
      String remaining = commaSeperatedExpressions
          .substring(firstCommaPostionOnTopLevel + 1);// skip the "," after the
                                                      // first expression
      spawnExpressions(remaining, expressions);
      return;
    }

    // first expression with parenthesis

    String firstExpression = commaSeperatedExpressions.substring(0,
        closingParenthesisPosition + 1);
    expressions.add(firstExpression);

    if (firstExpression.length() == commaSeperatedExpressions.length())
      return;// only one expression

    String remaining = commaSeperatedExpressions
        .substring(closingParenthesisPosition + 2);// skip the "," after the
                                                   // first expression
    spawnExpressions(remaining, expressions);
  }

  public static String getEscapedDelimiter(String delimiter) {
    StringBuilder sb = new StringBuilder();

    for (char c : delimiter.toCharArray()) {
      if (c == '|' || c == '$') {
        sb.append("\\").append(c);
      } else {
        sb.append(c);
      }
    }

    return sb.toString();
  }

  public static String getEscapedString(String s) {
    StringBuilder sb = new StringBuilder();

    for (char c : s.toCharArray()) {
      if (c == '|' || c == '$' || c == '\'') {
        sb.append("\\").append(c);
      } else {
        sb.append(c);
      }
    }

    return sb.toString();
  }

  public static String getStringFromMap(Map<String, String> map,
      String entrySeperator, String keyValueSeperator) {
    StringBuilder stringBuilder = new StringBuilder();
    for (Object key : map.keySet()) {
      if (stringBuilder.length() > 0) {
        stringBuilder.append(entrySeperator);
      }
      String value = (String) map.get(key);
      stringBuilder.append(key);
      stringBuilder.append(keyValueSeperator);
      stringBuilder.append(value);
    }

    return stringBuilder.toString();
  }

  public static Map<String, String> getMapFromString(String str,
      String entrySeperator, String keyValueSeperator) {
    HashMap<String, String> map = new HashMap<String, String>();
    String[] entrys = StringUtils.splitByWholeSeparatorPreserveAllTokens(str,
        entrySeperator);
    for (String entry : entrys) {
      String[] keyValues = StringUtils.splitByWholeSeparatorPreserveAllTokens(
          entry, keyValueSeperator);
      map.put(keyValues[0], keyValues[1]);
    }

    return map;
  }

  public static boolean doesTableExist(HBaseAdmin hbaseAdmin, String tableName)
      throws IOException {
    return hbaseAdmin.tableExists(tableName);
  }

  public static int getExistedTableRegionsNum(HBaseAdmin hbaseAdmin,
      String tableName) throws IOException {
    return hbaseAdmin.getTableRegions(Bytes.toBytes(tableName)).size();
  }

  public static void createTable(Configuration conf, HBaseAdmin hbaseAdmin,
      TargetTableSpec tableSpec) throws IOException {
    HTableDescriptor htd = new HTableDescriptor(TableName.valueOf(tableSpec
        .getTableName()));
    List<String> deDupFamilys = new ArrayList<String>();
    for (Map.Entry<String, ColumnSpec> e : tableSpec.getColumnMap().entrySet()) {
      String familyQualifierPair = e.getKey();
      String[] ss = StringUtils.splitByWholeSeparatorPreserveAllTokens(
          familyQualifierPair, Constants.FAMILY_QUALIFIER_SPLIT_CHARACTER);
      if (ss[0].isEmpty())
        continue;
      if (!deDupFamilys.contains(ss[0]))
        deDupFamilys.add(ss[0]);
    }

    if (deDupFamilys.isEmpty())
      throw new IOException("No target table family is found!");
    for (String family : deDupFamilys) {
      HColumnDescriptor hcd = new HColumnDescriptor(family).setMaxVersions(1)
          .setCompressionType(Algorithm.SNAPPY)
          .setBloomFilterType(BloomType.ROW);
      htd.addFamily(hcd);
    }

    // hard code column family i for index
    HColumnDescriptor indexFamily = new HColumnDescriptor(Constants.FAMILY_I)
        .setMaxVersions(1).setCompressionType(Algorithm.SNAPPY)
        .setBloomFilterType(BloomType.ROW);
    htd.addFamily(indexFamily);

    if (LOGGER.isDebugEnabled()) {
      LOGGER.debug("use index ");
    }

    try {
      // judge whether it builds index or not
      if (conf.getBoolean("buildIndex", false)) {
        htd.setValue(HTableDescriptor.SPLIT_POLICY,
            ConstantSizeRegionSplitPolicy.class.getName());
        // set index corprocessor
        htd.addCoprocessor(RecordServiceEndpoint.class.getName(),
            new Path(conf.get(Constants.HBASE_COPROCESSOR_LOCATION)), 1001,
            null);
        if (LOGGER.isDebugEnabled()) {
          LOGGER.debug("added coprocessor"
              + conf.get(Constants.HBASE_COPROCESSOR_LOCATION));
        }
      }
    } catch (IOException e1) {
      // TODO Auto-generated catch block e1.printStackTrace(); }
    }

    if (tableSpec.getSplitKeySpec() != null
        && !tableSpec.getSplitKeySpec().isEmpty()) {
      byte[][] splits = IndexUtil.getSplitKeys(tableSpec.getSplitKeySpec());
      hbaseAdmin.createTable(htd, splits);
    } else {
      hbaseAdmin.createTable(htd);
    }
  }

  public static void createMalformedLineLogTable(HBaseAdmin hbaseAdmin,
      String exceptionLogTableName) throws IOException {
    HTableDescriptor htd = new HTableDescriptor(
        exceptionLogTableName.getBytes());
    HColumnDescriptor hcd = new HColumnDescriptor(
        HBaseTableSpec.COLUMN_FAMILY_NAME_BYTES).setMaxVersions(1)
        .setCompressionType(Algorithm.SNAPPY).setBloomFilterType(BloomType.ROW);

    htd.addFamily(hcd);
    hbaseAdmin.createTable(htd);
  }

  public static void loadHFilesToHBase(Configuration conf,
      String hfileOutputDir, String htableName, String change777PermShDir)
      throws Exception {
    // load hifles to hbase table if hfileOutputDir exists
    if (hfileOutputDir != null && !hfileOutputDir.isEmpty()) {
      Path outputDir = new Path(hfileOutputDir);
      HTable hTable = new HTable(conf, htableName);
      // change hfile permission
      LOGGER.info("change hfile outout path to permission 777.");
      Runtime.getRuntime().exec(
          new String[] { "sh", change777PermShDir, hfileOutputDir });
      LOGGER.info("load hfiles to hbase table starts.");
      LoadIncrementalHFiles loader = new LoadIncrementalHFiles(conf);
      loader.doBulkLoad(outputDir, hTable);
      LOGGER.info("load hfiles to hbase table finished.");
    }
  }

  // convert String to Byte[]
  public static byte[] StringToByte(String str, String charEncode) {
    byte[] destObj = null;
    try {
      if (null == str || str.trim().equals("")) {
        destObj = new byte[0];
        return destObj;
      } else {
        destObj = str.getBytes(charEncode);
      }
    } catch (UnsupportedEncodingException e) {
      e.printStackTrace();
    }
    return destObj;
  }

  public static String convertByteArrayToString(byte[] rowkey) {
    String strRead = new String(rowkey);
    strRead = String.copyValueOf(strRead.toCharArray(), 0, rowkey.length);
    return strRead;
  }

  public static byte[] assignByteArray(byte[] src, int index, int length) {
    byte[] dest = new byte[length];
    for (int i = index; i < length + index; i++) {
      dest[i - index] = src[i];
    }
    return dest;
  }

  public static byte[] mergeByteArray(byte[] src, byte[] dest,
      int offsetOfDest, byte delimiter, boolean addDelimiter) {
    for (int i = 0; i < src.length; i++) {
      dest[offsetOfDest++] = src[i];
    }
    if (addDelimiter) {
      dest[offsetOfDest] = delimiter;
    }
    return dest;
  }

  public static HashMap<Integer, byte[]> split(final byte[] str, int length,
      final byte[] separator) {
    if (str == null) {
      return null;
    }
    substrings.clear();
    final int len = length;

    if (len == 0) {
      return null;
    }

    final int separatorLength = separator.length;

    int beg = 0;
    int end = 0;
    int i = 0;

    while (end < len) {
      end = indexOf(str, 0, len, separator, 0, separatorLength, beg);

      if (end > -1) {
        substrings.put(i, assignByteArray(str, beg, end - beg));
        beg = end + separatorLength;
      } else {
        end = len;
        substrings.put(i, assignByteArray(str, beg, end - beg));
      }
      i++;
    }
    return substrings;
  }

  public static int indexOf(final byte[] source, final int sourceOffset,
      final int sourceCount, final byte[] target, final int targetOffset,
      final int targetCount, int fromIndex) {
    if (fromIndex >= sourceCount) {
      return (targetCount == 0 ? sourceCount : -1);
    }
    if (fromIndex < 0) {
      fromIndex = 0;
    }
    if (targetCount == 0) {
      return fromIndex;
    }

    byte first = target[targetOffset];
    int max = sourceOffset + (sourceCount - targetCount);

    for (int i = sourceOffset + fromIndex; i <= max; i++) {
      /* Look for first character. */
      if (source[i] != first) {
        while (++i <= max && source[i] != first)
          ;
      }

      /* Found first character, now look at the rest of v2 */
      if (i <= max) {
        int j = i + 1;
        int end = j + targetCount - 1;
        for (int k = targetOffset + 1; j < end && source[j] == target[k]; j++, k++)
          ;

        if (j == end) {
          /* Found whole string. */
          return i - sourceOffset;
        }
      }
    }
    return -1;
  }

  public static String getTempDir(JobClient client) {
    String tempPathString = null;
    // Class<? extends JobClient> x = client.getClass();
    // Method m2 = x.getMethod("getTempDir", null);
    // if (m2 != null) {
    // tempPathString = (String) m2.invoke(client, null);
    // } else {
    // tempPathString = System.getProperty("java.io.tmpdir");
    // }

    // tempPathString = client.getTempDir().toString();
    tempPathString = System.getProperty("java.io.tmpdir");
    return tempPathString;
  }

  public static byte[] genRowKey() {
    byte[] randomRowKeyPrefix = RowKeyUtil.genRandomRowKeyPrefix();
    byte[] rowKey = RowKeyUtil.genRowKey(randomRowKeyPrefix);
    return rowKey;
  }

  public static byte[] findRegionStarKey(String arr, String key) {
    String[] arrays = arr.split(",");
    int bottom = 0, top = arrays.length - 1;
    int middle = -1;
    if (key.compareTo(arrays[0]) < 0) {
      return Bytes.toBytes("0000");
    }
    while (bottom <= top) {
      middle = (bottom + top) / 2;
      if (arrays[middle].compareTo(key) == 0) {
        return Bytes.toBytes(arrays[middle]);
      }
      if (arrays[middle].compareTo(key) > 0) {
        top = middle - 1;
      } else {
        bottom = middle + 1;
      }
    }
    if (bottom > top || arrays[bottom].compareTo(key) > 0) {
      return Bytes.toBytes(arrays[bottom - 1]);
    }
    return Bytes.toBytes(arrays[bottom]);
  }

  public static ByteArray convertStringToByteArray(String str) {
    byte[] StrBytes = Bytes.toBytes(str);
    return new ByteArray(StrBytes);
  }

}
