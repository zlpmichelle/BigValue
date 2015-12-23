package com.cloudera.bigdata.analysis.dataload.transform;

import java.util.HashMap;

import org.apache.hadoop.hbase.client.Put;

import com.cloudera.bigdata.analysis.dataload.exception.FormatException;
import com.cloudera.bigdata.analysis.dataload.util.Util;

/*
 * Generate RowKey value and convert raw line into HBase Put.
 */
public abstract class ExtensibleHBaseRowConverter {
  /* the timestamp of Put operation */
  protected long timeStamp;

  private static HashMap<Class<? extends ExtensibleHBaseRowConverter>, ExtensibleHBaseRowConverter> extensibleHBaseRowConverters;

  static {
    extensibleHBaseRowConverters = new HashMap<Class<? extends ExtensibleHBaseRowConverter>, ExtensibleHBaseRowConverter>();
  }

  public void setTimeStamp(long timeStamp) {
    this.timeStamp = timeStamp;
  }

  /**
   * Calculate rowkey value, and convert raw line into HBase Put.Users can
   * directly write method to generated rowkey and misc fields, then add it into
   * HBase Put
   * 
   * @param line
   *          raw line
   * @return Put return the HBase Put
   * @throws FormatException
   */
  public abstract Put convertToPut(String line, boolean writeToWAL)
      throws FormatException;

  // public abstract long getSplitTime() throws FormatException;
  //
  // public abstract long getBuildRowTime() throws FormatException;
  //
  // public abstract long getBuildColumnTime() throws FormatException;
  //
  // public abstract long getComposeTime() throws FormatException;
  //
  // public abstract long getPutTime() throws FormatException;

  // initialize the ExtensibleHBaseRowConverter class
  public static ExtensibleHBaseRowConverter getExtendedHBaseRowConverter(
      String className) {
    Class<?> clazz = Util.forceInit(className);
    Class<? extends ExtensibleHBaseRowConverter> subClazz;
    try {
      subClazz = clazz.asSubclass(ExtensibleHBaseRowConverter.class);
    } catch (ClassCastException e) {
      throw new IllegalArgumentException("Cannot cast ", e);
    }
    return getExtendedHBaseRowConverter(subClazz);
  }

  // initialize the ExtensibleHBaseRowConverter class
  public static ExtensibleHBaseRowConverter getExtendedHBaseRowConverter(
      Class<? extends ExtensibleHBaseRowConverter> clazz) {
    ExtensibleHBaseRowConverter source = extensibleHBaseRowConverters
        .get(clazz);
    if (source == null) {
      // Try to force initialize source implementation
      Util.forceInit(clazz.getName());
    }
    return extensibleHBaseRowConverters.get(clazz);
  }

  // define class at every extended class start
  public static void define(Class<? extends ExtensibleHBaseRowConverter> clazz,
      ExtensibleHBaseRowConverter extensibleHBaseRowConverter) {
    extensibleHBaseRowConverters.put(clazz, extensibleHBaseRowConverter);
  }

}
