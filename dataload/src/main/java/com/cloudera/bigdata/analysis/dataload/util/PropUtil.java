package com.cloudera.bigdata.analysis.dataload.util;

import java.util.ResourceBundle;

/**
 * The utility class for server module.
 */
public abstract class PropUtil {

  private static ResourceBundle resource = ResourceBundle
      .getBundle("index-table-conf");

  /**
   * Get string value of a property.
   * 
   * @param key
   *          the property key
   * @return the property string value
   */
  public static String getStringProperty(String key) {
    return resource.getString(key);
  }

  /**
   * Get int value of a property.
   * 
   * @param key
   *          the property key
   * @return the property int value
   */
  public static int getIntProperty(String key) {
    return Integer.parseInt(resource.getString(key));
  }

  /**
   * Get byte value of a property.
   * 
   * @param key
   *          the property key
   * @return the property int value
   */
  public static char getCharProperty(String key) {
    if (resource.getString(key).length() >= 2) {
      throw new RuntimeException("Value of Property:" + key + " is not a char!");
    }
    return resource.getString(key).charAt(0);
  }

}
