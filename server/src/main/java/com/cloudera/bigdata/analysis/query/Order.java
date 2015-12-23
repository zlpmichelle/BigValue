package com.cloudera.bigdata.analysis.query;

/**
 * Returned result sorting order.
 */
public enum Order {

  ASC(0), DESC(1);

  private int code;

  private Order(int code) {
    this.code = code;
  }

  public static Order fromCode(int code) {
    switch (code) {
    case 0:
      return ASC;
    case 1:
      return DESC;
    default:
      return null;
    }
  }

  public int getCode() {
    return code;
  }

  public String toString() {
    switch (code) {
    case 0:
      return "ASC";
    case 1:
      return "DESC";
    default:
      return "";
    }
  }
}
