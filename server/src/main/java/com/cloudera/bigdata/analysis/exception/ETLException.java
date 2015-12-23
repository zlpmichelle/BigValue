package com.cloudera.bigdata.analysis.exception;

public class ETLException extends Exception {
  /**
   * Customized FormatException
   */
  private static final long serialVersionUID = 1L;

  public ETLException(String msg) {
    super(msg);
  }

  public static void handle(Exception e) {
    e.printStackTrace();
    System.exit(1);
  }

  public static void handle(String errMsg) {
    System.err.println(errMsg);
    System.exit(1);
  }

}
