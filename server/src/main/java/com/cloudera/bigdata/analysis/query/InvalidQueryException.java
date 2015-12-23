package com.cloudera.bigdata.analysis.query;

/**
 * If condition is invalid or conflict, throw this exception, i.e. a > 10 and a
 * < 9 or given table,qualifier,operator,value are invalid.
 */
public class InvalidQueryException extends RuntimeException {
  public InvalidQueryException() {
  }

  public InvalidQueryException(String message) {
    super(message);
  }

  public InvalidQueryException(String message, Throwable cause) {
    super(message, cause);
  }

  public InvalidQueryException(Throwable cause) {
    super(cause);
  }
}
