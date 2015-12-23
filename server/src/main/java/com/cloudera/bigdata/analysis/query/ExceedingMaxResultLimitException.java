package com.cloudera.bigdata.analysis.query;

/**
 * There is a max result limit for
 * {@link com.cloudera.bigdata.analysis.query.Query}, if any query object's
 * result limit exceed max value, this exception will be thrown. NOTE: too big
 * max result limit might cause out-of-memory issure.
 */
public class ExceedingMaxResultLimitException extends RuntimeException {

  public ExceedingMaxResultLimitException() {
    super();
  }

  public ExceedingMaxResultLimitException(String message) {
    super(message);
  }

  public ExceedingMaxResultLimitException(String message, Throwable cause) {
    super(message, cause);
  }

  public ExceedingMaxResultLimitException(Throwable cause) {
    super(cause);
  }
}
