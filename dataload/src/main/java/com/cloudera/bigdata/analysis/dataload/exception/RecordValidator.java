package com.cloudera.bigdata.analysis.dataload.exception;

public interface RecordValidator {

  public void validate(String recordLine) throws FormatException;

}
