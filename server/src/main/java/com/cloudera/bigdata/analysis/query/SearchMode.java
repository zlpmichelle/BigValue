package com.cloudera.bigdata.analysis.query;

/**
 * To indicate what kind of strategy used when searching.
 */
public enum SearchMode {
  FULL_TABLE_SCAN_BASED_SEARCH(0), INDEX_BASED_SEARCH(1);

  private int code;

  private SearchMode(int code) {
    this.code = code;
  }

  public int getCode() {
    return code;
  }

  public static SearchMode fromCode(int code) {
    switch (code) {
    case 0:
      return FULL_TABLE_SCAN_BASED_SEARCH;
    case 1:
      return INDEX_BASED_SEARCH;
    default:
      return null;
    }
  }

  @Override
  public String toString() {
    switch (this) {
    case FULL_TABLE_SCAN_BASED_SEARCH:
      return "FULL_TABLE_SCAN_BASED_SEARCH";
    case INDEX_BASED_SEARCH:
      return "INDEX_BASED_SEARCH";
    default:
      return "";
    }
  }
}