package com.cloudera.bigdata.analysis.query;

/**
 * The enum for specifying the function we're performing in a
 * {@link SimpleCondition}.
 */
public enum Operator {
  /**
   * The equals operator.
   */
  EQ,
  /**
   * The greater than operator.
   */
  GT,
  /**
   * The greater than or equals operator.
   */
  GTE,
  /**
   * The less than operator.
   */
  LT,
  /**
   * The less than or equals operator.
   */
  LTE,
  /**
   * The not equals operator.
   */
  NEQ,
  /**
   * The like equals operator. NOTE: LIKE operate only work on String value!!
   */
  LIKE
}