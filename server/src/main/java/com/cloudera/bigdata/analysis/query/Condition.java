/**
 * Copyright 2010 The Apache Software Foundation
 *
 * Licensed to the Apache Software Foundation (ASF) under one
 * or more contributor license agreements.  See the NOTICE file
 * distributed with this work for additional information
 * regarding copyright ownership.  The ASF licenses this file
 * to you under the Apache License, Version 2.0 (the
 * "License"); you may not use this file except in compliance
 * with the License.  You may obtain a copy of the License at
 *
 *     http://www.apache.org/licenses/LICENSE-2.0
 *
 * Unless required by applicable law or agreed to in writing, software
 * distributed under the License is distributed on an "AS IS" BASIS,
 * WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
 * See the License for the specific language governing permissions and
 * limitations under the License.
 */
package com.cloudera.bigdata.analysis.query;

import org.apache.hadoop.hdfs.util.ByteArray;
import org.apache.hadoop.io.Writable;

/**
 * The Class representing search condition. The condition is similar to Node
 * conception in the condition tree. There is a concrete subclass (@link
 * SimpleCondition} which is leaf node, and several {@link CompositeCondition}
 * which is none-leaf node.
 */
public abstract class Condition implements Writable {
  /**
   * {@inheritDoc}
   */
  public abstract int hashCode();

  /**
   * {@inheritDoc}
   */
  public abstract boolean equals(Object o);

  // public abstract List<byte[]> getQualifiers();

  /**
   * Creates and returns an {@link OrCondition} instance.
   * 
   * @param conditions
   *          the conditions
   * @return an instance
   */
  public static OrCondition or(Condition... conditions) {
    if (conditions.length < 2) {
      throw new InvalidQueryException(
          "OR is binary operation, 2 conditions required at least!");
    }
    return new OrCondition(conditions);
  }

  /**
   * Creates and returns an {@link AndCondition} instance.
   * 
   * @param conditions
   *          the conditions
   * @return an instance
   */
  public static AndCondition and(Condition... conditions) {
    if (conditions.length < 2) {
      throw new InvalidQueryException(
          "AND is binary operation, 2 conditions required at least!");
    }
    return new AndCondition(conditions);
  }

  /**
   * Creates and returns an {@link SimpleCondition} instance.
   * 
   * @param family
   *          the column family name
   * @param qualifier
   *          the qualifier
   * @param operator
   *          the operator
   * @param value
   *          the value
   * @return the instance
   */
  public static SimpleCondition condition(ByteArray family,
      ByteArray qualifier, Operator operator, ByteArray value) {
    return new SimpleCondition(family, qualifier, operator, value);
  }

  /**
   * Creates and returns an {@link SimpleCondition} instance.
   * 
   * @param family
   *          the column family name
   * @param qualifier
   *          the qualifier
   * @param operator
   *          the operator
   * @param value
   *          the value
   * @param includeMissing
   *          include ids missing from the index. Same idea as
   *          {@link org.apache.hadoop.hbase.filter.SingleColumnValueFilter#filterIfMissing}
   *          . true by default
   * @return the instance
   */
  public static SimpleCondition condition(ByteArray family,
      ByteArray qualifier, Operator operator, ByteArray value,
      boolean includeMissing) {
    return new SimpleCondition(family, qualifier, operator, value,
        includeMissing);
  }

  /**
   * Creates and returns an {@link SimpleCondition} instance.
   * 
   * @param family
   *          the column family name
   * @param qualifier
   *          the qualifier
   * @param operator
   *          the operator
   * @param value
   *          the value
   * @return the instance
   */
  public static SimpleCondition condition(String family, String qualifier,
      Operator operator, ByteArray value) {
    return new SimpleCondition(family, qualifier, operator, value);
  }

  /**
   * Creates and returns an {@link SimpleCondition} instance.
   * 
   * @param family
   *          the column family name
   * @param qualifier
   *          the qualifier
   * @param operator
   *          the operator
   * @param value
   *          the value
   * @param includeMissing
   *          include ids missing from the index. Same idea as
   *          {@link org.apache.hadoop.hbase.filter.SingleColumnValueFilter#filterIfMissing}
   *          . true by default
   * @return the instance
   */
  public static SimpleCondition condition(String family, String qualifier,
      Operator operator, ByteArray value, boolean includeMissing) {
    return new SimpleCondition(family, qualifier, operator, value,
        includeMissing);
  }

}
