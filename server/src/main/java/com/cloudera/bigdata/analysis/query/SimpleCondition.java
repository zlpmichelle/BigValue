/*
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

import java.io.DataInput;
import java.io.DataOutput;
import java.io.IOException;

import org.apache.hadoop.hbase.util.Bytes;
import org.apache.hadoop.hdfs.util.ByteArray;

/**
 * The basic simple condition.
 */
public class SimpleCondition extends Condition {

  private ByteArray columnFamily;
  private ByteArray qualifier;
  private Operator operator;
  private ByteArray value;

  /**
   * No args constructor.
   */
  public SimpleCondition() {
  }

  /**
   * Convenience constrcutor that takes strings and converts from to ByteArray.
   * 
   * @param columnFamily
   *          the column name
   * @param qualifier
   *          the column qualifier
   * @param operator
   *          the operator
   * @param value
   *          the value
   */
  public SimpleCondition(String columnFamily, String qualifier,
      Operator operator, ByteArray value) {
    this(new ByteArray(Bytes.toBytes(columnFamily)), new ByteArray(
        Bytes.toBytes(qualifier)), operator, value);
  }

  /**
   * Convenience constrcutor that takes strings and converts from to ByteArray.
   * 
   * @param columnFamily
   *          the column name
   * @param qualifier
   *          the column qualifier
   * @param operator
   *          the operator
   * @param value
   *          the value
   * @param includeMissing
   *          include missing ids
   */
  public SimpleCondition(String columnFamily, String qualifier,
      Operator operator, ByteArray value, boolean includeMissing) {
    this(new ByteArray(Bytes.toBytes(columnFamily)), new ByteArray(
        Bytes.toBytes(qualifier)), operator, value, includeMissing);
  }

  /**
   * Partial constructor with all required fields.
   * 
   * @param columnFamily
   *          the column name
   * @param qualifier
   *          the column qualifier
   * @param operator
   *          the operator
   * @param value
   *          the value
   */
  public SimpleCondition(ByteArray columnFamily, ByteArray qualifier,
      Operator operator, ByteArray value) {
    this(columnFamily, qualifier, operator, value, true);
  }

  /**
   * Full constructor with all fields.
   * 
   * @param columnFamily
   *          the column name
   * @param qualifier
   *          the column qualifier
   * @param operator
   *          the operator
   * @param value
   *          the value
   * @param includeMissing
   *          should the condition result include ids which are missing from the
   *          index. Same idea as
   *          {@link org.apache.hadoop.hbase.filter.SingleColumnValueFilter#filterIfMissing}
   *          . Default value is true.
   */
  public SimpleCondition(ByteArray columnFamily, ByteArray qualifier,
      Operator operator, ByteArray value, boolean includeMissing) {
    assert columnFamily != null : "The columnFamily must not be null";
    assert qualifier != null : "The qualifier must not be null";
    assert operator != null : "The operator must not be null";
    assert value != null : "The value must not be null";

    this.columnFamily = columnFamily;
    this.qualifier = qualifier;
    this.operator = operator;
    this.value = value;
  }

  /**
   * The {@link org.apache.hadoop.hbase.HColumnDescriptor#getName() column
   * family name} that the {@link #getQualifier() qualifier} is a member of.
   * 
   * @return the column family name
   */
  public ByteArray getColumnFamily() {
    return columnFamily;
  }

  /**
   * The column qualifier.
   * 
   * @return the qualifier
   */
  public ByteArray getQualifier() {
    return qualifier;
  }

  /**
   * The operator.
   * 
   * @return the operator
   */
  public Operator getOperator() {
    return operator;
  }

  /**
   * The value.
   * 
   * @return the value
   */
  public ByteArray getValue() {
    return value;
  }

  /**
   * {@inheritDoc}
   */
  @Override
  public void write(DataOutput dataOutput) throws IOException {
    Bytes.writeByteArray(dataOutput, columnFamily.getBytes());
    Bytes.writeByteArray(dataOutput, qualifier.getBytes());
    Bytes.writeByteArray(dataOutput, Bytes.toBytes(operator.toString()));
    Bytes.writeByteArray(dataOutput, value.getBytes());
  }

  /**
   * {@inheritDoc}
   */
  @Override
  public void readFields(DataInput dataInput) throws IOException {
    columnFamily = new ByteArray(Bytes.readByteArray(dataInput));
    qualifier = new ByteArray(Bytes.readByteArray(dataInput));
    operator = Operator.valueOf(Bytes.toString(Bytes.readByteArray(dataInput)));
    value = new ByteArray(Bytes.readByteArray(dataInput));
  }

  @Override
  public String toString() {
    return "Condition{" + "columnFamily="
        + Bytes.toStringBinary(columnFamily.getBytes()) + ", qualifier="
        + Bytes.toStringBinary(qualifier.getBytes()) + ", operator=" + operator
        + ", value=" + Bytes.toStringBinary(value.getBytes()) + '}';
  }

  /**
   * {@inheritDoc}
   */
  @Override
  public boolean equals(Object o) {
    if (this == o)
      return true;
    if (o == null || getClass() != o.getClass())
      return false;

    SimpleCondition that = (SimpleCondition) o;

    if (!columnFamily.equals(that.columnFamily))
      return false;
    if (operator != that.operator)
      return false;
    if (!qualifier.equals(that.qualifier))
      return false;
    if (!value.equals(that.value))
      return false;

    return true;
  }

  /**
   * {@inheritDoc}
   */
  @Override
  public int hashCode() {
    int result = columnFamily.hashCode();
    result = 31 * result + qualifier.hashCode();
    result = 31 * result + operator.hashCode();
    result = 31 * result + value.hashCode();
    return result;
  }

}
