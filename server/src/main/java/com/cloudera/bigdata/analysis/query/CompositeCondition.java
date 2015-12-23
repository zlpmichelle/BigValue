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
import java.util.Arrays;
import java.util.Collection;
import java.util.HashSet;
import java.util.Set;

import com.cloudera.bigdata.analysis.index.util.WritableUtil;

/**
 * A composite condition aggregates a set of child conditions into a logical
 * group such as boolean logic.
 */
public abstract class CompositeCondition extends Condition {
  /**
   * The set of child expressions.
   */
  protected Set<Condition> children;

  /**
   * Class constructor.
   * 
   * @param conditions
   *          the conditions to be evaluated
   */
  public CompositeCondition(Condition... conditions) {
    assert conditions != null : "conditions cannot be null or empty";
    this.children = new HashSet<Condition>(Arrays.asList(conditions));
  }

  /**
   * Class constructor.
   * 
   * @param conditions
   *          the conditions to be evaluated
   */
  public CompositeCondition(Collection<Condition> conditions) {
    this.children = new HashSet<Condition>(conditions);
  }

  /**
   * Add an condition to the child set.
   * 
   * @param condition
   *          the condition to add
   * @return this
   */
  public CompositeCondition add(Condition condition) {
    this.children.add(condition);
    return this;
  }

  /**
   * Returns the set of child expressions.
   * 
   * @return the expression set
   */
  public Set<Condition> getChildren() {
    return children;
  }

  /**
   * {@inheritDoc}
   */
  @Override
  public boolean equals(Object o) {
    if (this == o) {
      return true;
    }
    if (o == null || getClass() != o.getClass()) {
      return false;
    }

    CompositeCondition that = (CompositeCondition) o;

    if (!children.equals(that.children)) {
      return false;
    }

    return true;
  }

  /**
   * {@inheritDoc}
   */
  @Override
  public int hashCode() {
    return children.hashCode();
  }

  /**
   * {@inheritDoc}
   */
  @Override
  public void write(DataOutput dataOutput) throws IOException {
    dataOutput.writeInt(children.size());
    for (Condition child : children) {
      WritableUtil.writeInstance(dataOutput, child);
    }
  }

  /**
   * {@inheritDoc}
   */
  @Override
  public void readFields(DataInput dataInput) throws IOException {
    int size = dataInput.readInt();
    children = new HashSet<Condition>(size);
    for (int i = 0; i < size; i++) {
      Condition condition = WritableUtil.readInstance(dataInput,
          Condition.class);
      children.add(condition);
    }
  }

}
