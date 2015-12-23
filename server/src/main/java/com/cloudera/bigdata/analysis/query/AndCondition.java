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

import java.util.Collection;

/**
 * This class implements boolean AND - all sub-expressions must be true in order
 * for it to be true.
 */
public class AndCondition extends CompositeCondition {
  /**
   * Internal constructor.
   */
  public AndCondition() {
    super();
  }

  /**
   * Constructs an and expression with provided expression.
   * 
   * @param conditions
   *          the expression
   */
  public AndCondition(Condition... conditions) {
    super(conditions);
  }

  /**
   * Constructs an and expression with provided expression.
   * 
   * @param conditions
   *          the expression
   */
  public AndCondition(Collection<Condition> conditions) {
    super(conditions);
  }

  /**
   * Adds the condition to the set of condition.
   * 
   * @param condition
   *          the condition
   * @return this
   * @see CompositeCondition#add(Condition)
   */
  public AndCondition and(Condition condition) {
    return (AndCondition) super.add(condition);
  }

  @Override
  public String toString() {
    StringBuilder sb = new StringBuilder("AND{");
    for (Condition condition : children) {
      sb.append(condition).append(",");
    }
    sb.append("}");
    return sb.toString();
  }
}
