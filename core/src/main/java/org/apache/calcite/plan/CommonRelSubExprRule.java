/*
 * Licensed to the Apache Software Foundation (ASF) under one or more
 * contributor license agreements.  See the NOTICE file distributed with
 * this work for additional information regarding copyright ownership.
 * The ASF licenses this file to you under the Apache License, Version 2.0
 * (the "License"); you may not use this file except in compliance with
 * the License.  You may obtain a copy of the License at
 *
 * http://www.apache.org/licenses/LICENSE-2.0
 *
 * Unless required by applicable law or agreed to in writing, software
 * distributed under the License is distributed on an "AS IS" BASIS,
 * WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
 * See the License for the specific language governing permissions and
 * limitations under the License.
 */
package org.apache.calcite.plan;


/**
 * A <code>CommonRelSubExprRule</code> is an abstract base class for rules
 * that are fired only on relational expressions that appear more than once
 * in a query tree.
 */
public abstract class CommonRelSubExprRule extends RelOptRule {
  //~ Constructors -----------------------------------------------------------

  /**
   * Creates a <code>CommonRelSubExprRule</code>.
   *
   * @param operand root operand, must not be null
   */
  public CommonRelSubExprRule(RelOptRuleOperand operand) {
    super(operand);
  }
}

// End CommonRelSubExprRule.java
