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
 * Policy by which operands will be matched by relational expressions with
 * any number of children.
 */
public enum RelOptRuleOperandChildPolicy {
  /**
   * Signifies that operand can have any number of children.
   */
  ANY,

  /**
   * Signifies that operand has no children. Therefore it matches a
   * leaf node, such as a table scan or VALUES operator.
   *
   * <p>{@code RelOptRuleOperand(Foo.class, NONE)} is equivalent to
   * {@code RelOptRuleOperand(Foo.class)} but we prefer the former because
   * it is more explicit.</p>
   */
  LEAF,

  /**
   * Signifies that the operand's children must precisely match its
   * child operands, in order.
   */
  SOME,

  /**
   * Signifies that the rule matches any one of its parents' children.
   * The parent may have one or more children.
   */
  UNORDERED,
}

// End RelOptRuleOperandChildPolicy.java
