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
package org.apache.calcite.rex;

import org.apache.calcite.rel.type.RelDataType;

import java.util.List;

/** Can reduce expressions, writing a literal for each into a list. */
public interface RexExecutor {

  /** Reduces expressions, and writes their results into {@code reducedValues}.
   *
   * <p>If an expression cannot be reduced, writes the original expression.
   * For example, {@code CAST('abc' AS INTEGER)} gives an error when executed, so the executor
   * ignores the error and writes the original expression.
   *
   * @param rexBuilder Rex builder
   * @param constExps Expressions to be reduced
   * @param reducedValues List to which reduced expressions are appended
   */
  void reduce(RexBuilder rexBuilder, List<RexNode> constExps, List<RexNode> reducedValues);

  /**
   * Checks if condition first implies (&rArr;) condition second.
   *
   * <p>This reduces to SAT problem which is NP-Complete.
   * When this method says first implies second then it is definitely true.
   * But it cannot prove that first does not imply second.
   *
   * @param rexBuilder Rex builder
   * @param rowType row type
   * @param first first condition
   * @param second second condition
   * @return true if it can prove first &rArr; second; otherwise false i.e.,
   * it doesn't know if implication holds
   */
  boolean implies(RexBuilder rexBuilder, RelDataType rowType, RexNode first, RexNode second);
}

// End RexExecutor.java
