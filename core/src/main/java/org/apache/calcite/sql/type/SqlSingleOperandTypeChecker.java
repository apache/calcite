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
package org.apache.calcite.sql.type;

import org.apache.calcite.sql.SqlCallBinding;
import org.apache.calcite.sql.SqlNode;

/**
 * SqlSingleOperandTypeChecker is an extension of {@link SqlOperandTypeChecker}
 * for implementations which are capable of checking the type of a single
 * operand in isolation. This isn't meaningful for all type-checking rules (e.g.
 * SameOperandTypeChecker requires two operands to have matching types, so
 * checking one in isolation is meaningless).
 */
public interface SqlSingleOperandTypeChecker extends SqlOperandTypeChecker {
  //~ Methods ----------------------------------------------------------------

  /**
   * Checks the type of a single operand against a particular ordinal position
   * within a formal operator signature. Note that the actual ordinal position
   * of the operand being checked may be <em>different</em> from the position
   * of the formal operand.
   *
   * <p>For example, when validating the actual call
   *
   * <blockquote>
   * <pre>C(X, Y, Z)</pre>
   * </blockquote>
   *
   * <p>the strategy for validating the operand Z might involve checking its
   * type against the formal signature OP(W). In this case,
   * <code>iFormalOperand</code> would be zero, even though the position of Z
   * within call C is two.
   *
   * @param callBinding    description of the call being checked; this is only
   *                       provided for context when throwing an exception; the
   *                       implementation should <em>NOT</em> examine the
   *                       operands of the call as part of the check
   * @param operand        the actual operand to be checked
   * @param iFormalOperand the 0-based formal operand ordinal
   * @param throwOnFailure whether to throw an exception if check fails
   *                       (otherwise returns false in that case)
   * @return whether check succeeded
   */
  boolean checkSingleOperandType(
      SqlCallBinding callBinding,
      SqlNode operand,
      int iFormalOperand,
      boolean throwOnFailure);
}

// End SqlSingleOperandTypeChecker.java
