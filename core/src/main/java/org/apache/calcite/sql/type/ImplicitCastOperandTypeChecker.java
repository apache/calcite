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
import org.apache.calcite.sql.validate.implicit.TypeCoercion;

import java.util.List;

/**
 * An operand type checker that supports implicit type cast, see
 * {@link TypeCoercion#builtinFunctionCoercion(SqlCallBinding, List, List)}
 * for details.
 */
public interface ImplicitCastOperandTypeChecker {
  /**
   * Checks the types of an operator's all operands, but without type coercion.
   * This is mainly used as a pre-check when this checker is included as one of the rules in
   * {@link CompositeOperandTypeChecker} and the composite predicate is `OR`.
   *
   * @param callBinding    description of the call to be checked
   * @param throwOnFailure whether to throw an exception if check fails
   *                       (otherwise returns false in that case)
   * @return whether check succeeded
   */
  boolean checkOperandTypesWithoutTypeCoercion(SqlCallBinding callBinding, boolean throwOnFailure);

  /**
   * Get the operand SqlTypeFamily of formal index {@code iFormalOperand}.
   * This is mainly used to get the operand SqlTypeFamily when this checker is included as
   * one of the rules in {@link CompositeOperandTypeChecker} and the
   * composite predicate is `SEQUENCE`.
   *
   * @param iFormalOperand the formal operand index.
   * @return SqlTypeFamily of the operand.
   */
  SqlTypeFamily getOperandSqlTypeFamily(int iFormalOperand);
}

// End ImplicitCastOperandTypeChecker.java
