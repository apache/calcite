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
package org.apache.calcite.sql.fun;

import org.apache.calcite.rel.type.RelDataType;
import org.apache.calcite.sql.SqlInternalOperator;
import org.apache.calcite.sql.SqlKind;
import org.apache.calcite.sql.SqlOperatorBinding;
import org.apache.calcite.sql.type.InferTypes;
import org.apache.calcite.sql.type.OperandTypes;
import org.apache.calcite.sql.type.ReturnTypes;
import org.apache.calcite.util.Sarg;

/** Operator that tests whether its left operand is included in the range of
 * values covered by search arguments. */
class SqlSearchOperator extends SqlInternalOperator {
  SqlSearchOperator() {
    super("SEARCH", SqlKind.SEARCH, 30, true,
        ReturnTypes.BOOLEAN.andThen(SqlSearchOperator::makeNullable),
        InferTypes.FIRST_KNOWN,
        OperandTypes.COMPARABLE_UNORDERED_COMPARABLE_UNORDERED);
  }

  /** Sets whether a call to SEARCH should allow nulls.
   *
   * <p>For example, if the type of {@code x} is NOT NULL, then
   * {@code SEARCH(x, Sarg[10])} will never return UNKNOWN.
   * It is evident from the expansion, "x = 10", but holds for all Sarg
   * values.
   *
   * <p>If {@link Sarg#containsNull} is true, SEARCH will never return
   * UNKNOWN. For example, {@code SEARCH(x, Sarg[10 OR NULL])} expands to
   * {@code x = 10 OR x IS NOT NULL}, which returns {@code TRUE} if
   * {@code x} is NULL, {@code TRUE} if {@code x} is 10, and {@code FALSE}
   * for all other values.
   */
  private static RelDataType makeNullable(SqlOperatorBinding binding,
      RelDataType type) {
    final boolean nullable = binding.getOperandType(0).isNullable()
        && !binding.getOperandLiteralValue(1, Sarg.class).containsNull;
    return binding.getTypeFactory().createTypeWithNullability(type, nullable);
  }
}
