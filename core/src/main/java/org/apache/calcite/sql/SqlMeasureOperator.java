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
package org.apache.calcite.sql;

import org.apache.calcite.sql.type.InferTypes;
import org.apache.calcite.sql.type.OperandTypes;
import org.apache.calcite.sql.type.ReturnTypes;
import org.apache.calcite.sql.type.SqlTypeTransforms;

/**
 * The {@code MEASURE} wraps an expression in the {@code SELECT} clause and tags
 * it as a measure.
 *
 * <p>For example, the SQL '{@code x + 1 AS MEASURE y}' becomes the
 * {@code SqlNode} AST '{@code AS(MEASURE(x + 1), y)}'.
 *
 * <p>The operator is not used in {@code RexNode}.
 */
public class SqlMeasureOperator extends SqlInternalOperator {
  /**
   * Creates a MEASURE operator.
   */
  public SqlMeasureOperator() {
    super("MEASURE", SqlKind.MEASURE, 20, true,
        ReturnTypes.ARG0.andThen(SqlTypeTransforms.TO_MEASURE),
        InferTypes.RETURN_TYPE, OperandTypes.ANY);
  }

  //~ Methods ----------------------------------------------------------------

  @Override public SqlSyntax getSyntax() {
    return SqlSyntax.INTERNAL;
  }

  @Override public void unparse(SqlWriter writer, SqlCall call, int leftPrec,
      int rightPrec) {
    // AS should have checked for MEASURE wrapper around first argument
    throw new UnsupportedOperationException();
  }
}
