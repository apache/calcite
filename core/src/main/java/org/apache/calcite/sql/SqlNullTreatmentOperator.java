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

import org.apache.calcite.sql.type.OperandTypes;
import org.apache.calcite.sql.type.ReturnTypes;
import org.apache.calcite.sql.validate.SqlValidator;
import org.apache.calcite.sql.validate.SqlValidatorScope;

import com.google.common.base.Preconditions;

import static org.apache.calcite.util.Static.RESOURCE;

/**
 * An operator that decides how to handle null input
 * ({@code RESPECT NULLS} and {@code IGNORE NULLS}).
 *
 * <p>Currently, only the windowed aggregate functions {@code FIRST_VALUE},
 * {@code LAST_VALUE}, {@code LEAD} and {@code LAG} support it.
 *
 * @see SqlAggFunction#allowsNullTreatment()
 */
public class SqlNullTreatmentOperator extends SqlSpecialOperator {
  public SqlNullTreatmentOperator(SqlKind kind) {
    super(kind.sql, kind, 20, true, ReturnTypes.ARG0, null, OperandTypes.ANY);
    Preconditions.checkArgument(kind == SqlKind.RESPECT_NULLS
        || kind == SqlKind.IGNORE_NULLS);
  }

  @Override public void unparse(SqlWriter writer, SqlCall call, int leftPrec,
      int rightPrec) {
    assert call.operandCount() == 1;
    call.operand(0).unparse(writer, getLeftPrec(), getRightPrec());
    writer.keyword(getName());
  }

  public void validateCall(
      SqlCall call,
      SqlValidator validator,
      SqlValidatorScope scope,
      SqlValidatorScope operandScope) {
    assert call.getOperator() == this;
    assert call.operandCount() == 1;
    SqlCall aggCall = call.operand(0);
    if (!aggCall.getOperator().isAggregator()
        || !((SqlAggFunction) aggCall.getOperator()).allowsNullTreatment()) {
      throw validator.newValidationError(aggCall,
          RESOURCE.disallowsNullTreatment(aggCall.getOperator().getName()));
    }
  }
}

// End SqlNullTreatmentOperator.java
