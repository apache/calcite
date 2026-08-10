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
import org.apache.calcite.sql.SqlCall;
import org.apache.calcite.sql.SqlFunction;
import org.apache.calcite.sql.SqlFunctionCategory;
import org.apache.calcite.sql.SqlKind;
import org.apache.calcite.sql.SqlNode;
import org.apache.calcite.sql.SqlOperatorBinding;
import org.apache.calcite.sql.SqlTableFunction;
import org.apache.calcite.sql.SqlWriter;
import org.apache.calcite.sql.type.OperandTypes;
import org.apache.calcite.sql.type.ReturnTypes;
import org.apache.calcite.sql.type.SqlReturnTypeInference;

/**
 * Generic representation of a user-defined table function (UDTF) that is not
 * tied to any specific SQL dialect.
 *
 * <p>Several source dialects (e.g. SQL Server, Snowflake) expose table-valued
 * functions whose row type is known only at the point where the function is
 * referenced, rather than from a fixed catalog definition. Rather than
 * defining a dedicated {@link SqlFunction} subclass per dialect/function
 * name, this class lets callers construct an ad-hoc table function operator
 * on the fly by supplying just the function {@code name} and its
 * {@link RelDataType row type}.
 *
 * <p>Instances are typically created while converting/translating a
 * dialect-specific table function call (e.g. during rel-to-SQL conversion),
 * so that the resulting {@link SqlCall} can be unparsed back out using the
 * original function name while still reporting the correct row type for
 * type inference.
 */
public class UserDefinedTableFunctionOperator extends SqlFunction implements SqlTableFunction {
  private final RelDataType rowType;

  public UserDefinedTableFunctionOperator(String name, RelDataType rowType) {
    super(name,
        SqlKind.UDTF,
        ReturnTypes.explicit(rowType),
        null,
        OperandTypes.VARIADIC,
        SqlFunctionCategory.USER_DEFINED_TABLE_FUNCTION);
    this.rowType = rowType;
  }

  @Override public SqlReturnTypeInference getRowTypeInference() {
    return this::getRowType;
  }

  public RelDataType getRowType(SqlOperatorBinding opBinding) {
    return rowType;
  }

  @Override public void unparse(
      SqlWriter writer,
      SqlCall call,
      int leftPrec,
      int rightPrec) {
    final SqlWriter.Frame frame = writer.startFunCall(getName());
    for (SqlNode sqlNode : call.getOperandList()) {
      writer.sep(",");
      sqlNode.unparse(writer, leftPrec, rightPrec);
    }
    writer.endFunCall(frame);
  }
}
