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
import org.apache.calcite.rel.type.RelDataTypeFactory;
import org.apache.calcite.sql.SqlCall;
import org.apache.calcite.sql.SqlKind;
import org.apache.calcite.sql.SqlOperatorBinding;
import org.apache.calcite.sql.SqlSpecialOperator;
import org.apache.calcite.sql.SqlUtil;
import org.apache.calcite.sql.SqlWriter;
import org.apache.calcite.sql.type.InferTypes;
import org.apache.calcite.sql.type.OperandTypes;

/**
 * SqlRowOperator represents the special ROW constructor.
 *
 * <p>TODO: describe usage for row-value construction and row-type construction
 * (SQL supports both).
 */
public class SqlRowOperator extends SqlSpecialOperator {
  //~ Constructors -----------------------------------------------------------

  public SqlRowOperator(String name) {
    super(name,
        SqlKind.ROW, MDX_PRECEDENCE,
        false,
        null,
        InferTypes.RETURN_TYPE,
        OperandTypes.VARIADIC);
  }

  //~ Methods ----------------------------------------------------------------

  @Override public RelDataType inferReturnType(
      final SqlOperatorBinding opBinding) {
    // The type of a ROW(e1,e2) expression is a record with the types
    // {e1type,e2type}.  According to the standard, field names are
    // implementation-defined.
    final RelDataTypeFactory typeFactory = opBinding.getTypeFactory();
    final RelDataTypeFactory.Builder builder = typeFactory.builder();
    for (int index = 0; index < opBinding.getOperandCount(); index++) {
      builder.add(SqlUtil.deriveAliasFromOrdinal(index),
          opBinding.getOperandType(index));
    }
    final RelDataType recordType = builder.build();

    // The value of ROW(e1,e2) is considered null if and only all of its
    // fields (i.e., e1, e2) are null. Otherwise, ROW can not be null.
    final boolean nullable =
        recordType.getFieldList().stream()
            .allMatch(f -> f.getType().isNullable());
    return typeFactory.createTypeWithNullability(recordType, nullable);
  }

  @Override public void unparse(
      SqlWriter writer,
      SqlCall call,
      int leftPrec,
      int rightPrec) {
    SqlUtil.unparseFunctionSyntax(this, writer, call, false);
  }

  // override SqlOperator
  @Override public boolean requiresDecimalExpansion() {
    return false;
  }
}
