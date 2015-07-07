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

import org.apache.calcite.sql.SqlCall;
import org.apache.calcite.sql.SqlFunctionCategory;
import org.apache.calcite.sql.SqlKind;
import org.apache.calcite.sql.SqlOperatorBinding;
import org.apache.calcite.sql.SqlWriter;
import org.apache.calcite.sql.type.OperandTypes;
import org.apache.calcite.sql.type.ReturnTypes;
import org.apache.calcite.sql.validate.SqlMonotonicity;

import com.google.common.base.Preconditions;

/**
 * Definition of the "FLOOR" and "CEIL" built-in SQL functions.
 */
public class SqlFloorFunction extends SqlMonotonicUnaryFunction {
  //~ Constructors -----------------------------------------------------------

  public SqlFloorFunction(SqlKind kind) {
    super(kind.name(), kind, ReturnTypes.ARG0_OR_EXACT_NO_SCALE, null,
        OperandTypes.or(OperandTypes.NUMERIC_OR_INTERVAL,
            OperandTypes.sequence(
                "'" + kind + "(<DATE> TO <TIME_UNIT>)'\n"
                + "'" + kind + "(<TIME> TO <TIME_UNIT>)'\n"
                + "'" + kind + "(<TIMESTAMP> TO <TIME_UNIT>)'",
                OperandTypes.DATETIME,
                OperandTypes.ANY)),
        SqlFunctionCategory.NUMERIC);
    Preconditions.checkArgument(kind == SqlKind.FLOOR || kind == SqlKind.CEIL);
  }

  //~ Methods ----------------------------------------------------------------

  @Override public SqlMonotonicity getMonotonicity(SqlOperatorBinding call) {
    // Monotonic iff its first argument is, but not strict.
    return call.getOperandMonotonicity(0).unstrict();
  }

  @Override public void unparse(SqlWriter writer, SqlCall call, int leftPrec,
      int rightPrec) {
    final SqlWriter.Frame frame = writer.startFunCall(getName());
    if (call.operandCount() == 2) {
      call.operand(0).unparse(writer, 0, 100);
      writer.sep("TO");
      call.operand(1).unparse(writer, 100, 0);
    } else {
      call.operand(0).unparse(writer, 0, 0);
    }
    writer.endFunCall(frame);
  }
}

// End SqlFloorFunction.java
