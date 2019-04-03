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
import org.apache.calcite.sql.SqlFunction;
import org.apache.calcite.sql.SqlFunctionCategory;
import org.apache.calcite.sql.SqlKind;
import org.apache.calcite.sql.SqlOperandCountRange;
import org.apache.calcite.sql.SqlWriter;
import org.apache.calcite.sql.type.OperandTypes;
import org.apache.calcite.sql.type.ReturnTypes;
import org.apache.calcite.sql.type.SqlOperandCountRanges;
import org.apache.calcite.sql.type.SqlOperandTypeChecker;
import org.apache.calcite.sql.validate.SqlValidator;

import java.util.Locale;

/**
 * The <code>JSON_REMOVE</code> function.
 */
public class SqlJsonRemoveFunction extends SqlFunction {
  public SqlJsonRemoveFunction() {
    super("JSON_REMOVE", SqlKind.OTHER_FUNCTION, ReturnTypes.VARCHAR_2000, null,
        OperandTypes.VARIADIC, SqlFunctionCategory.SYSTEM);
  }

  @Override public SqlOperandCountRange getOperandCountRange() {
    return SqlOperandCountRanges.from(2);
  }

  @Override protected void checkOperandCount(SqlValidator validator,
      SqlOperandTypeChecker argType, SqlCall call) {
    assert call.operandCount() >= 2;
  }

  @Override public String getSignatureTemplate(int operandsCount) {
    assert operandsCount >= 2;
    final StringBuilder sb = new StringBuilder();
    sb.append("{0}(");
    for (int i = 1; i < operandsCount; i++) {
      sb.append(String.format(Locale.ROOT, "{%d} ", i + 1));
    }
    sb.append("{1})");
    return sb.toString();
  }

  @Override public void unparse(SqlWriter writer, SqlCall call, int leftPrec,
      int rightPrec) {
    assert call.operandCount() >= 2;
    final SqlWriter.Frame frame = writer.startFunCall(getName());
    call.operand(0).unparse(writer, leftPrec, rightPrec);
    SqlWriter.Frame listFrame = writer.startList("", "");
    writer.sep(",");
    for (int i = 1; i < call.operandCount(); i++) {
      writer.sep(",");
      call.operand(i).unparse(writer, leftPrec, rightPrec);
    }
    writer.endList(listFrame);
    writer.endFunCall(frame);
  }
}

// End SqlJsonRemoveFunction.java
