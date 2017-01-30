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

import org.apache.calcite.avatica.util.TimeUnit;
import org.apache.calcite.sql.SqlCall;
import org.apache.calcite.sql.SqlCallBinding;
import org.apache.calcite.sql.SqlFunction;
import org.apache.calcite.sql.SqlFunctionCategory;
import org.apache.calcite.sql.SqlIntervalQualifier;
import org.apache.calcite.sql.SqlKind;
import org.apache.calcite.sql.SqlNode;
import org.apache.calcite.sql.SqlOperandCountRange;
import org.apache.calcite.sql.parser.SqlParserPos;
import org.apache.calcite.sql.type.InferTypes;
import org.apache.calcite.sql.type.OperandTypes;
import org.apache.calcite.sql.type.ReturnTypes;
import org.apache.calcite.sql.type.SqlOperandCountRanges;
import org.apache.calcite.sql.validate.SqlValidator;

import java.util.List;

/**
 * SqlDatePartFunction represents the SQL:1999 standard {@code YEAR},
 * {@code QUARTER}, {@code MONTH} and {@code DAY} functions.
 */
public class SqlDatePartFunction extends SqlFunction {
  //~ Constructors -----------------------------------------------------------
  private final TimeUnit timeUnit;

  public SqlDatePartFunction(String name, TimeUnit timeUnit) {
    super(name,
        SqlKind.OTHER,
        ReturnTypes.BIGINT_NULLABLE,
        InferTypes.FIRST_KNOWN,
        OperandTypes.DATETIME,
        SqlFunctionCategory.TIMEDATE);
    this.timeUnit = timeUnit;
  }

  //~ Methods ----------------------------------------------------------------

  @Override public SqlNode rewriteCall(SqlValidator validator, SqlCall call) {
    final List<SqlNode> operands = call.getOperandList();
    final SqlParserPos pos = call.getParserPosition();
    return SqlStdOperatorTable.EXTRACT.createCall(pos,
        new SqlIntervalQualifier(timeUnit, null, SqlParserPos.ZERO),
        operands.get(0));
  }

  public SqlOperandCountRange getOperandCountRange() {
    return SqlOperandCountRanges.of(1);
  }

  public String getSignatureTemplate(int operandsCount) {
    assert 1 == operandsCount;
    return "{0}({1})";
  }

  public boolean checkOperandTypes(SqlCallBinding callBinding,
      boolean throwOnFailure) {
    return OperandTypes.DATETIME.checkSingleOperandType(callBinding,
        callBinding.operand(0), 0, throwOnFailure);
  }
}

// End SqlDatePartFunction.java
