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

import org.apache.calcite.sql.SqlAggFunction;
import org.apache.calcite.sql.SqlCall;
import org.apache.calcite.sql.SqlCallBinding;
import org.apache.calcite.sql.SqlFunctionCategory;
import org.apache.calcite.sql.SqlKind;
import org.apache.calcite.sql.SqlLiteral;
import org.apache.calcite.sql.SqlNode;
import org.apache.calcite.sql.SqlNodeList;
import org.apache.calcite.sql.SqlOperandCountRange;
import org.apache.calcite.sql.parser.SqlParserPos;
import org.apache.calcite.sql.type.FamilyOperandTypeChecker;
import org.apache.calcite.sql.type.OperandTypes;
import org.apache.calcite.sql.type.ReturnTypes;
import org.apache.calcite.sql.type.SqlOperandCountRanges;
import org.apache.calcite.sql.type.SqlOperandTypeChecker;
import org.apache.calcite.sql.validate.SqlValidator;
import org.apache.calcite.util.Optionality;

import java.util.List;

/**
 * Definition of the SQL <code>COUNTIF</code> aggregation function.
 *
 * <p><code>COUNTIF</code> is an aggregator which returns the number of rows which
 * fulfil its boolean expression.
 */
public class SqlCountIfAggFunction extends SqlAggFunction {
  //~ Constructors -----------------------------------------------------------

  public SqlCountIfAggFunction(String name) {
    this(name, OperandTypes.BOOLEAN);
  }

  public SqlCountIfAggFunction(String name,
      SqlOperandTypeChecker sqlOperandTypeChecker) {
    super(name, null, SqlKind.COUNT_IF, ReturnTypes.BIGINT, null,
        sqlOperandTypeChecker, SqlFunctionCategory.NUMERIC, false, false,
        Optionality.FORBIDDEN);
  }

  //~ Methods ----------------------------------------------------------------

  @Override public SqlNode rewriteCall(SqlValidator validator, SqlCall call) {
    List<SqlNode> operands = call.getOperandList();
    SqlParserPos pos = call.getParserPosition();

    checkOperandCount(
        validator,
        getOperandTypeChecker(),
        call);

    SqlNodeList whenList = new SqlNodeList(pos);
    SqlNodeList thenList = new SqlNodeList(pos);
    whenList.add(operands.get(0));
    thenList.add(SqlLiteral.createExactNumeric("1", SqlParserPos.ZERO));
    return SqlCase.createSwitched(pos, null, whenList, thenList,
        SqlLiteral.createExactNumeric("0", SqlParserPos.ZERO));
  }

  @Override public SqlOperandCountRange getOperandCountRange() {
    return SqlOperandCountRanges.of(1);
  }

  @Override public boolean checkOperandTypes(
      SqlCallBinding callBinding,
      boolean throwOnFailure) {
    final SqlNode node = callBinding.operand(0);
    final FamilyOperandTypeChecker checker = OperandTypes.BOOLEAN;
    return checker.checkSingleOperandType(callBinding, node, 0,
        throwOnFailure);
  }
}
