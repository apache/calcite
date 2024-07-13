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

import org.apache.calcite.rel.type.RelDataType;
import org.apache.calcite.sql.SqlBasicCall;
import org.apache.calcite.sql.SqlBinaryOperator;
import org.apache.calcite.sql.SqlCallBinding;
import org.apache.calcite.sql.SqlKind;
import org.apache.calcite.sql.SqlLiteral;
import org.apache.calcite.sql.parser.SqlParserPos;
import org.apache.calcite.sql.test.SqlTestFactory;
import org.apache.calcite.sql.validate.EmptyScope;
import org.apache.calcite.sql.validate.SqlValidator;
import org.apache.calcite.sql.validate.SqlValidatorImpl;

import com.google.common.collect.ImmutableList;

import org.junit.jupiter.api.Test;

import java.util.List;

import static org.junit.jupiter.api.Assertions.assertEquals;

/**
 * Test of {@link org.apache.calcite.sql.type.SqlOperandTypeChecker}.
 */
public class SqlOperandTypeCheckerTest {

  /**
   * Test case for
   * <a href="https://issues.apache.org/jira/browse/CALCITE-5532">[CALCITE-5532]
   * CompositeOperandTypeChecker should check operands without type coercion first</a>.
   */
  @Test void testCompositeOperandTypeCheckWithoutTypeCoercionFirst() {
    SqlValidator validator = SqlTestFactory.INSTANCE.createValidator();

    SqlSingleOperandTypeChecker operandTypeChecker = OperandTypes.or(
        OperandTypes.family(SqlTypeFamily.STRING, SqlTypeFamily.STRING)
            .and(OperandTypes.SAME_SAME),
        OperandTypes.family(SqlTypeFamily.INTEGER, SqlTypeFamily.INTEGER)
            .and(OperandTypes.SAME_SAME));

    SqlBinaryOperator op = new SqlBinaryOperator(
        "~",
        SqlKind.OTHER,
        60,
        true,
        null,
        null,
        null);

    List<SqlLiteral> args = ImmutableList.of(
        SqlLiteral.createExactNumeric("20", SqlParserPos.ZERO),
        SqlLiteral.createExactNumeric("30", SqlParserPos.ZERO));

    SqlCallBinding binding = new SqlCallBinding(
        validator,
        new EmptyScope((SqlValidatorImpl) validator),
        new SqlBasicCall(op, args, SqlParserPos.ZERO));
    List<RelDataType> typesBeforeChecking =
        ImmutableList.of(binding.getOperandType(0), binding.getOperandType(1));

    operandTypeChecker.checkOperandTypes(binding, false);

    assertEquals(typesBeforeChecking.get(0), binding.getOperandType(0));
    assertEquals(typesBeforeChecking.get(1), binding.getOperandType(1));
  }
}
