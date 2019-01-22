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
package org.apache.calcite.test.fuzzer;

import org.apache.calcite.adapter.java.JavaTypeFactory;
import org.apache.calcite.rel.type.RelDataType;
import org.apache.calcite.rex.RexBuilder;
import org.apache.calcite.rex.RexNode;
import org.apache.calcite.sql.SqlOperator;
import org.apache.calcite.sql.fun.SqlStdOperatorTable;
import org.apache.calcite.sql.type.SqlTypeName;
import org.apache.calcite.test.RexProgramBuilderBase;

import java.util.ArrayList;
import java.util.List;
import java.util.Random;
import java.util.function.Function;

/**
 * Generates random {@link RexNode} instances for tests.
 */
public class RexFuzzer extends RexProgramBuilderBase {
  private static final int MAX_VARS = 2;

  private static final SqlOperator[] BOOL_TO_BOOL = {
      SqlStdOperatorTable.NOT,
      SqlStdOperatorTable.IS_TRUE,
      SqlStdOperatorTable.IS_FALSE,
      SqlStdOperatorTable.IS_NOT_TRUE,
      SqlStdOperatorTable.IS_NOT_FALSE,
  };

  private static final SqlOperator[] ANY_TO_BOOL = {
      SqlStdOperatorTable.IS_NULL,
      SqlStdOperatorTable.IS_NOT_NULL,
      SqlStdOperatorTable.IS_UNKNOWN,
      SqlStdOperatorTable.IS_NOT_UNKNOWN,
  };

  private static final SqlOperator[] COMPARABLE_TO_BOOL = {
      SqlStdOperatorTable.EQUALS,
      SqlStdOperatorTable.NOT_EQUALS,
      SqlStdOperatorTable.GREATER_THAN,
      SqlStdOperatorTable.GREATER_THAN_OR_EQUAL,
      SqlStdOperatorTable.LESS_THAN,
      SqlStdOperatorTable.LESS_THAN_OR_EQUAL,
      SqlStdOperatorTable.IS_DISTINCT_FROM,
      SqlStdOperatorTable.IS_NOT_DISTINCT_FROM,
  };

  private static final SqlOperator[] BOOL_TO_BOOL_MULTI_ARG = {
      SqlStdOperatorTable.OR,
      SqlStdOperatorTable.AND,
      SqlStdOperatorTable.COALESCE,
  };

  private static final SqlOperator[] ANY_SAME_TYPE_MULTI_ARG = {
      SqlStdOperatorTable.COALESCE,
  };

  private static final SqlOperator[] NUMERIC_TO_NUMERIC = {
      SqlStdOperatorTable.PLUS,
      SqlStdOperatorTable.MINUS,
      SqlStdOperatorTable.MULTIPLY,
      // Divide by zero is not allowed, so we do not generate divide
//      SqlStdOperatorTable.DIVIDE,
//      SqlStdOperatorTable.DIVIDE_INTEGER,
  };

  private static final SqlOperator[] UNARY_NUMERIC = {
      SqlStdOperatorTable.UNARY_MINUS,
      SqlStdOperatorTable.UNARY_PLUS,
  };


  private static final int[] INT_VALUES = {-1, 0, 1, 100500};

  private final RelDataType intType;
  private final RelDataType nullableIntType;

  /**
   * Generates randomized {@link RexNode}.
   *
   * @param rexBuilder  builder to be used to create nodes
   * @param typeFactory type factory
   */
  public RexFuzzer(RexBuilder rexBuilder, JavaTypeFactory typeFactory) {
    setUp();
    this.rexBuilder = rexBuilder;
    this.typeFactory = typeFactory;

    intType = typeFactory.createSqlType(SqlTypeName.INTEGER);
    nullableIntType = typeFactory.createTypeWithNullability(intType, true);
  }

  public RexNode getExpression(Random r, int depth) {
    return getComparableExpression(r, depth);
  }

  private RexNode fuzzOperator(Random r, SqlOperator[] operators, RexNode... args) {
    return rexBuilder.makeCall(operators[r.nextInt(operators.length)], args);
  }

  private RexNode fuzzOperator(Random r, SqlOperator[] operators, int length,
      Function<Random, RexNode> factory) {
    List<RexNode> args = new ArrayList<>(length);
    for (int i = 0; i < length; i++) {
      args.add(factory.apply(r));
    }
    return rexBuilder.makeCall(operators[r.nextInt(operators.length)], args);
  }

  public RexNode getComparableExpression(Random r, int depth) {
    int v = r.nextInt(2);
    switch (v) {
    case 0:
      return getBoolExpression(r, depth);
    case 1:
      return getIntExpression(r, depth);
    }
    throw new AssertionError("should not reach here");
  }

  public RexNode getSimpleBool(Random r) {
    int v = r.nextInt(2);
    switch (v) {
    case 0:
      boolean nullable = r.nextBoolean();
      int field = r.nextInt(MAX_VARS);
      return nullable ? vBool(field) : vBoolNotNull(field);
    case 1:
      return r.nextBoolean() ? trueLiteral : falseLiteral;
    case 2:
      return nullBool;
    }
    throw new AssertionError("should not reach here");
  }

  public RexNode getBoolExpression(Random r, int depth) {
    int v = depth <= 0 ? 0 : r.nextInt(7);
    switch (v) {
    case 0:
      return getSimpleBool(r);
    case 1:
      return fuzzOperator(r, ANY_TO_BOOL, getExpression(r, depth - 1));
    case 2:
      return fuzzOperator(r, BOOL_TO_BOOL, getBoolExpression(r, depth - 1));
    case 3:
      return fuzzOperator(r, COMPARABLE_TO_BOOL, getBoolExpression(r, depth - 1),
          getBoolExpression(r, depth - 1));
    case 4:
      return fuzzOperator(r, COMPARABLE_TO_BOOL, getIntExpression(r, depth - 1),
          getIntExpression(r, depth - 1));
    case 5:
      return fuzzOperator(r, BOOL_TO_BOOL_MULTI_ARG, r.nextInt(3) + 2,
          x -> getBoolExpression(x, depth - 1));
    case 6:
      return fuzzCase(r, depth - 1,
          x -> getBoolExpression(x, depth - 1));
    }
    throw new AssertionError("should not reach here");
  }

  public RexNode getSimpleInt(Random r) {
    int v = r.nextInt(3);
    switch (v) {
    case 0:
      boolean nullable = r.nextBoolean();
      int field = r.nextInt(MAX_VARS);
      return nullable ? vInt(field) : vIntNotNull(field);
    case 1: {
      int i = r.nextInt(INT_VALUES.length + 1);
      int val = i < INT_VALUES.length ? INT_VALUES[i] : r.nextInt();
      return rexBuilder.makeLiteral(val, r.nextBoolean() ? intType : nullableIntType, false);
    }
    case 2:
      return nullInt;
    }
    throw new AssertionError("should not reach here");
  }

  public RexNode getIntExpression(Random r, int depth) {
    int v = depth <= 0 ? 0 : r.nextInt(5);
    switch (v) {
    case 0:
      return getSimpleInt(r);
    case 1:
      return fuzzOperator(r, UNARY_NUMERIC, getIntExpression(r, depth - 1));
    case 2:
      return fuzzOperator(r, NUMERIC_TO_NUMERIC, getIntExpression(r, depth - 1),
          getIntExpression(r, depth - 1));
    case 3:
      return fuzzOperator(r, ANY_SAME_TYPE_MULTI_ARG, r.nextInt(3) + 2,
          x -> getIntExpression(x, depth - 1));
    case 4:
      return fuzzCase(r, depth - 1,
          x -> getIntExpression(x, depth - 1));
    }
    throw new AssertionError("should not reach here");
  }

  public RexNode fuzzCase(Random r, int depth, Function<Random, RexNode> resultFactory) {
    boolean caseArgWhen = r.nextBoolean();
    int caseBranches = 1 + (depth <= 0 ? 0 : r.nextInt(3));
    List<RexNode> args = new ArrayList<>(caseBranches + 1);

    Function<Random, RexNode> exprFactory;
    if (!caseArgWhen) {
      exprFactory = x -> getBoolExpression(x, depth - 1);
    } else {
      int type = r.nextInt(2);
      RexNode arg;
      Function<Random, RexNode> baseExprFactory;
      switch (type) {
      case 0:
        baseExprFactory = x -> getBoolExpression(x, depth - 1);
        break;
      case 1:
        baseExprFactory = x -> getIntExpression(x, depth - 1);
        break;
      default:
        throw new AssertionError("should not reach here: " + type);
      }
      arg = baseExprFactory.apply(r);
      // emulate  case when arg=2 then .. when arg=4 then ...
      exprFactory = x -> eq(arg, baseExprFactory.apply(x));
    }

    for (int i = 0; i < caseBranches; i++) {
      args.add(exprFactory.apply(r)); // when
      args.add(resultFactory.apply(r)); // then
    }
    args.add(resultFactory.apply(r)); // else
    return case_(args);
  }

}

// End RexFuzzer.java
