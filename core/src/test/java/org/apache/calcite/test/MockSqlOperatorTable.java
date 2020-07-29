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
package org.apache.calcite.test;

import org.apache.calcite.rel.type.RelDataType;
import org.apache.calcite.rel.type.RelDataTypeFactory;
import org.apache.calcite.sql.SqlAggFunction;
import org.apache.calcite.sql.SqlFunction;
import org.apache.calcite.sql.SqlFunctionCategory;
import org.apache.calcite.sql.SqlIdentifier;
import org.apache.calcite.sql.SqlKind;
import org.apache.calcite.sql.SqlOperator;
import org.apache.calcite.sql.SqlOperatorBinding;
import org.apache.calcite.sql.SqlOperatorTable;
import org.apache.calcite.sql.SqlTableFunction;
import org.apache.calcite.sql.parser.SqlParserPos;
import org.apache.calcite.sql.type.OperandTypes;
import org.apache.calcite.sql.type.ReturnTypes;
import org.apache.calcite.sql.type.SqlReturnTypeInference;
import org.apache.calcite.sql.type.SqlTypeFamily;
import org.apache.calcite.sql.type.SqlTypeName;
import org.apache.calcite.sql.util.ChainedSqlOperatorTable;
import org.apache.calcite.sql.util.ListSqlOperatorTable;
import org.apache.calcite.util.Optionality;

import com.google.common.collect.ImmutableList;


/**
 * Mock operator table for testing purposes. Contains the standard SQL operator
 * table, plus a list of operators.
 */
public class MockSqlOperatorTable extends ChainedSqlOperatorTable {
  private final ListSqlOperatorTable listOpTab;

  public MockSqlOperatorTable(SqlOperatorTable parentTable) {
    super(ImmutableList.of(parentTable, new ListSqlOperatorTable()));
    listOpTab = (ListSqlOperatorTable) tableList.get(1);
  }

  /**
   * Adds an operator to this table.
   */
  public void addOperator(SqlOperator op) {
    listOpTab.add(op);
  }

  public static void addRamp(MockSqlOperatorTable opTab) {
    // Don't use anonymous inner classes. They can't be instantiated
    // using reflection when we are deserializing from JSON.
    opTab.addOperator(new RampFunction());
    opTab.addOperator(new DedupFunction());
    opTab.addOperator(new MyFunction());
    opTab.addOperator(new MyAvgAggFunction());
    opTab.addOperator(new RowFunction());
    opTab.addOperator(new NotATableFunction());
    opTab.addOperator(new BadTableFunction());
    opTab.addOperator(new StructuredFunction());
  }

  /** "RAMP" user-defined table function. */
  public static class RampFunction extends SqlFunction
      implements SqlTableFunction {
    public RampFunction() {
      super("RAMP",
          SqlKind.OTHER_FUNCTION,
          ReturnTypes.CURSOR,
          null,
          OperandTypes.NUMERIC,
          SqlFunctionCategory.USER_DEFINED_TABLE_FUNCTION);
    }

    @Override public SqlReturnTypeInference getRowTypeInference() {
      return opBinding -> opBinding.getTypeFactory().builder()
          .add("I", SqlTypeName.INTEGER)
          .build();
    }
  }

  /** Not valid as a table function, even though it returns CURSOR, because
   * it does not implement {@link SqlTableFunction}. */
  public static class NotATableFunction extends SqlFunction {
    public NotATableFunction() {
      super("BAD_RAMP",
          SqlKind.OTHER_FUNCTION,
          ReturnTypes.CURSOR,
          null,
          OperandTypes.NUMERIC,
          SqlFunctionCategory.USER_DEFINED_FUNCTION);
    }
  }

  /** Another bad table function: declares itself as a table function but does
   * not return CURSOR. */
  public static class BadTableFunction extends SqlFunction
      implements SqlTableFunction {
    public BadTableFunction() {
      super("BAD_TABLE_FUNCTION",
          SqlKind.OTHER_FUNCTION,
          null,
          null,
          OperandTypes.NUMERIC,
          SqlFunctionCategory.USER_DEFINED_TABLE_FUNCTION);
    }

    public RelDataType inferReturnType(SqlOperatorBinding opBinding) {
      // This is wrong. A table function should return CURSOR.
      return opBinding.getTypeFactory().builder()
          .add("I", SqlTypeName.INTEGER)
          .build();
    }

    @Override public SqlReturnTypeInference getRowTypeInference() {
      return this::inferReturnType;
    }
  }

  /** "DEDUP" user-defined table function. */
  public static class DedupFunction extends SqlFunction
      implements SqlTableFunction {
    public DedupFunction() {
      super("DEDUP",
          SqlKind.OTHER_FUNCTION,
          ReturnTypes.CURSOR,
          null,
          OperandTypes.VARIADIC,
          SqlFunctionCategory.USER_DEFINED_TABLE_FUNCTION);
    }

    @Override public SqlReturnTypeInference getRowTypeInference() {
      return opBinding -> opBinding.getTypeFactory().builder()
          .add("NAME", SqlTypeName.VARCHAR, 1024)
          .build();
    }
  }

  /** "MYFUN" user-defined scalar function. */
  public static class MyFunction extends SqlFunction {
    public MyFunction() {
      super("MYFUN",
          new SqlIdentifier("MYFUN", SqlParserPos.ZERO),
          SqlKind.OTHER_FUNCTION,
          null,
          null,
          OperandTypes.NUMERIC,
          null,
          SqlFunctionCategory.USER_DEFINED_FUNCTION);
    }

    public RelDataType inferReturnType(SqlOperatorBinding opBinding) {
      final RelDataTypeFactory typeFactory =
          opBinding.getTypeFactory();
      return typeFactory.createSqlType(SqlTypeName.BIGINT);
    }
  }

  /** "MYAGGFUNC" user-defined aggregate function. This agg function accept one or more arguments
   * in order to reproduce the throws of CALCITE-3929. */
  public static class MyAggFunc extends SqlAggFunction {
    public MyAggFunc() {
      super("myAggFunc", null, SqlKind.OTHER_FUNCTION, ReturnTypes.BIGINT, null,
          OperandTypes.ONE_OR_MORE, SqlFunctionCategory.USER_DEFINED_FUNCTION, false, false,
          Optionality.FORBIDDEN);
    }
  }

  /**
   * "SPLIT" user-defined function. This function return array type
   * in order to reproduce the throws of CALCITE-4062.
   */
  public static class SplitFunction extends SqlFunction {

    public SplitFunction() {
      super("SPLIT", new SqlIdentifier("SPLIT", SqlParserPos.ZERO), SqlKind.OTHER_FUNCTION, null,
          null, OperandTypes.family(SqlTypeFamily.STRING, SqlTypeFamily.STRING), null,
          SqlFunctionCategory.USER_DEFINED_FUNCTION);
    }

    @Override public RelDataType inferReturnType(SqlOperatorBinding opBinding) {
      final RelDataTypeFactory typeFactory =
          opBinding.getTypeFactory();
      return typeFactory.createArrayType(typeFactory.createSqlType(SqlTypeName.VARCHAR), -1);
    }

  }

  /** "MYAGG" user-defined aggregate function. This agg function accept two numeric arguments
   * in order to reproduce the throws of CALCITE-2744. */
  public static class MyAvgAggFunction extends SqlAggFunction {
    public MyAvgAggFunction() {
      super("MYAGG", null, SqlKind.AVG, ReturnTypes.AVG_AGG_FUNCTION,
          null, OperandTypes.family(SqlTypeFamily.NUMERIC, SqlTypeFamily.NUMERIC),
          SqlFunctionCategory.NUMERIC, false, false, Optionality.FORBIDDEN);
    }

    @Override public boolean isDeterministic() {
      return false;
    }
  }

  /** "ROW_FUNC" user-defined table function whose return type is
   * row type with nullable and non-nullable fields. */
  public static class RowFunction extends SqlFunction
      implements SqlTableFunction {
    RowFunction() {
      super("ROW_FUNC", SqlKind.OTHER_FUNCTION, ReturnTypes.CURSOR, null,
          OperandTypes.NILADIC, SqlFunctionCategory.USER_DEFINED_TABLE_FUNCTION);
    }

    private static RelDataType inferRowType(SqlOperatorBinding opBinding) {
      final RelDataTypeFactory typeFactory = opBinding.getTypeFactory();
      final RelDataType bigintType =
          typeFactory.createSqlType(SqlTypeName.BIGINT);
      return typeFactory.builder()
          .add("NOT_NULL_FIELD", bigintType)
          .add("NULLABLE_FIELD", bigintType).nullable(true)
          .build();
    }

    @Override public SqlReturnTypeInference getRowTypeInference() {
      return RowFunction::inferRowType;
    }
  }

  /** "STRUCTURED_FUNC" user-defined function whose return type is structured type. */
  public static class StructuredFunction extends SqlFunction {
    StructuredFunction() {
      super("STRUCTURED_FUNC", new SqlIdentifier("STRUCTURED_FUNC", SqlParserPos.ZERO),
          SqlKind.OTHER_FUNCTION, null, null, OperandTypes.NILADIC, null,
          SqlFunctionCategory.USER_DEFINED_FUNCTION);
    }

    @Override public RelDataType inferReturnType(SqlOperatorBinding opBinding) {
      final RelDataTypeFactory typeFactory = opBinding.getTypeFactory();
      final RelDataType bigintType =
          typeFactory.createSqlType(SqlTypeName.BIGINT);
      final RelDataType varcharType =
          typeFactory.createSqlType(SqlTypeName.VARCHAR, 20);
      return typeFactory.builder()
          .add("F0", bigintType)
          .add("F1", varcharType)
          .build();
    }
  }
}
