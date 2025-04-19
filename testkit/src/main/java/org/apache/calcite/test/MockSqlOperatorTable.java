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
import org.apache.calcite.sql.SqlBasicFunction;
import org.apache.calcite.sql.SqlCallBinding;
import org.apache.calcite.sql.SqlFunction;
import org.apache.calcite.sql.SqlFunctionCategory;
import org.apache.calcite.sql.SqlIdentifier;
import org.apache.calcite.sql.SqlKind;
import org.apache.calcite.sql.SqlNode;
import org.apache.calcite.sql.SqlOperandCountRange;
import org.apache.calcite.sql.SqlOperator;
import org.apache.calcite.sql.SqlOperatorBinding;
import org.apache.calcite.sql.SqlOperatorTable;
import org.apache.calcite.sql.SqlTableFunction;
import org.apache.calcite.sql.TableCharacteristic;
import org.apache.calcite.sql.fun.SqlLibrary;
import org.apache.calcite.sql.fun.SqlLibraryOperatorTableFactory;
import org.apache.calcite.sql.fun.SqlStdOperatorTable;
import org.apache.calcite.sql.parser.SqlParserPos;
import org.apache.calcite.sql.type.OperandTypes;
import org.apache.calcite.sql.type.ReturnTypes;
import org.apache.calcite.sql.type.SqlOperandCountRanges;
import org.apache.calcite.sql.type.SqlOperandMetadata;
import org.apache.calcite.sql.type.SqlReturnTypeInference;
import org.apache.calcite.sql.type.SqlTypeFamily;
import org.apache.calcite.sql.type.SqlTypeName;
import org.apache.calcite.sql.type.SqlTypeUtil;
import org.apache.calcite.sql.type.TableFunctionReturnTypeInference;
import org.apache.calcite.sql.util.ChainedSqlOperatorTable;
import org.apache.calcite.sql.util.SqlOperatorTables;
import org.apache.calcite.sql.validate.SqlValidator;
import org.apache.calcite.util.Optionality;

import com.google.common.collect.ImmutableList;
import com.google.common.collect.ImmutableMap;
import com.google.common.collect.Iterables;
import com.google.common.collect.Lists;

import org.checkerframework.checker.nullness.qual.Nullable;

import java.util.List;
import java.util.Map;

/**
 * Mock operator table for testing purposes. Contains the standard SQL operator
 * table, plus a list of operators.
 */
public class MockSqlOperatorTable extends ChainedSqlOperatorTable {
  /** Internal constructor; call {@link #standard()},
   * {@link #of(SqlOperatorTable)},
   * {@link #plus(Iterable)}, or
   * {@link #extend()}. */
  private MockSqlOperatorTable(SqlOperatorTable parentTable) {
    super(ImmutableList.of(parentTable));
  }

  /** Returns the operator table that contains only the standard operators. */
  public static MockSqlOperatorTable standard() {
    return of(SqlStdOperatorTable.instance());
  }

  /** Returns a mock operator table based on the given operator table. */
  public static MockSqlOperatorTable of(SqlOperatorTable operatorTable) {
    return new MockSqlOperatorTable(operatorTable);
  }

  /** Returns this table with a few mock operators added. */
  public MockSqlOperatorTable extend() {
    // Don't use anonymous inner classes. They can't be instantiated
    // using reflection when we are deserializing from JSON.
    final SqlOperatorTable parentTable = Iterables.getOnlyElement(tableList);
    return new MockSqlOperatorTable(
        SqlOperatorTables.chain(parentTable,
            SqlOperatorTables.of(new RampFunction(),
                new DedupFunction(),
                new TableFunctionReturnTableFunction(),
                new MyFunction(),
                new MyAvgAggFunction(),
                new RowFunction(),
                new NotATableFunction(),
                new BadTableFunction(),
                new StructuredFunction(),
                new CompositeFunction(),
                new ScoreTableFunction(),
                new TopNTableFunction(),
                new SimilarlityTableFunction(),
                new InvalidTableFunction(),
                new CompareStringsOrNumericValues(),
                HIGHER_ORDER_FUNCTION,
                HIGHER_ORDER_FUNCTION2)));
  }

  /** Adds a library set. */
  public MockSqlOperatorTable plus(Iterable<SqlLibrary> librarySet) {
    final SqlOperatorTable parentTable = Iterables.getOnlyElement(tableList);
    return new MockSqlOperatorTable(
        SqlOperatorTables.chain(parentTable,
            SqlLibraryOperatorTableFactory.INSTANCE
                .getOperatorTable(librarySet)));
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

  /** "DYNTYPE" user-defined table function. */
  public static class DynamicTypeFunction extends SqlFunction
      implements SqlTableFunction {
    public DynamicTypeFunction() {
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

    @Override public RelDataType inferReturnType(SqlOperatorBinding opBinding) {
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

  /** "TFRT" user-defined table function. */
  public static class TableFunctionReturnTableFunction extends SqlFunction
      implements SqlTableFunction {
    TableFunctionReturnTypeInference inference;

    public TableFunctionReturnTableFunction() {
      super("TFRT",
          SqlKind.OTHER_FUNCTION,
          null,
          null,
          OperandTypes.VARIADIC,
          SqlFunctionCategory.USER_DEFINED_FUNCTION);
    }

    @Override public RelDataType inferReturnType(SqlOperatorBinding opBinding) {
      inference =
          new TableFunctionReturnTypeInference(factory -> factory.builder()
              .add("NAME", SqlTypeName.CURSOR)
              .build(),
          Lists.newArrayList("NAME"), true);
      inference.inferReturnType(opBinding);
      return opBinding.getTypeFactory().createSqlType(SqlTypeName.CURSOR);
    }

    @Override public @Nullable SqlReturnTypeInference getReturnTypeInference() {
      return inference;
    }

    @Override public SqlReturnTypeInference getRowTypeInference() {
      return inference;
    }
  }

  /** "Score" user-defined table function. First parameter is input table
   * with row semantics. */
  public static class ScoreTableFunction extends SqlFunction
      implements SqlTableFunction {

    private final Map<Integer, TableCharacteristic> tableParams =
        ImmutableMap.of(
            0,
            TableCharacteristic
                .builder(TableCharacteristic.Semantics.ROW)
                .passColumnsThrough().build());

    public ScoreTableFunction() {
      super("SCORE",
          SqlKind.OTHER_FUNCTION,
          ReturnTypes.CURSOR,
          null,
          new OperandMetadataImpl(),
          SqlFunctionCategory.USER_DEFINED_TABLE_FUNCTION);
    }

    private static RelDataType inferRowType(SqlOperatorBinding opBinding) {
      final RelDataTypeFactory typeFactory = opBinding.getTypeFactory();
      final RelDataType inputRowType = opBinding.getOperandType(0);
      final RelDataType bigintType =
          typeFactory.createSqlType(SqlTypeName.BIGINT);
      return typeFactory.builder()
          .kind(inputRowType.getStructKind())
          .addAll(inputRowType.getFieldList())
          .add("SCORE_VALUE", bigintType).nullable(true)
          .build();
    }

    @Override public SqlReturnTypeInference getRowTypeInference() {
      return ScoreTableFunction::inferRowType;
    }

    @Override public TableCharacteristic tableCharacteristic(int ordinal) {
      return tableParams.get(ordinal);
    }

    @Override public boolean argumentMustBeScalar(int ordinal) {
      return !tableParams.containsKey(ordinal);
    }

    /** Operand type checker for {@link ScoreTableFunction}. */
    private static class OperandMetadataImpl implements SqlOperandMetadata {

      @Override public List<RelDataType> paramTypes(RelDataTypeFactory typeFactory) {
        return ImmutableList.of(typeFactory.createSqlType(SqlTypeName.ANY));
      }

      @Override public List<String> paramNames() {
        return ImmutableList.of("DATA");
      }

      @Override public boolean checkOperandTypes(
          SqlCallBinding callBinding, boolean throwOnFailure) {
        return true;
      }

      @Override public SqlOperandCountRange getOperandCountRange() {
        return SqlOperandCountRanges.of(1);
      }

      @Override public String getAllowedSignatures(SqlOperator op, String opName) {
        return "Score(TABLE table_name)";
      }
    }
  }

  /** "TopN" user-defined table function. First parameter is input table
   * with set semantics. */
  public static class TopNTableFunction extends SqlFunction
      implements SqlTableFunction {

    private final Map<Integer, TableCharacteristic> tableParams =
        ImmutableMap.of(
            0,
            TableCharacteristic
                .builder(TableCharacteristic.Semantics.SET)
                .passColumnsThrough()
                .pruneIfEmpty()
                .build());

    public TopNTableFunction() {
      super("TOPN",
          SqlKind.OTHER_FUNCTION,
          ReturnTypes.CURSOR,
          null,
          new OperandMetadataImpl(),
          SqlFunctionCategory.USER_DEFINED_TABLE_FUNCTION);
    }

    private static RelDataType inferRowType(SqlOperatorBinding opBinding) {
      final RelDataTypeFactory typeFactory = opBinding.getTypeFactory();
      final RelDataType inputRowType = opBinding.getOperandType(0);
      final RelDataType bigintType =
          typeFactory.createSqlType(SqlTypeName.BIGINT);
      return typeFactory.builder()
          .kind(inputRowType.getStructKind())
          .addAll(inputRowType.getFieldList())
          .add("RANK_NUMBER", bigintType).nullable(true)
          .build();
    }

    @Override public SqlReturnTypeInference getRowTypeInference() {
      return TopNTableFunction::inferRowType;
    }

    @Override public TableCharacteristic tableCharacteristic(int ordinal) {
      return tableParams.get(ordinal);
    }

    @Override public boolean argumentMustBeScalar(int ordinal) {
      return !tableParams.containsKey(ordinal);
    }

    /** Operand type checker for {@link TopNTableFunction}. */
    private static class OperandMetadataImpl implements SqlOperandMetadata {

      @Override public List<RelDataType> paramTypes(RelDataTypeFactory typeFactory) {
        return ImmutableList.of(
            typeFactory.createSqlType(SqlTypeName.ANY),
            typeFactory.createSqlType(SqlTypeName.INTEGER));
      }

      @Override public List<String> paramNames() {
        return ImmutableList.of("DATA", "COL");
      }

      @Override public boolean checkOperandTypes(
          SqlCallBinding callBinding, boolean throwOnFailure) {
        final SqlNode operand1 = callBinding.operand(1);
        final SqlValidator validator = callBinding.getValidator();
        final RelDataType type = validator.getValidatedNodeType(operand1);
        if (!SqlTypeUtil.isIntType(type)) {
          if (throwOnFailure) {
            throw callBinding.newValidationSignatureError();
          } else {
            return false;
          }
        } else {
          return true;
        }
      }

      @Override public SqlOperandCountRange getOperandCountRange() {
        return SqlOperandCountRanges.of(2);
      }

      @Override public String getAllowedSignatures(SqlOperator op, String opName) {
        return "TopN(TABLE table_name, BIGINT rows)";
      }
    }
  }

  /** Similarity performs an analysis on two data sets, which are both tables
   * of two columns, treated as the x and y axes of a graph. It has two input
   * tables with set semantics. */
  public static class SimilarlityTableFunction extends SqlFunction
      implements SqlTableFunction {

    private final Map<Integer, TableCharacteristic> tableParams =
        ImmutableMap.of(
            0,
            TableCharacteristic
                .builder(TableCharacteristic.Semantics.SET)
                .build(),
            1,
            TableCharacteristic
                .builder(TableCharacteristic.Semantics.SET)
                .build());

    public SimilarlityTableFunction() {
      super("SIMILARLITY",
          SqlKind.OTHER_FUNCTION,
          ReturnTypes.CURSOR,
          null,
          new OperandMetadataImpl(),
          SqlFunctionCategory.USER_DEFINED_TABLE_FUNCTION);
    }

    @Override public SqlReturnTypeInference getRowTypeInference() {
      return opBinding -> opBinding.getTypeFactory().builder()
          .add("VAL", SqlTypeName.DECIMAL, 5, 2)
          .build();
    }

    @Override public TableCharacteristic tableCharacteristic(int ordinal) {
      return tableParams.get(ordinal);
    }

    @Override public boolean argumentMustBeScalar(int ordinal) {
      return !tableParams.containsKey(ordinal);
    }


    /** Operand type checker for {@link TopNTableFunction}. */
    private static class OperandMetadataImpl implements SqlOperandMetadata {

      @Override public List<RelDataType> paramTypes(RelDataTypeFactory typeFactory) {
        return ImmutableList.of(
            typeFactory.createSqlType(SqlTypeName.ANY),
            typeFactory.createSqlType(SqlTypeName.ANY));
      }

      @Override public List<String> paramNames() {
        return ImmutableList.of("LTABLE", "RTABLE");
      }

      @Override public boolean checkOperandTypes(
          SqlCallBinding callBinding, boolean throwOnFailure) {
        return true;
      }

      @Override public SqlOperandCountRange getOperandCountRange() {
        return SqlOperandCountRanges.of(2);
      }

      @Override public String getAllowedSignatures(SqlOperator op, String opName) {
        return "SIMILARITY(TABLE table_name, TABLE table_name)";
      }
    }
  }

  /** Invalid user-defined table function with multiple input tables with
   * row semantics. */
  public static class InvalidTableFunction extends SqlFunction
      implements SqlTableFunction {

    private final Map<Integer, TableCharacteristic> tableParams =
        ImmutableMap.of(
            0,
            TableCharacteristic
                .builder(TableCharacteristic.Semantics.ROW)
                .passColumnsThrough()
                .build(),
            1,
            TableCharacteristic
                .builder(TableCharacteristic.Semantics.ROW)
                .passColumnsThrough()
                .build());

    public InvalidTableFunction() {
      super("INVALID",
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

    @Override public TableCharacteristic tableCharacteristic(int ordinal) {
      return tableParams.get(ordinal);
    }

    @Override public boolean argumentMustBeScalar(int ordinal) {
      return !tableParams.containsKey(ordinal);
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
          SqlFunctionCategory.USER_DEFINED_FUNCTION);
    }

    @Override public RelDataType inferReturnType(SqlOperatorBinding opBinding) {
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
      super("SPLIT", new SqlIdentifier("SPLIT", SqlParserPos.ZERO),
          SqlKind.OTHER_FUNCTION, null, null,
          OperandTypes.family(SqlTypeFamily.STRING, SqlTypeFamily.STRING),
          SqlFunctionCategory.USER_DEFINED_FUNCTION);
    }

    @Override public RelDataType inferReturnType(SqlOperatorBinding opBinding) {
      final RelDataTypeFactory typeFactory =
          opBinding.getTypeFactory();
      return typeFactory.createArrayType(typeFactory.createSqlType(SqlTypeName.VARCHAR), -1);
    }

  }

  /**
   * "MAP" user-defined function. This function return map type
   * in order to reproduce the throws of CALCITE-4895.
   */
  public static class MapFunction extends SqlFunction {

    public MapFunction() {
      super("MAP", new SqlIdentifier("MAP", SqlParserPos.ZERO),
          SqlKind.OTHER_FUNCTION, null, null,
          OperandTypes.family(SqlTypeFamily.STRING, SqlTypeFamily.STRING),
          SqlFunctionCategory.USER_DEFINED_FUNCTION);
    }

    @Override public RelDataType inferReturnType(SqlOperatorBinding opBinding) {
      final RelDataTypeFactory typeFactory =
          opBinding.getTypeFactory();
      return typeFactory.createMapType(typeFactory.createSqlType(SqlTypeName.VARCHAR),
          typeFactory.createSqlType(SqlTypeName.VARCHAR));
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
      super("STRUCTURED_FUNC",
          new SqlIdentifier("STRUCTURED_FUNC", SqlParserPos.ZERO),
          SqlKind.OTHER_FUNCTION, null, null, OperandTypes.NILADIC,
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

  /** "COMPOSITE" user-defined scalar function. */
  public static class CompositeFunction extends SqlFunction {
    public CompositeFunction() {
      super("COMPOSITE",
          new SqlIdentifier("COMPOSITE", SqlParserPos.ZERO),
          SqlKind.OTHER_FUNCTION,
          null,
          null,
          OperandTypes.variadic(SqlOperandCountRanges.from(1))
              .or(OperandTypes.variadic(SqlOperandCountRanges.from(2))),
          SqlFunctionCategory.USER_DEFINED_FUNCTION);
    }

    @Override public RelDataType inferReturnType(SqlOperatorBinding opBinding) {
      final RelDataTypeFactory typeFactory =
          opBinding.getTypeFactory();
      return typeFactory.createSqlType(SqlTypeName.BIGINT);
    }
  }

  /**
   * "COMPARE_STRINGS_OR_NUMERIC_VALUES" is a user-defined function whose arguments can be either
   * two strings or two numeric values of the same type.
   */
  public static class CompareStringsOrNumericValues extends SqlFunction {
    public CompareStringsOrNumericValues() {
      super("COMPARE_STRINGS_OR_NUMERIC_VALUES",
          new SqlIdentifier("COMPARE_STRINGS_OR_NUMERIC_VALUES", SqlParserPos.ZERO),
          SqlKind.OTHER_FUNCTION,
          null,
          null,
          OperandTypes.STRING_SAME_SAME.or(
              OperandTypes.NUMERIC_NUMERIC.and(OperandTypes.SAME_SAME)),
          SqlFunctionCategory.USER_DEFINED_FUNCTION);
    }

    @Override public RelDataType inferReturnType(SqlOperatorBinding opBinding) {
      return opBinding.getOperandType(0);
    }
  }

  private static final SqlFunction HIGHER_ORDER_FUNCTION =
      SqlBasicFunction.create("HIGHER_ORDER_FUNCTION",
          ReturnTypes.ARG0,
          OperandTypes.sequence("HIGHER_ORDER_FUNCTION(INTEGER, FUNCTION(STRING, ANY) -> NUMERIC)",
              OperandTypes.family(SqlTypeFamily.INTEGER),
              OperandTypes.function(
                  SqlTypeFamily.NUMERIC, SqlTypeFamily.STRING, SqlTypeFamily.ANY)),
          SqlFunctionCategory.SYSTEM);

  private static final SqlFunction HIGHER_ORDER_FUNCTION2 =
      SqlBasicFunction.create("HIGHER_ORDER_FUNCTION2",
          ReturnTypes.ARG0,
          OperandTypes.sequence("HIGHER_ORDER_FUNCTION(INTEGER, FUNCTION() -> NUMERIC)",
              OperandTypes.family(SqlTypeFamily.INTEGER),
              OperandTypes.function(SqlTypeFamily.NUMERIC)),
          SqlFunctionCategory.SYSTEM);
}
