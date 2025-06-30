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
package org.apache.calcite.util;

import org.apache.calcite.rel.type.RelDataType;
import org.apache.calcite.rex.RexBuilder;
import org.apache.calcite.rex.RexCall;
import org.apache.calcite.rex.RexNode;
import org.apache.calcite.rex.RexUtil;
import org.apache.calcite.sql.SqlCall;
import org.apache.calcite.sql.SqlCharStringLiteral;
import org.apache.calcite.sql.SqlCollectionTypeNameSpec;
import org.apache.calcite.sql.SqlDataTypeSpec;
import org.apache.calcite.sql.SqlDialect;
import org.apache.calcite.sql.SqlKind;
import org.apache.calcite.sql.SqlLiteral;
import org.apache.calcite.sql.SqlMapTypeNameSpec;
import org.apache.calcite.sql.SqlNode;
import org.apache.calcite.sql.SqlSpecialOperator;
import org.apache.calcite.sql.SqlTypeNameSpec;
import org.apache.calcite.sql.SqlWriter;
import org.apache.calcite.sql.fun.SqlStdOperatorTable;
import org.apache.calcite.sql.fun.SqlTrimFunction;
import org.apache.calcite.sql.parser.SqlParserPos;
import org.apache.calcite.sql.type.ArraySqlType;
import org.apache.calcite.sql.type.MapSqlType;
import org.apache.calcite.sql.type.SqlTypeName;

import static org.apache.calcite.sql.fun.SqlLibraryOperators.REGEXP_REPLACE_3;

import static java.util.Objects.requireNonNull;

/**
 * Utilities used by multiple dialect for RelToSql conversion.
 */
public abstract class RelToSqlConverterUtil {

  /**
   * For usage of TRIM, LTRIM and RTRIM in Hive, see
   * <a href="https://cwiki.apache.org/confluence/display/Hive/LanguageManual+UDF">Hive UDF usage</a>.
   */
  public static void unparseHiveTrim(
      SqlWriter writer,
      SqlCall call,
      int leftPrec,
      int rightPrec) {
    final SqlLiteral valueToTrim = call.operand(1);
    String value =
        requireNonNull(valueToTrim.toValue(),
            () -> "call.operand(1).toValue() for call " + call);
    if (value.matches("\\s+")) {
      unparseTrimWithSpace(writer, call, leftPrec, rightPrec);
    } else {
      // SELECT TRIM(both 'A' from "ABC") -> SELECT REGEXP_REPLACE("ABC", '^(A)*', '')
      final SqlLiteral trimFlag = call.operand(0);
      final SqlCharStringLiteral regexNode =
          createRegexPatternLiteral(call.operand(1), trimFlag);
      final SqlCharStringLiteral blankLiteral =
          SqlLiteral.createCharString("", call.getParserPosition());
      final SqlNode[] trimOperands = new SqlNode[] { call.operand(2), regexNode, blankLiteral };
      final SqlCall regexReplaceCall = REGEXP_REPLACE_3.createCall(SqlParserPos.ZERO, trimOperands);
      regexReplaceCall.unparse(writer, leftPrec, rightPrec);
    }
  }

  /**
   * For usage of TRIM(LEADING 'A' FROM 'ABCA') convert to TRIM, LTRIM and RTRIM,
   * can refer to BigQuery and Presto documents:
   * <a href="https://cloud.google.com/bigquery/docs/reference/standard-sql/functions-and-operators#trim">
   * BigQuery Trim Function</a>,
   * <a href="https://prestodb.io/docs/current/functions/string.html#trim-string-varchar">
   * Presto Trim Function</a>.
   */
  public static void unparseTrimLR(SqlWriter writer, SqlCall call, int leftPrec,
      int rightPrec) {
    final String operatorName;
    SqlLiteral trimFlag = call.operand(0);
    SqlLiteral valueToTrim = call.operand(1);
    switch (trimFlag.getValueAs(SqlTrimFunction.Flag.class)) {
    case LEADING:
      operatorName = "LTRIM";
      break;
    case TRAILING:
      operatorName = "RTRIM";
      break;
    default:
      operatorName = call.getOperator().getName();
      break;
    }
    final SqlWriter.Frame trimFrame = writer.startFunCall(operatorName);
    call.operand(2).unparse(writer, leftPrec, rightPrec);

    // If the trimmed character is a non-space character, add it to the target SQL.
    // eg: TRIM(BOTH 'A' from 'ABCD')
    // Output Query: TRIM('ABC', 'A')
    String value = requireNonNull(valueToTrim.toValue(), "valueToTrim.toValue()");
    if (!value.equals(" ")) {
      writer.literal(",");
      call.operand(1).unparse(writer, leftPrec, rightPrec);
    }
    writer.endFunCall(trimFrame);
  }

  /**
   * Unparses IS TRUE,IS FALSE,IS NOT TRUE and IS NOT FALSE.
   *
   * <p>For example :
   *
   * <blockquote><pre>
   * A IS TRUE &rarr; A IS NOT NUL AND A
   * A IS FALSE &rarr; A IS NOT NUL AND NOT A
   * A IS NOT TRUE &rarr; A IS NUL OR NOT A
   * A IS NOT FALSE &rarr; A IS NUL OR A
   *
   * an exception will be thrown when A is a non-deterministic function such as RAND_INTEGER.
   * </pre></blockquote>
   *
   * @param rexNode rexNode
   */
  public static RexNode unparseIsTrueOrFalse(RexNode rexNode) {
    if (!(rexNode instanceof RexCall)) {
      return rexNode;
    }
    RexCall call = (RexCall) rexNode;
    switch (call.getOperator().getKind()) {
    case IS_FALSE:
      RexNode operandIsFalse = call.operands.get(0);
      if (RexUtil.isDeterministic(operandIsFalse)) {
        // A IS FALSE -> A IS NOT NULL AND NOT A
        RexNode isNotNullFunc =
            RexBuilder.DEFAULT.makeCall(SqlStdOperatorTable.IS_NOT_NULL, operandIsFalse);
        RexNode notFunc =
            RexBuilder.DEFAULT.makeCall(SqlStdOperatorTable.NOT, operandIsFalse);
        return RexBuilder.DEFAULT.makeCall(SqlStdOperatorTable.AND, isNotNullFunc, notFunc);
      } else {
        throw new UnsupportedOperationException("Unsupported unparse: "
            + call.getOperator().getName());
      }
    case IS_NOT_FALSE:
      RexNode operandIsNotFalse = call.operands.get(0);
      if (RexUtil.isDeterministic(operandIsNotFalse)) {
        // A IS NOT FALSE -> A IS NULL OR A
        RexNode isNullFunc =
            RexBuilder.DEFAULT.makeCall(SqlStdOperatorTable.IS_NULL, operandIsNotFalse);
        return RexBuilder.DEFAULT.makeCall(SqlStdOperatorTable.OR, isNullFunc, operandIsNotFalse);
      } else {
        throw new UnsupportedOperationException("Unsupported unparse: "
            + call.getOperator().getName());
      }
    case IS_TRUE:
      RexNode operandIsTrue = call.operands.get(0);
      if (RexUtil.isDeterministic(operandIsTrue)) {
        // A IS TRUE -> A IS NOT NULL AND A
        RexNode isNotNullFunc =
            RexBuilder.DEFAULT.makeCall(SqlStdOperatorTable.IS_NOT_NULL, operandIsTrue);
        return RexBuilder.DEFAULT.makeCall(SqlStdOperatorTable.AND, isNotNullFunc, operandIsTrue);
      } else {
        throw new UnsupportedOperationException("Unsupported unparse: "
            + call.getOperator().getName());
      }
    case IS_NOT_TRUE:
      RexNode operandIsNotTrue = call.operands.get(0);
      if (RexUtil.isDeterministic(operandIsNotTrue)) {
        // A IS NOT TRUE -> A IS NULL OR NOT A
        RexNode isNullFunc =
            RexBuilder.DEFAULT.makeCall(SqlStdOperatorTable.IS_NULL, operandIsNotTrue);
        RexNode notFunc =
            RexBuilder.DEFAULT.makeCall(SqlStdOperatorTable.NOT, operandIsNotTrue);
        return RexBuilder.DEFAULT.makeCall(SqlStdOperatorTable.OR, isNullFunc, notFunc);
      } else {
        throw new UnsupportedOperationException("Unsupported unparse: "
            + call.getOperator().getName());
      }
    default:
      return rexNode;
    }
  }

  /**
   * Unparses Array and Map value constructor.
   *
   * <p>For example :
   *
   * <blockquote><pre>
   * SELECT ARRAY[1, 2, 3] &rarr; SELECT ARRAY (1, 2, 3)
   * SELECT MAP['k1', 'v1', 'k2', 'v2'] &rarr; SELECT MAP ('k1', 'v1', 'k2', 'v2')
   * </pre></blockquote>
   *
   * @param writer writer
   * @param call the call
   */
  public static void unparseSparkArrayAndMap(SqlWriter writer,
      SqlCall call,
      int leftPrec,
      int rightPrec) {
    final String keyword =
        call.getKind() == SqlKind.ARRAY_VALUE_CONSTRUCTOR ? "array" : "map";

    writer.keyword(keyword);

    final SqlWriter.Frame frame = writer.startList("(", ")");
    for (SqlNode operand : call.getOperandList()) {
      writer.sep(",");
      operand.unparse(writer, leftPrec, rightPrec);
    }
    writer.endList(frame);
  }

  /**
   * Unparses TRIM function with value as space.
   *
   * <p>For example :
   *
   * <blockquote><pre>
   * SELECT TRIM(both ' ' from "ABC") &rarr; SELECT TRIM(ABC)
   * </pre></blockquote>
   *
   * @param writer writer
   * @param call the call
   */
  private static void unparseTrimWithSpace(
      SqlWriter writer, SqlCall call, int leftPrec, int rightPrec) {
    final String operatorName;
    final SqlLiteral trimFlag = call.operand(0);
    switch (trimFlag.getValueAs(SqlTrimFunction.Flag.class)) {
    case LEADING:
      operatorName = "LTRIM";
      break;
    case TRAILING:
      operatorName = "RTRIM";
      break;
    default:
      operatorName = call.getOperator().getName();
      break;
    }
    final SqlWriter.Frame trimFrame = writer.startFunCall(operatorName);
    call.operand(2).unparse(writer, leftPrec, rightPrec);
    writer.endFunCall(trimFrame);
  }

  /**
   * Creates regex pattern based on the TRIM flag.
   *
   * @param call     SqlCall contains the values that need to be trimmed
   * @param trimFlag the trimFlag, either BOTH, LEADING or TRAILING
   * @return the regex pattern of the character to be trimmed
   */
  public static SqlCharStringLiteral createRegexPatternLiteral(SqlNode call, SqlLiteral trimFlag) {
    final String regexPattern =
        requireNonNull(((SqlCharStringLiteral) call).toValue(),
            () -> "null value for SqlNode " + call);
    String escaped = escapeSpecialChar(regexPattern);
    final StringBuilder builder = new StringBuilder();
    switch (trimFlag.getValueAs(SqlTrimFunction.Flag.class)) {
    case LEADING:
      builder.append("^(").append(escaped).append(")*");
      break;
    case TRAILING:
      builder.append("(").append(escaped).append(")*$");
      break;
    default:
      builder.append("^(")
          .append(escaped)
          .append(")*|(")
          .append(escaped)
          .append(")*$");
      break;
    }
    return SqlLiteral.createCharString(builder.toString(),
      call.getParserPosition());
  }

  /**
   * Escapes the special character.
   *
   * @param inputString the string
   * @return escape character if any special character is present in the string
   */
  private static String escapeSpecialChar(String inputString) {
    final String[] specialCharacters =
        {"\\", "^", "$", "{", "}", "[", "]", "(", ")", ".",
            "*", "+", "?", "|", "<", ">", "-", "&", "%", "@"};

    for (String specialCharacter : specialCharacters) {
      if (inputString.contains(specialCharacter)) {
        inputString = inputString.replace(specialCharacter, "\\" + specialCharacter);
      }
    }
    return inputString;
  }

  /** Returns a {@link SqlSpecialOperator} with given operator name, mainly used for
   * unparse override. */
  public static SqlSpecialOperator specialOperatorByName(String opName) {
    return new SqlSpecialOperator(opName, SqlKind.OTHER_FUNCTION) {
      @Override public void unparse(
          SqlWriter writer,
          SqlCall call,
          int leftPrec,
          int rightPrec) {
        writer.print(getName());
        final SqlWriter.Frame frame =
            writer.startList(SqlWriter.FrameTypeEnum.FUN_CALL, "(", ")");
        for (SqlNode operand : call.getOperandList()) {
          writer.sep(",");
          operand.unparse(writer, 0, 0);
        }
        writer.endList(frame);
      }
    };
  }

  /**
   * Writes TRUE/FALSE or 1 = 1/ 1 &lt; &gt; 1 for certain.
   *
   * @param writer current SqlWriter object
   * @param value  boolean value to be unparsed.
   */
  public static void unparseBoolLiteralToCondition(SqlWriter writer, boolean value) {
    final SqlWriter.Frame frame = writer.startList("(", ")");
    writer.literal("1");
    writer.sep(SqlStdOperatorTable.EQUALS.getName());
    writer.literal(value ? "1" : "0");
    writer.endList(frame);
  }

  /**
   * Transformation Map type from {@code MAP<VARCHAR,VARCHAR>} to {@code Map(VARCHAR,VARCHAR)}.
   */
  public static SqlDataTypeSpec getCastSpecClickHouseSqlMapType(SqlDialect dialect,
      RelDataType type, SqlParserPos pos) {
    MapSqlType mapSqlType = (MapSqlType) type;
    SqlDataTypeSpec keySpec = (SqlDataTypeSpec) dialect.getCastSpec(mapSqlType.getKeyType());
    SqlDataTypeSpec valueSpec =
        (SqlDataTypeSpec) dialect.getCastSpec(mapSqlType.getValueType());
    SqlDataTypeSpec nonNullKeySpec =
        requireNonNull(keySpec, "keySpec");
    SqlDataTypeSpec nonNullValueSpec =
        requireNonNull(valueSpec, "valueSpec");
    SqlMapTypeNameSpec sqlMapTypeNameSpec =
        new ClickHouseSqlMapTypeNameSpec(nonNullKeySpec, nonNullValueSpec, pos);
    return new SqlDataTypeSpec(sqlMapTypeNameSpec,
        SqlParserPos.ZERO);
  }

  /**
   * Transformation Map type from {@code VARCHAR ARRAY} to {@code Array(VARCHAR)}.
   */
  public static SqlDataTypeSpec getCastSpecClickHouseSqlArrayType(SqlDialect dialect,
      RelDataType type, SqlParserPos pos) {
    ArraySqlType arraySqlType = (ArraySqlType) type;
    SqlDataTypeSpec arrayValueSpec =
        (SqlDataTypeSpec) dialect.getCastSpec(arraySqlType.getComponentType());
    SqlDataTypeSpec nonNullarrayValueSpec =
        requireNonNull(arrayValueSpec, "arrayValueSpec");
    ClickHouseSqlArrayTypeNameSpec sqlArrayTypeNameSpec =
        new ClickHouseSqlArrayTypeNameSpec(nonNullarrayValueSpec.getTypeNameSpec(),
            arraySqlType.getSqlTypeName(), pos);
    return new SqlDataTypeSpec(sqlArrayTypeNameSpec, SqlParserPos.ZERO);
  }

  /**
   * ClickHouseSqlMapTypeNameSpec to parse or unparse SQL MAP type to {@code Map(VARCHAR, VARCHAR)}.
   */
  public static class ClickHouseSqlMapTypeNameSpec extends SqlMapTypeNameSpec {

    /**
     * Creates a {@code SqlMapTypeNameSpec}.
     * example: MAP type would convert to Map(VARCHAR, VARCHAR).
     *
     * @param keyType key type of the Map
     * @param valType value type of the Map
     * @param pos the parser position, must not be null
     */
    public ClickHouseSqlMapTypeNameSpec(SqlDataTypeSpec keyType,
        SqlDataTypeSpec valType, SqlParserPos pos) {
      super(keyType, valType, pos);
    }

    @Override public void unparse(SqlWriter writer, int leftPrec, int rightPrec) {
      writer.print("Map");
      SqlWriter.Frame frame =
          writer.startList(SqlWriter.FrameTypeEnum.FUN_CALL, "(", ")");
      writer.sep(","); // configures the writer
      getKeyType().unparse(writer, leftPrec, rightPrec);
      writer.sep(",");
      getValType().unparse(writer, leftPrec, rightPrec);
      writer.endList(frame);
    }
  }

  /**
   * A ClickHouseSqlArrayTypeNameSpec to parse or unparse SQL ARRAY type to {@code Array(VARCHAR)}.
   */
  public static class ClickHouseSqlArrayTypeNameSpec extends SqlCollectionTypeNameSpec {

    /**
     * Creates a {@code ClickHouseSqlArrayTypeNameSpec}.
     * example: integer array would convert to Array(integer).
     *
     * @param elementTypeName    Type of the collection element
     * @param collectionTypeName Collection type name
     * @param pos                Parser position, must not be null
     */
    public ClickHouseSqlArrayTypeNameSpec(SqlTypeNameSpec elementTypeName,
        SqlTypeName collectionTypeName, SqlParserPos pos) {
      super(elementTypeName, collectionTypeName, pos);
    }

    @Override public void unparse(SqlWriter writer, int leftPrec, int rightPrec) {
      writer.print("Array");
      SqlWriter.Frame frame = writer.startList(SqlWriter.FrameTypeEnum.FUN_CALL, "(", ")");
      this.getElementTypeName().unparse(writer, leftPrec, rightPrec);
      writer.endList(frame);
    }
  }
}
