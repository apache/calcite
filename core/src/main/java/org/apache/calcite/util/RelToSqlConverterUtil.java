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

import org.apache.calcite.sql.SqlCall;
import org.apache.calcite.sql.SqlCharStringLiteral;
import org.apache.calcite.sql.SqlKind;
import org.apache.calcite.sql.SqlLiteral;
import org.apache.calcite.sql.SqlNode;
import org.apache.calcite.sql.SqlSpecialOperator;
import org.apache.calcite.sql.SqlWriter;
import org.apache.calcite.sql.fun.SqlTrimFunction;
import org.apache.calcite.sql.parser.SqlParserPos;

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
}
