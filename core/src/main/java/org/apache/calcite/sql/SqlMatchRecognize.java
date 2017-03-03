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
package org.apache.calcite.sql;

import org.apache.calcite.sql.parser.SqlParserPos;
import org.apache.calcite.sql.util.SqlBasicVisitor;
import org.apache.calcite.sql.util.SqlVisitor;
import org.apache.calcite.sql.validate.SqlValidator;
import org.apache.calcite.sql.validate.SqlValidatorScope;
import org.apache.calcite.util.ImmutableNullableList;

import java.util.List;

/**
 * SqlNode for Match_recognize clause
 */
public class SqlMatchRecognize extends SqlCall {
  public static final int OPERAND_TABLEREF = 0;
  public static final int OPERAND_PATTERN = 1;
  public static final int OPERAND_STRICT_STARTS = 2;
  public static final int OPERAND_STRICT_ENDS = 3;
  public static final int OPERAND_PATTERN_DEFINES = 4;
  //~ Instance fields -------------------------------------------

  private SqlNode tableRef;
  private SqlNode pattern;
  private SqlLiteral isStrictStarts;
  private SqlLiteral isStrictEnds;
  private SqlNodeList patternDefList;

  //~ Constructors
  public SqlMatchRecognize(SqlParserPos pos, SqlNode tableRef, SqlNode pattern,
    SqlLiteral isStrictStarts, SqlLiteral isStrictEnds, SqlNodeList patternDefList) {
    super(pos);
    this.tableRef = tableRef;
    this.pattern = pattern;
    this.isStrictStarts = isStrictStarts;
    this.isStrictEnds = isStrictEnds;
    this.patternDefList = patternDefList;

    assert tableRef != null;
    assert  pattern != null;
    assert patternDefList != null && patternDefList.size() > 0;
  }

  // ~ Methods

  @Override public SqlOperator getOperator() {
    return SqlMatchRecognizeOperator.INSTANCE;
  }

  @Override public SqlKind getKind() {
    return SqlKind.MATCH_RECOGNIZE;
  }

  @Override public List<SqlNode> getOperandList() {
    return ImmutableNullableList.of(tableRef, pattern, isStrictStarts,
      isStrictEnds, patternDefList);
  }

  @Override public void unparse(
    SqlWriter writer,
    int leftPrec,
    int rightPrec) {
    getOperator().unparse(writer, this, 0, 0);
  }

  @Override public void validate(SqlValidator validator, SqlValidatorScope scope) {
    validator.validateMatchRecognize(this);
  }

  @Override public void setOperand(int i, SqlNode operand) {
    switch (i) {
    case OPERAND_TABLEREF:
      tableRef = operand;
      break;
    case OPERAND_PATTERN:
      pattern = operand;
      break;
    case OPERAND_STRICT_STARTS:
      isStrictStarts = (SqlLiteral) operand;
      break;
    case OPERAND_STRICT_ENDS:
      isStrictEnds = (SqlLiteral) operand;
      break;
    case OPERAND_PATTERN_DEFINES:
      patternDefList = (SqlNodeList) operand;
      break;
    default:
      new AssertionError(i);
    }
  }

  public SqlNode getTableRef() {
    return tableRef;
  }

  public SqlNode getPattern() {
    return pattern;
  }

  public SqlLiteral getIsStrictStarts() {
    return isStrictStarts;
  }

  public SqlLiteral getIsStrictEnds() {
    return isStrictEnds;
  }

  public SqlNodeList getPatternDefList() {
    return patternDefList;
  }

  /**
   * An operator describing a match_recognize specification
   */
  public static class SqlMatchRecognizeOperator extends SqlOperator {
    public static final SqlMatchRecognizeOperator INSTANCE = new SqlMatchRecognizeOperator();

    private SqlMatchRecognizeOperator() {
      super("MATCH_RECOGNIZE", SqlKind.MATCH_RECOGNIZE, 2, true, null, null, null);
    }

    @Override public SqlSyntax getSyntax() {
      return SqlSyntax.SPECIAL;
    }

    @Override   public SqlCall createCall(
      SqlLiteral functionQualifier,
      SqlParserPos pos,
      SqlNode... operands) {
      assert functionQualifier == null;
      assert operands.length == 5;

      //  public SqlMatchRecognize(SqlParserPos pos, SqlNode tableRef, SqlNode pattern,
      //SqlLiteral isStrictStarts, SqlLiteral isStrictEnds, SqlNodeList patternDefList) {
      return new SqlMatchRecognize(pos,
        operands[0],
        operands[1],
        (SqlLiteral) operands[2],
        (SqlLiteral) operands[3],
        (SqlNodeList) operands[4]);
    }

    @Override public <R> void acceptCall(
      SqlVisitor<R> visitor,
      SqlCall call,
      boolean onlyExpressions,
      SqlBasicVisitor.ArgHandler<R> argHandler) {
      if (onlyExpressions) {
        List<SqlNode> operands = call.getOperandList();
        for (int i = 0; i < operands.size(); i++) {
          SqlNode operand = operands.get(i);
          if (operand == null) {
            continue;
          }
          argHandler.visitChild(visitor, call, i, operand);
        }
      } else {
        super.acceptCall(visitor, call, onlyExpressions, argHandler);
      }
    }

    @Override public void validateCall(
      SqlCall call,
      SqlValidator validator,
      SqlValidatorScope scope,
      SqlValidatorScope operandScope) {
      validator.validateMatchRecognize(call);
    }

    @Override public void unparse(
      SqlWriter writer,
      SqlCall call,
      int leftPrec,
      int rightPrec) {
      final SqlMatchRecognize pattern = (SqlMatchRecognize) call;

      pattern.tableRef.unparse(writer, 0, 0);
      final SqlWriter.Frame mrFrame = writer.startFunCall("MATCH_RECOGNIZE");

      writer.newlineAndIndent();
      writer.sep("PATTERN");

      SqlWriter.Frame patternFrame = writer.startList("(", ")");
      if (pattern.isStrictStarts.booleanValue()) {
        writer.sep("^");
      }
      pattern.pattern.unparse(writer, 0, 0);
      if (pattern.isStrictEnds.booleanValue()) {
        writer.sep("$");
      }
      writer.endList(patternFrame);

      writer.newlineAndIndent();
      writer.sep("DEFINE");

      SqlWriter.Frame patternDefFrame = writer.startList("", "");
      pattern.patternDefList.unparse(writer, 0, 0);
      writer.endList(patternDefFrame);

      writer.endList(mrFrame);
    }
  }
}

// End SqlMatchRecognize.java
