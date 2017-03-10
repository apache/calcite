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
  public static final int OPERAND_TABLE_REF = 0;
  public static final int OPERAND_PATTERN = 1;
  public static final int OPERAND_STRICT_START = 2;
  public static final int OPERAND_STRICT_END = 3;
  public static final int OPERAND_PATTERN_DEFINES = 4;

  //~ Instance fields -------------------------------------------

  private SqlNode tableRef;
  private SqlNode pattern;
  private SqlLiteral strictStart;
  private SqlLiteral strictEnd;
  private SqlNodeList patternDefList;

  /** Creates a SqlMatchRecognize. */
  public SqlMatchRecognize(SqlParserPos pos, SqlNode tableRef, SqlNode pattern,
      SqlLiteral strictStart, SqlLiteral strictEnd, SqlNodeList patternDefList) {
    super(pos);
    this.tableRef = tableRef;
    this.pattern = pattern;
    this.strictStart = strictStart;
    this.strictEnd = strictEnd;
    this.patternDefList = patternDefList;

    assert tableRef != null;
    assert pattern != null;
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
    return ImmutableNullableList.of(tableRef, pattern, strictStart, strictEnd,
        patternDefList);
  }

  @Override public void unparse(SqlWriter writer, int leftPrec,
      int rightPrec) {
    getOperator().unparse(writer, this, 0, 0);
  }

  @Override public void validate(SqlValidator validator, SqlValidatorScope scope) {
    validator.validateMatchRecognize(this);
  }

  @Override public void setOperand(int i, SqlNode operand) {
    switch (i) {
    case OPERAND_TABLE_REF:
      tableRef = operand;
      break;
    case OPERAND_PATTERN:
      pattern = operand;
      break;
    case OPERAND_STRICT_START:
      strictStart = (SqlLiteral) operand;
      break;
    case OPERAND_STRICT_END:
      strictEnd = (SqlLiteral) operand;
      break;
    case OPERAND_PATTERN_DEFINES:
      patternDefList = (SqlNodeList) operand;
      break;
    default:
      throw new AssertionError(i);
    }
  }

  public SqlNode getTableRef() {
    return tableRef;
  }

  public SqlNode getPattern() {
    return pattern;
  }

  public SqlLiteral getStrictStart() {
    return strictStart;
  }

  public SqlLiteral getStrictEnd() {
    return strictEnd;
  }

  public SqlNodeList getPatternDefList() {
    return patternDefList;
  }

  /**
   * An operator describing a MATCH_RECOGNIZE specification.
   */
  public static class SqlMatchRecognizeOperator extends SqlOperator {
    public static final SqlMatchRecognizeOperator INSTANCE =
        new SqlMatchRecognizeOperator();

    private SqlMatchRecognizeOperator() {
      super("MATCH_RECOGNIZE", SqlKind.MATCH_RECOGNIZE, 2, true, null, null, null);
    }

    @Override public SqlSyntax getSyntax() {
      return SqlSyntax.SPECIAL;
    }

    @Override public SqlCall createCall(
        SqlLiteral functionQualifier,
        SqlParserPos pos,
        SqlNode... operands) {
      assert functionQualifier == null;
      assert operands.length == 5;

      return new SqlMatchRecognize(pos, operands[0], operands[1],
          (SqlLiteral) operands[2], (SqlLiteral) operands[3],
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
      if (pattern.strictStart.booleanValue()) {
        writer.sep("^");
      }
      pattern.pattern.unparse(writer, 0, 0);
      if (pattern.strictEnd.booleanValue()) {
        writer.sep("$");
      }
      writer.endList(patternFrame);

      writer.newlineAndIndent();
      writer.sep("DEFINE");

      final SqlWriter.Frame patternDefFrame = writer.startList("", "");
      final SqlNodeList newDefineList = new SqlNodeList(SqlParserPos.ZERO);
      for (SqlNode node : pattern.getPatternDefList()) {
        final SqlCall call2 = (SqlCall) node;
        // swap the position of alias position in AS operator
        newDefineList.add(
            call2.getOperator().createCall(SqlParserPos.ZERO, call2.operand(1),
                call2.operand(0)));
      }
      newDefineList.unparse(writer, 0, 0);
      writer.endList(patternDefFrame);
      writer.endList(mrFrame);
    }
  }
}

// End SqlMatchRecognize.java
