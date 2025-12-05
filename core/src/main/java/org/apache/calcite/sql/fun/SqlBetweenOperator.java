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

import org.apache.calcite.rel.type.RelDataType;
import org.apache.calcite.rel.type.RelDataTypeComparability;
import org.apache.calcite.sql.ExplicitOperatorBinding;
import org.apache.calcite.sql.SqlCall;
import org.apache.calcite.sql.SqlInfixOperator;
import org.apache.calcite.sql.SqlKind;
import org.apache.calcite.sql.SqlNode;
import org.apache.calcite.sql.SqlOperator;
import org.apache.calcite.sql.SqlOperatorBinding;
import org.apache.calcite.sql.SqlUtil;
import org.apache.calcite.sql.SqlWriter;
import org.apache.calcite.sql.parser.SqlParserPos;
import org.apache.calcite.sql.parser.SqlParserUtil;
import org.apache.calcite.sql.type.ComparableOperandTypeChecker;
import org.apache.calcite.sql.type.InferTypes;
import org.apache.calcite.sql.type.ReturnTypes;
import org.apache.calcite.sql.type.SqlOperandTypeChecker;
import org.apache.calcite.sql.util.SqlBasicVisitor;
import org.apache.calcite.util.Litmus;
import org.apache.calcite.util.Util;

import static org.apache.calcite.util.Static.RESOURCE;

/**
 * Defines the BETWEEN operator.
 *
 * <p>Syntax:
 *
 * <blockquote><code>X [NOT] BETWEEN [ASYMMETRIC | SYMMETRIC] Y AND
 * Z</code></blockquote>
 *
 * <p>If the asymmetric/symmetric keywords are left out ASYMMETRIC is default.
 *
 * <p>This operator is always expanded (into something like <code>Y &lt;= X AND
 * X &lt;= Z</code>) before being converted into Rex nodes.
 */
public class SqlBetweenOperator extends SqlInfixOperator {
  //~ Static fields/initializers ---------------------------------------------

  private static final String[] BETWEEN_NAMES = {"BETWEEN", "AND"};
  private static final String[] NOT_BETWEEN_NAMES = {"NOT BETWEEN", "AND"};

  /**
   * Ordinal of the 'value' operand.
   */
  public static final int VALUE_OPERAND = 0;

  /**
   * Ordinal of the 'lower' operand.
   */
  public static final int LOWER_OPERAND = 1;

  /**
   * Ordinal of the 'upper' operand.
   */
  public static final int UPPER_OPERAND = 2;

  /**
   * Custom operand-type checking strategy.
   */
  private static final SqlOperandTypeChecker OTC_CUSTOM =
      new ComparableOperandTypeChecker(3, RelDataTypeComparability.ALL,
          SqlOperandTypeChecker.Consistency.COMPARE);
  private static final SqlWriter.FrameType FRAME_TYPE =
      SqlWriter.FrameTypeEnum.create("BETWEEN");

  //~ Enums ------------------------------------------------------------------

  /**
   * Defines the "SYMMETRIC" and "ASYMMETRIC" keywords.
   */
  public enum Flag {
    ASYMMETRIC, SYMMETRIC
  }

  //~ Instance fields --------------------------------------------------------

  public final Flag flag;

  /**
   * If true the call represents 'NOT BETWEEN'.
   */
  private final boolean negated;

  //~ Constructors -----------------------------------------------------------

  public SqlBetweenOperator(Flag flag, boolean negated) {
    super(negated ? NOT_BETWEEN_NAMES : BETWEEN_NAMES, SqlKind.BETWEEN, 32,
        null, InferTypes.FIRST_KNOWN, OTC_CUSTOM);
    this.flag = flag;
    this.negated = negated;
  }

  //~ Methods ----------------------------------------------------------------

  @Override public boolean validRexOperands(int count, Litmus litmus) {
    return litmus.fail("not a rex operator");
  }

  /**
   * Returns whether this is 'NOT' variant of an operator.
   *
   * @see #not()
   */
  public boolean isNegated() {
    return negated;
  }

  @Override public SqlOperator not() {
    return of(negated, flag == Flag.SYMMETRIC);
  }

  private static SqlBetweenOperator of(boolean negated, boolean symmetric) {
    if (symmetric) {
      return negated
          ? SqlStdOperatorTable.SYMMETRIC_BETWEEN
          : SqlStdOperatorTable.SYMMETRIC_NOT_BETWEEN;
    } else {
      return negated
          ? SqlStdOperatorTable.NOT_BETWEEN
          : SqlStdOperatorTable.BETWEEN;
    }
  }

  @Override public RelDataType inferReturnType(
      SqlOperatorBinding opBinding) {
    ExplicitOperatorBinding newOpBinding =
        new ExplicitOperatorBinding(
            opBinding,
            opBinding.collectOperandTypes());
    RelDataType type =
        ReturnTypes.BOOLEAN_NULLABLE.inferReturnType(newOpBinding);
    if (type == null) {
      throw opBinding.newError(
          RESOURCE.cannotInferReturnType(
              opBinding.getOperator().toString(),
              opBinding.collectOperandTypes().toString()));
    }
    return type;
  }

  @Override public String getSignatureTemplate(final int operandsCount) {
    Util.discard(operandsCount);
    return "{1} {0} {2} AND {3}";
  }

  @Override public String getName() {
    return super.getName()
        + " "
        + flag.name();
  }

  @Override public void unparse(
      SqlWriter writer,
      SqlCall call,
      int leftPrec,
      int rightPrec) {
    final SqlWriter.Frame frame =
        writer.startList(FRAME_TYPE, "", "");
    call.operand(VALUE_OPERAND).unparse(writer, getLeftPrec(), 0);
    writer.sep(super.getName());
    writer.sep(flag.name());

    // If the expression for the lower bound contains a call to an AND
    // operator, we need to wrap the expression in parentheses to prevent
    // the AND from associating with BETWEEN. For example, we should
    // unparse
    //    a BETWEEN b OR (c AND d) OR e AND f
    // as
    //    a BETWEEN (b OR c AND d) OR e) AND f
    // If it were unparsed as
    //    a BETWEEN b OR c AND d OR e AND f
    // then it would be interpreted as
    //    (a BETWEEN (b OR c) AND d) OR (e AND f)
    // which would be wrong.
    final SqlNode lower = call.operand(LOWER_OPERAND);
    final SqlNode upper = call.operand(UPPER_OPERAND);
    int lowerPrec = new AndFinder().containsAnd(lower) ? 100 : 0;
    lower.unparse(writer, lowerPrec, lowerPrec);
    writer.sep("AND");
    upper.unparse(writer, 0, getRightPrec());
    writer.endList(frame);
  }

  @Override public ReduceResult reduceExpr(int opOrdinal, TokenSequence list) {
    SqlOperator op = list.op(opOrdinal);
    assert op == this;

    // Break the expression up into expressions. For example, a simple
    // expression breaks down as follows:
    //
    //            opOrdinal   endExp1
    //            |           |
    //     a + b BETWEEN c + d AND e + f
    //    |_____|       |_____|   |_____|
    //     exp0          exp1      exp2
    // Create the expression between 'BETWEEN' and 'AND'.
    SqlNode exp1 =
        SqlParserUtil.toTreeEx(list, opOrdinal + 1, 0, SqlKind.AND);
    if ((opOrdinal + 2) >= list.size()) {
      SqlParserPos lastPos = list.pos(list.size() - 1);
      final int line = lastPos.getEndLineNum();
      final int col = lastPos.getEndColumnNum() + 1;
      SqlParserPos errPos = new SqlParserPos(line, col, line, col);
      throw SqlUtil.newContextException(errPos, RESOURCE.betweenWithoutAnd());
    }
    if (!list.isOp(opOrdinal + 2)
        || list.op(opOrdinal + 2).getKind() != SqlKind.AND) {
      SqlParserPos errPos = list.pos(opOrdinal + 2);
      throw SqlUtil.newContextException(errPos, RESOURCE.betweenWithoutAnd());
    }

    // Create the expression after 'AND', but stopping if we encounter an
    // operator of lower precedence.
    //
    // For example,
    //   a BETWEEN b AND c + d OR e
    // becomes
    //   (a BETWEEN b AND c + d) OR e
    // because OR has lower precedence than BETWEEN.
    SqlNode exp2 =
        SqlParserUtil.toTreeEx(list,
            opOrdinal + 3,
            getRightPrec(),
            SqlKind.OTHER);

    // Create the call.
    SqlNode exp0 = list.node(opOrdinal - 1);
    SqlCall newExp =
        createCall(
            list.pos(opOrdinal),
            exp0,
            exp1,
            exp2);

    // Replace all of the matched nodes with the single reduced node.
    return new ReduceResult(opOrdinal - 1, opOrdinal + 4, newExp);
  }

  //~ Inner Classes ----------------------------------------------------------

  /**
   * Finds an AND operator in an expression.
   */
  private static class AndFinder extends SqlBasicVisitor<Void> {
    @Override public Void visit(SqlCall call) {
      final SqlOperator operator = call.getOperator();
      if (operator == SqlStdOperatorTable.AND) {
        throw Util.FoundOne.NULL;
      }
      return super.visit(call);
    }

    boolean containsAnd(SqlNode node) {
      try {
        node.accept(this);
        return false;
      } catch (Util.FoundOne e) {
        return true;
      }
    }
  }
}
