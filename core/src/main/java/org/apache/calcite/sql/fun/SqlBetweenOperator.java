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
package org.eigenbase.sql.fun;

import java.util.*;

import org.eigenbase.reltype.*;
import org.eigenbase.sql.*;
import org.eigenbase.sql.parser.*;
import org.eigenbase.sql.type.*;
import org.eigenbase.sql.util.*;
import org.eigenbase.sql.validate.*;
import org.eigenbase.util.*;

import static org.eigenbase.util.Static.RESOURCE;

/**
 * Defines the BETWEEN operator.
 *
 * <p>Syntax:
 *
 * <blockquote><code>X [NOT] BETWEEN [ASYMMETRIC | SYMMETRIC] Y AND
 * Z</code></blockquote>
 *
 * <p>If the asymmetric/symmeteric keywords are left out ASYMMETRIC is default.
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
      new ComparableOperandTypeChecker(
          3,
          RelDataTypeComparability.ALL);
  private static final SqlWriter.FrameType FRAME_TYPE =
      SqlWriter.FrameTypeEnum.create("BETWEEN");

  //~ Enums ------------------------------------------------------------------

  /**
   * Defines the "SYMMETRIC" and "ASYMMETRIC" keywords.
   */
  public enum Flag implements SqlLiteral.SqlSymbol {
    ASYMMETRIC, SYMMETRIC
  }

  //~ Instance fields --------------------------------------------------------

  public final Flag flag;

  /**
   * If true the call represents 'NOT BETWEEN'.
   */
  private final boolean negated;

  //~ Constructors -----------------------------------------------------------

  public SqlBetweenOperator(
      Flag flag,
      boolean negated) {
    super(
        negated ? NOT_BETWEEN_NAMES : BETWEEN_NAMES,
        SqlKind.BETWEEN,
        30,
        null,
        null, OTC_CUSTOM);
    this.flag = flag;
    this.negated = negated;
  }

  //~ Methods ----------------------------------------------------------------

  public boolean isNegated() {
    return negated;
  }

  private List<RelDataType> collectOperandTypes(
      SqlValidator validator,
      SqlValidatorScope scope,
      SqlCall call) {
    List<RelDataType> argTypes =
        SqlTypeUtil.deriveAndCollectTypes(
            validator, scope, call.getOperandList());
    return ImmutableNullableList.of(
        argTypes.get(VALUE_OPERAND),
        argTypes.get(LOWER_OPERAND),
        argTypes.get(UPPER_OPERAND));
  }

  public RelDataType inferReturnType(
      SqlOperatorBinding opBinding) {
    SqlCallBinding callBinding = (SqlCallBinding) opBinding;
    ExplicitOperatorBinding newOpBinding =
        new ExplicitOperatorBinding(
            opBinding,
            collectOperandTypes(
                callBinding.getValidator(),
                callBinding.getScope(),
                callBinding.getCall()));
    return ReturnTypes.BOOLEAN_NULLABLE.inferReturnType(
        newOpBinding);
  }

  public String getSignatureTemplate(final int operandsCount) {
    Util.discard(operandsCount);
    return "{1} {0} {2} AND {3}";
  }

  public void unparse(
      SqlWriter writer,
      SqlCall call,
      int leftPrec,
      int rightPrec) {
    final SqlWriter.Frame frame =
        writer.startList(FRAME_TYPE, "", "");
    call.operand(VALUE_OPERAND).unparse(writer, getLeftPrec(), 0);
    writer.sep(getName());
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

  public int reduceExpr(
      int opOrdinal,
      List<Object> list) {
    final SqlParserUtil.ToTreeListItem betweenNode =
        (SqlParserUtil.ToTreeListItem) list.get(opOrdinal);
    SqlOperator op = betweenNode.getOperator();
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
    final SqlParserPos pos =
        ((SqlNode) list.get(opOrdinal + 1)).getParserPosition();
    SqlNode exp1 =
        SqlParserUtil.toTreeEx(list, opOrdinal + 1, 0, SqlKind.AND);
    if ((opOrdinal + 2) >= list.size()) {
      SqlParserPos lastPos =
          ((SqlNode) list.get(list.size() - 1)).getParserPosition();
      final int line = lastPos.getEndLineNum();
      final int col = lastPos.getEndColumnNum() + 1;
      SqlParserPos errPos = new SqlParserPos(line, col, line, col);
      throw SqlUtil.newContextException(errPos, RESOURCE.betweenWithoutAnd());
    }
    final Object o = list.get(opOrdinal + 2);
    if (!(o instanceof SqlParserUtil.ToTreeListItem)) {
      SqlParserPos errPos = ((SqlNode) o).getParserPosition();
      throw SqlUtil.newContextException(errPos, RESOURCE.betweenWithoutAnd());
    }
    if (((SqlParserUtil.ToTreeListItem) o).getOperator().getKind()
        != SqlKind.AND) {
      SqlParserPos errPos = ((SqlParserUtil.ToTreeListItem) o).getPos();
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
        SqlParserUtil.toTreeEx(
            list,
            opOrdinal + 3,
            getRightPrec(),
            SqlKind.OTHER);

    // Create the call.
    SqlNode exp0 = (SqlNode) list.get(opOrdinal - 1);
    SqlCall newExp =
        createCall(
            betweenNode.getPos(),
            exp0,
            exp1,
            exp2);

    // Replace all of the matched nodes with the single reduced node.
    SqlParserUtil.replaceSublist(
        list,
        opOrdinal - 1,
        opOrdinal + 4,
        newExp);

    // Return the ordinal of the new current node.
    return opOrdinal - 1;
  }

  //~ Inner Classes ----------------------------------------------------------

  /**
   * Finds an AND operator in an expression.
   */
  private static class AndFinder extends SqlBasicVisitor<Void> {
    public Void visit(SqlCall call) {
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

// End SqlBetweenOperator.java
