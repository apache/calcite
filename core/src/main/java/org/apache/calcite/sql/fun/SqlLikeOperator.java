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

import org.apache.calcite.sql.SqlCall;
import org.apache.calcite.sql.SqlCallBinding;
import org.apache.calcite.sql.SqlKind;
import org.apache.calcite.sql.SqlNode;
import org.apache.calcite.sql.SqlOperandCountRange;
import org.apache.calcite.sql.SqlOperator;
import org.apache.calcite.sql.SqlSpecialOperator;
import org.apache.calcite.sql.SqlWriter;
import org.apache.calcite.sql.parser.SqlParserPos;
import org.apache.calcite.sql.parser.SqlParserUtil;
import org.apache.calcite.sql.type.InferTypes;
import org.apache.calcite.sql.type.OperandTypes;
import org.apache.calcite.sql.type.ReturnTypes;
import org.apache.calcite.sql.type.SqlOperandCountRanges;
import org.apache.calcite.sql.type.SqlTypeUtil;
import org.apache.calcite.sql.validate.SqlValidator;
import org.apache.calcite.sql.validate.SqlValidatorScope;
import org.apache.calcite.util.Litmus;

/**
 * An operator describing the <code>LIKE</code> and <code>SIMILAR</code>
 * operators.
 *
 * <p>Syntax of the two operators:
 *
 * <ul>
 * <li><code>src-value [NOT] LIKE pattern-value [ESCAPE
 * escape-value]</code></li>
 * <li><code>src-value [NOT] SIMILAR pattern-value [ESCAPE
 * escape-value]</code></li>
 * </ul>
 *
 * <p><b>NOTE</b> If the <code>NOT</code> clause is present the
 * {@link org.apache.calcite.sql.parser.SqlParser parser} will generate a
 * equivalent to <code>NOT (src LIKE pattern ...)</code>
 */
public class SqlLikeOperator extends SqlSpecialOperator {
  //~ Instance fields --------------------------------------------------------

  private final boolean negated;
  private final boolean caseSensitive;

  //~ Constructors -----------------------------------------------------------

  /**
   * Creates a SqlLikeOperator.
   *
   * @param name        Operator name
   * @param kind        Kind
   * @param negated     Whether this is 'NOT LIKE'
   * @param caseSensitive Whether this operator ignores the case of its operands
   */
  SqlLikeOperator(
      String name,
      SqlKind kind,
      boolean negated,
      boolean caseSensitive) {
    // LIKE is right-associative, because that makes it easier to capture
    // dangling ESCAPE clauses: "a like b like c escape d" becomes
    // "a like (b like c escape d)".
    super(
        name,
        kind,
        32,
        false,
        ReturnTypes.BOOLEAN_NULLABLE,
        InferTypes.FIRST_KNOWN,
        OperandTypes.STRING_SAME_SAME_SAME);
    if (!caseSensitive && kind != SqlKind.LIKE) {
      throw new IllegalArgumentException("Only (possibly negated) "
          + SqlKind.LIKE + " can be made case-insensitive, not " + kind);
    }

    this.negated = negated;
    this.caseSensitive = caseSensitive;
  }

  //~ Methods ----------------------------------------------------------------

  /**
   * Returns whether this is the 'NOT LIKE' operator.
   *
   * @return whether this is 'NOT LIKE'
   *
   * @see #not()
   */
  public boolean isNegated() {
    return negated;
  }

  /**
   * Returns whether this operator matches the case of its operands.
   * For example, returns true for {@code LIKE} and false for {@code ILIKE}.
   *
   * @return whether this operator matches the case of its operands
   */
  public boolean isCaseSensitive() {
    return caseSensitive;
  }

  @Override public SqlOperator not() {
    return of(kind, !negated, caseSensitive);
  }

  private static SqlOperator of(SqlKind kind, boolean negated,
      boolean caseSensitive) {
    switch (kind) {
    case SIMILAR:
      return negated
          ? SqlStdOperatorTable.NOT_SIMILAR_TO
          : SqlStdOperatorTable.SIMILAR_TO;
    case RLIKE:
      return negated
          ? SqlLibraryOperators.NOT_RLIKE
          : SqlLibraryOperators.RLIKE;
    case LIKE:
      if (caseSensitive) {
        return negated
            ? SqlStdOperatorTable.NOT_LIKE
            : SqlStdOperatorTable.LIKE;
      } else {
        return negated
            ? SqlLibraryOperators.NOT_ILIKE
            : SqlLibraryOperators.ILIKE;
      }
    default:
      throw new AssertionError("unexpected " + kind);
    }
  }

  @Override public SqlOperandCountRange getOperandCountRange() {
    return SqlOperandCountRanges.between(2, 3);
  }

  @Override public boolean checkOperandTypes(
      SqlCallBinding callBinding,
      boolean throwOnFailure) {
    switch (callBinding.getOperandCount()) {
    case 2:
      if (!OperandTypes.STRING_SAME_SAME.checkOperandTypes(
          callBinding,
          throwOnFailure)) {
        return false;
      }
      break;
    case 3:
      if (!OperandTypes.STRING_SAME_SAME_SAME.checkOperandTypes(
          callBinding,
          throwOnFailure)) {
        return false;
      }

      // calc implementation should
      // enforce the escape character length to be 1
      break;
    default:
      throw new AssertionError("unexpected number of args to "
          + callBinding.getCall() + ": " + callBinding.getOperandCount());
    }

    return SqlTypeUtil.isCharTypeComparable(
        callBinding,
        callBinding.operands(),
        throwOnFailure);
  }

  @Override public void validateCall(SqlCall call, SqlValidator validator,
      SqlValidatorScope scope, SqlValidatorScope operandScope) {
    super.validateCall(call, validator, scope, operandScope);
  }

  @Override public boolean validRexOperands(int count, Litmus litmus) {
    if (negated) {
      litmus.fail("unsupported negated operator {}", this);
    }
    return super.validRexOperands(count, litmus);
  }

  @Override public void unparse(
      SqlWriter writer,
      SqlCall call,
      int leftPrec,
      int rightPrec) {
    final SqlWriter.Frame frame = writer.startList("", "");
    call.operand(0).unparse(writer, getLeftPrec(), getRightPrec());
    writer.sep(getName());

    call.operand(1).unparse(writer, getLeftPrec(), getRightPrec());
    if (call.operandCount() == 3) {
      writer.sep("ESCAPE");
      call.operand(2).unparse(writer, getLeftPrec(), getRightPrec());
    }
    writer.endList(frame);
  }

  @Override public ReduceResult reduceExpr(
      final int opOrdinal,
      TokenSequence list) {
    // Example:
    //   a LIKE b || c ESCAPE d || e AND f
    // |  |    |      |      |      |
    //  exp0    exp1          exp2
    SqlNode exp0 = list.node(opOrdinal - 1);
    SqlOperator op = list.op(opOrdinal);
    assert op instanceof SqlLikeOperator;
    SqlNode exp1 =
        SqlParserUtil.toTreeEx(
            list,
            opOrdinal + 1,
            getRightPrec(),
            SqlKind.ESCAPE);
    SqlNode exp2 = null;
    if ((opOrdinal + 2) < list.size()) {
      if (list.isOp(opOrdinal + 2)) {
        final SqlOperator op2 = list.op(opOrdinal + 2);
        if (op2.getKind() == SqlKind.ESCAPE) {
          exp2 =
              SqlParserUtil.toTreeEx(
                  list,
                  opOrdinal + 3,
                  getRightPrec(),
                  SqlKind.ESCAPE);
        }
      }
    }
    final SqlNode[] operands;
    final int end;
    if (exp2 != null) {
      operands = new SqlNode[]{exp0, exp1, exp2};
      end = opOrdinal + 4;
    } else {
      operands = new SqlNode[]{exp0, exp1};
      end = opOrdinal + 2;
    }
    SqlCall call = createCall(SqlParserPos.sum(operands), operands);
    return new ReduceResult(opOrdinal - 1, end, call);
  }
}
