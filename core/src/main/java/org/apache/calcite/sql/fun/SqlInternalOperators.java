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

import org.apache.calcite.rex.RexCall;
import org.apache.calcite.sql.SqlAggFunction;
import org.apache.calcite.sql.SqlCall;
import org.apache.calcite.sql.SqlInternalOperator;
import org.apache.calcite.sql.SqlKind;
import org.apache.calcite.sql.SqlMeasureOperator;
import org.apache.calcite.sql.SqlNode;
import org.apache.calcite.sql.SqlNodeList;
import org.apache.calcite.sql.SqlOperator;
import org.apache.calcite.sql.SqlOperatorTable;
import org.apache.calcite.sql.SqlSyntax;
import org.apache.calcite.sql.SqlWriter;
import org.apache.calcite.sql.type.InferTypes;
import org.apache.calcite.sql.type.OperandTypes;
import org.apache.calcite.sql.type.ReturnTypes;
import org.apache.calcite.sql.type.SqlTypeTransforms;
import org.apache.calcite.util.Litmus;

import org.checkerframework.checker.nullness.qual.Nullable;

import java.util.List;

/**
 * Contains internal operators.
 *
 * <p>These operators are always created directly, not by looking up a function
 * or operator by name or syntax, and therefore this class does not implement
 * interface {@link SqlOperatorTable}.
 */
public abstract class SqlInternalOperators {
  private SqlInternalOperators() {
  }

  /** Similar to {@link SqlStdOperatorTable#ROW}, but does not print "ROW".
   *
   * <p>For arguments [1, TRUE], ROW would print "{@code ROW (1, TRUE)}",
   * but this operator prints "{@code (1, TRUE)}". */
  public static final SqlRowOperator ANONYMOUS_ROW =
      new SqlRowOperator("$ANONYMOUS_ROW") {
        @Override public void unparse(SqlWriter writer, SqlCall call,
            int leftPrec, int rightPrec) {
          @SuppressWarnings("assignment.type.incompatible")
          List<@Nullable SqlNode> operandList = call.getOperandList();
          writer.list(SqlWriter.FrameTypeEnum.PARENTHESES, SqlWriter.COMMA,
              SqlNodeList.of(call.getParserPosition(), operandList));
        }
      };

  /** Similar to {@link #ANONYMOUS_ROW}, but does not print "ROW" or
   * parentheses.
   *
   * <p>For arguments [1, TRUE], prints "{@code 1, TRUE}".  It is used in
   * contexts where parentheses have been printed (because we thought we were
   * about to print "{@code (ROW (1, TRUE))}") and we wish we had not. */
  public static final SqlRowOperator ANONYMOUS_ROW_NO_PARENTHESES =
      new SqlRowOperator("$ANONYMOUS_ROW_NO_PARENTHESES") {
        @Override public void unparse(SqlWriter writer, SqlCall call,
            int leftPrec, int rightPrec) {
          final SqlWriter.Frame frame =
              writer.startList(SqlWriter.FrameTypeEnum.FUN_CALL);
          for (SqlNode operand : call.getOperandList()) {
            writer.sep(",");
            operand.unparse(writer, leftPrec, rightPrec);
          }
          writer.endList(frame);
        }
      };

  /** "$THROW_UNLESS(condition, message)" throws an error with the given message
   * if condition is not TRUE, otherwise returns TRUE. */
  public static final SqlInternalOperator THROW_UNLESS =
      new SqlInternalOperator("$THROW_UNLESS", SqlKind.OTHER);

  /** <code>MEASURE</code> operator wraps an expression in the SELECT clause
   * that is a measure. It always occurs inside a call to "AS". */
  public static final SqlMeasureOperator MEASURE =
      new SqlMeasureOperator();

  /** {@code V2M} operator converts a measure to a value. */
  public static final SqlOperator M2V =
      new SqlInternalOperator("M2V", SqlKind.M2V, 2, true,
          ReturnTypes.ARG0.andThen(SqlTypeTransforms.FROM_MEASURE), null,
          OperandTypes.ANY);

  /** {@code V2M} operator converts a value to a measure. */
  public static final SqlOperator V2M =
      new SqlInternalOperator("V2M", SqlKind.V2M, 2, true,
          ReturnTypes.ARG0.andThen(SqlTypeTransforms.TO_MEASURE), null,
          OperandTypes.ANY);

  /** {@code M2X} operator evaluates an expression in a context. As for
   * {@link #V2M}, the expression may involve aggregate functions, so that it
   * can be evaluated in any aggregation context. */
  public static final SqlOperator M2X =
      new SqlInternalOperator("M2X", SqlKind.M2X, 2, true,
          ReturnTypes.ARG0.andThen(SqlTypeTransforms.FROM_MEASURE), null,
          OperandTypes.MEASURE_BOOLEAN);

  /** {@code AGG_M2M} aggregate function takes a measure as its argument and
   * returns a measure. It is used to propagate measures through the
   * {@code Aggregate} relational operator.
   *
   * @see SqlLibraryOperators#AGGREGATE */
  public static final SqlAggFunction AGG_M2M =
      SqlBasicAggFunction.create(SqlKind.AGG_M2M, ReturnTypes.ARG0,
          OperandTypes.ANY);

  /** {@code AGG_M2V} aggregate function takes a measure as its argument and
   * returns value. */
  public static final SqlAggFunction AGG_M2V =
      SqlBasicAggFunction.create(SqlKind.AGG_M2V,
          ReturnTypes.ARG0.andThen(SqlTypeTransforms.FROM_MEASURE),
          OperandTypes.ANY);

  /** {@code SAME_PARTITION} operator takes a number of expressions and returns
   * whether the values of those expressions in current row are all the same as
   * the values of those expressions in the 'anchor' row of a call to
   * {@link #M2X}.
   *
   * <p>Its role in {@code M2X} is the same as the {@code PARTITION BY} clause
   * of a windowed aggregate. For example,
   *
   * <pre>{@code
   * SUM(sal) OVER (PARTITION BY deptno, job)
   * }</pre>
   *
   * <p>is equivalent to
   *
   * <pre>{@code
   * M2X(SUM(sal), SAME_PARTITION(deptno, job))
   * }</pre>
   *
   * <p>You may think of it as expanding to a {@code BOOLEAN} expression in
   * terms of the {@code ANCHOR} record; for example,
   *
   * <pre>{@code
   * SAME_PARTITION(deptno, job)
   * }</pre>
   *
   * <p>expands to
   *
   * <pre>{@code
   * deptno IS NOT DISTINCT FROM anchor.deptno
   * AND job IS NOT DISTINCT FROM anchor.job
   * AND GROUPING(deptno, job) = GROUPING(anchor.deptno, anchor.job)
   * }</pre>
   *
   * <p>But we prefer to leave it intact for easier matching and eventual
   * elimination in transformation rules. */
  public static final SqlOperator SAME_PARTITION =
      new SqlInternalOperator("SAME_PARTITION", SqlKind.SAME_PARTITION, 2, true,
          ReturnTypes.BOOLEAN, InferTypes.ANY_NULLABLE, OperandTypes.VARIADIC);

  /** An IN operator for Druid.
   *
   * <p>Unlike the regular
   * {@link SqlStdOperatorTable#IN} operator it may
   * be used in {@link RexCall}. It does not require that
   * its operands have consistent types. */
  public static final SqlInOperator DRUID_IN =
      new SqlInOperator(SqlKind.DRUID_IN);

  /** A NOT IN operator for Druid, analogous to {@link #DRUID_IN}. */
  public static final SqlInOperator DRUID_NOT_IN =
      new SqlInOperator(SqlKind.DRUID_NOT_IN);

  /** A BETWEEN operator for Druid, analogous to {@link #DRUID_IN}. */
  public static final SqlBetweenOperator DRUID_BETWEEN =
      new SqlBetweenOperator(SqlBetweenOperator.Flag.SYMMETRIC, false) {
        @Override public SqlKind getKind() {
          return SqlKind.DRUID_BETWEEN;
        }

        @Override public boolean validRexOperands(int count, Litmus litmus) {
          return litmus.succeed();
        }
      };

  /** Separator expression inside GROUP_CONCAT, e.g. '{@code SEPARATOR ','}'. */
  public static final SqlOperator SEPARATOR =
      new SqlInternalOperator("SEPARATOR", SqlKind.SEPARATOR, 20, false,
          ReturnTypes.ARG0, InferTypes.RETURN_TYPE, OperandTypes.ANY);

  /** {@code DISTINCT} operator, occurs within {@code GROUP BY} clause. */
  public static final SqlInternalOperator GROUP_BY_DISTINCT =
      new SqlRollupOperator("GROUP BY DISTINCT", SqlKind.GROUP_BY_DISTINCT);

  /** Fetch operator is ONLY used for its precedence during unparsing. */
  public static final SqlOperator FETCH =
      SqlBasicOperator.create("FETCH")
          .withPrecedence(SqlStdOperatorTable.UNION.getLeftPrec() - 2, true);

  /** 2-argument form of the special minus-date operator
   * to be used with BigQuery subtraction functions. It differs from
   * the standard MINUS_DATE operator in that it has 2 arguments,
   * and subtracts an interval from a datetime. */
  public static final SqlDatetimeSubtractionOperator MINUS_DATE2 =
      new SqlDatetimeSubtractionOperator("MINUS_DATE2", ReturnTypes.ARG0_NULLABLE);

  /** Offset operator is ONLY used for its precedence during unparsing. */
  public static final SqlOperator OFFSET =
      SqlBasicOperator.create("OFFSET")
          .withPrecedence(SqlStdOperatorTable.UNION.getLeftPrec() - 2, true);

  /** Aggregate function that always returns a given literal. */
  public static final SqlAggFunction LITERAL_AGG =
      SqlLiteralAggFunction.INSTANCE;

  /** CAST NOT NULL operator used for cast expression to make it non-nullable in SqlNode.
   *
   * <p>For example:
   * <pre>{@code COALESCE(a,b)
   * }</pre>
   *
   * <p>is converted to
   *
   * <pre>{@code CASE WHEN a is not null THEN a ELSE b
   * }</pre>
   * by {@code SqlCoalesceFunction#rewriteCall}.
   *
   * <p>When a is nullable and b is non-nullable, the {@code COALESCE(a,b)} data type will be
   * non-nullable and {@code CASE WHEN a is not null THEN a ELSE b} data type will be nullable.
   * The validator will throw an exception about failing to preserve the data type.
   *
   * <p>Add CAST NOT NULL operator to {@code CASE WHEN a is not null THEN a ELSE b},
   * making it becomes
   * <pre>{@code CASE WHEN a is not null THEN CAST NOT NULL(a) ELSE b}
   * </pre>
   *
   * <p>Then the validator knows this data type is non-nullable.
   * So we can keep the types consistent before and after conversion.
   * */
  public static final SqlOperator CAST_NOT_NULL =
      new SqlInternalOperator("CAST NOT NULL", SqlKind.CAST_NOT_NULL, 2, true,
          ReturnTypes.ARG0.andThen(SqlTypeTransforms.TO_NOT_NULLABLE), null,
          OperandTypes.ANY) {
        // This is an internal operator, which should not be unparsed to sql.
        @Override public void unparse(SqlWriter writer, SqlCall call, int leftPrec, int rightPrec) {
          call.operand(0).unparse(writer, leftPrec, rightPrec);
        }
      };

  /** Subject to change. */
  private static class SqlBasicOperator extends SqlOperator {
    @Override public SqlSyntax getSyntax() {
      return SqlSyntax.SPECIAL;
    }

    /** Private constructor. Use {@link #create}. */
    private SqlBasicOperator(String name, int leftPrecedence, int rightPrecedence) {
      super(name, SqlKind.OTHER, leftPrecedence, rightPrecedence,
          ReturnTypes.BOOLEAN, InferTypes.RETURN_TYPE, OperandTypes.ANY);
    }

    static SqlBasicOperator create(String name) {
      return new SqlBasicOperator(name, 0, 0);
    }

    SqlBasicOperator withPrecedence(int prec, boolean leftAssoc) {
      return new SqlBasicOperator(getName(), leftPrec(prec, leftAssoc),
          rightPrec(prec, leftAssoc));
    }
  }
}
