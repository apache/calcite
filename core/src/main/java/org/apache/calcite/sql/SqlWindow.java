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

import org.apache.calcite.linq4j.Ord;
import org.apache.calcite.rel.type.RelDataType;
import org.apache.calcite.rex.RexWindowBound;
import org.apache.calcite.sql.parser.SqlParserPos;
import org.apache.calcite.sql.type.ReturnTypes;
import org.apache.calcite.sql.type.SqlTypeFamily;
import org.apache.calcite.sql.util.SqlBasicVisitor;
import org.apache.calcite.sql.util.SqlVisitor;
import org.apache.calcite.sql.validate.SqlValidator;
import org.apache.calcite.sql.validate.SqlValidatorScope;
import org.apache.calcite.sql.validate.SqlValidatorUtil;
import org.apache.calcite.util.ControlFlowException;
import org.apache.calcite.util.ImmutableNullableList;
import org.apache.calcite.util.Litmus;
import org.apache.calcite.util.Util;

import com.google.common.collect.ImmutableList;

import java.util.List;

import static org.apache.calcite.util.Static.RESOURCE;

/**
 * SQL window specification.
 *
 * <p>For example, the query</p>
 *
 * <blockquote>
 * <pre>SELECT sum(a) OVER (w ROWS 3 PRECEDING)
 * FROM t
 * WINDOW w AS (PARTITION BY x, y ORDER BY z),
 *     w1 AS (w ROWS 5 PRECEDING UNBOUNDED FOLLOWING)</pre>
 * </blockquote>
 *
 * <p>declares windows w and w1, and uses a window in an OVER clause. It thus
 * contains 3 {@link SqlWindow} objects.</p>
 */
public class SqlWindow extends SqlCall {
  /**
   * The FOLLOWING operator used exclusively in a window specification.
   */
  public static final SqlPostfixOperator FOLLOWING_OPERATOR =
      new SqlPostfixOperator("FOLLOWING", SqlKind.FOLLOWING, 20,
          ReturnTypes.ARG0, null,
          null);
  /**
   * The PRECEDING operator used exclusively in a window specification.
   */
  public static final SqlPostfixOperator PRECEDING_OPERATOR =
      new SqlPostfixOperator("PRECEDING", SqlKind.PRECEDING, 20,
          ReturnTypes.ARG0, null,
          null);

  //~ Instance fields --------------------------------------------------------

  /** The name of the window being declared. */
  SqlIdentifier declName;

  /** The name of the window being referenced, or null. */
  SqlIdentifier refName;

  /** The list of partitioning columns. */
  SqlNodeList partitionList;

  /** The list of ordering columns. */
  SqlNodeList orderList;

  /** Whether it is a physical (rows) or logical (values) range. */
  SqlLiteral isRows;

  /** The lower bound of the window. */
  SqlNode lowerBound;

  /** The upper bound of the window. */
  SqlNode upperBound;

  /** Whether to allow partial results. It may be null. */
  SqlLiteral allowPartial;

  private SqlCall windowCall = null;

  //~ Constructors -----------------------------------------------------------

  /**
   * Creates a window.
   */
  public SqlWindow(SqlParserPos pos, SqlIdentifier declName,
      SqlIdentifier refName, SqlNodeList partitionList, SqlNodeList orderList,
      SqlLiteral isRows, SqlNode lowerBound, SqlNode upperBound,
      SqlLiteral allowPartial) {
    super(pos);
    this.declName = declName;
    this.refName = refName;
    this.partitionList = partitionList;
    this.orderList = orderList;
    this.isRows = isRows;
    this.lowerBound = lowerBound;
    this.upperBound = upperBound;
    this.allowPartial = allowPartial;

    assert declName == null || declName.isSimple();
    assert partitionList != null;
    assert orderList != null;
  }

  public static SqlWindow create(SqlIdentifier declName, SqlIdentifier refName,
      SqlNodeList partitionList, SqlNodeList orderList, SqlLiteral isRows,
      SqlNode lowerBound, SqlNode upperBound, SqlLiteral allowPartial,
      SqlParserPos pos) {
    // If there's only one bound and it's 'FOLLOWING', make it the upper
    // bound.
    if (upperBound == null
        && lowerBound != null
        && lowerBound.getKind() == SqlKind.FOLLOWING) {
      upperBound = lowerBound;
      lowerBound = null;
    }
    return new SqlWindow(pos, declName, refName, partitionList, orderList,
        isRows, lowerBound, upperBound, allowPartial);
  }

  //~ Methods ----------------------------------------------------------------

  public SqlOperator getOperator() {
    return SqlWindowOperator.INSTANCE;
  }

  @Override public SqlKind getKind() {
    return SqlKind.WINDOW;
  }

  public List<SqlNode> getOperandList() {
    return ImmutableNullableList.of(declName, refName, partitionList, orderList,
        isRows, lowerBound, upperBound, allowPartial);
  }

  @Override public void setOperand(int i, SqlNode operand) {
    switch (i) {
    case 0:
      this.declName = (SqlIdentifier) operand;
      break;
    case 1:
      this.refName = (SqlIdentifier) operand;
      break;
    case 2:
      this.partitionList = (SqlNodeList) operand;
      break;
    case 3:
      this.orderList = (SqlNodeList) operand;
      break;
    case 4:
      this.isRows = (SqlLiteral) operand;
      break;
    case 5:
      this.lowerBound = operand;
      break;
    case 6:
      this.upperBound = operand;
      break;
    case 7:
      this.allowPartial = (SqlLiteral) operand;
      break;
    default:
      throw new AssertionError(i);
    }
  }

  @Override public void unparse(SqlWriter writer, int leftPrec, int rightPrec) {
    if (null != declName) {
      declName.unparse(writer, 0, 0);
      writer.keyword("AS");
    }

    // Override, so we don't print extra parentheses.
    getOperator().unparse(writer, this, 0, 0);
  }

  public SqlIdentifier getDeclName() {
    return declName;
  }

  public void setDeclName(SqlIdentifier declName) {
    assert declName.isSimple();
    this.declName = declName;
  }

  public SqlNode getLowerBound() {
    return lowerBound;
  }

  public void setLowerBound(SqlNode lowerBound) {
    this.lowerBound = lowerBound;
  }

  public SqlNode getUpperBound() {
    return upperBound;
  }

  public void setUpperBound(SqlNode upperBound) {
    this.upperBound = upperBound;
  }

  /**
   * Returns if the window is guaranteed to have rows.
   * This is useful to refine data type of window aggregates.
   * For instance sum(non-nullable) over (empty window) is NULL.
   *
   * @return true when the window is non-empty
   *
   * @see org.apache.calcite.rel.core.Window.Group#isAlwaysNonEmpty()
   * @see SqlOperatorBinding#getGroupCount()
   * @see org.apache.calcite.sql.validate.SqlValidatorImpl#resolveWindow(SqlNode, org.apache.calcite.sql.validate.SqlValidatorScope, boolean)
   */
  public boolean isAlwaysNonEmpty() {
    final SqlWindow tmp;
    if (lowerBound == null || upperBound == null) {
      // Keep the current window unmodified
      tmp = new SqlWindow(getParserPosition(), null, null, partitionList,
          orderList, isRows, lowerBound, upperBound, allowPartial);
      tmp.populateBounds();
    } else {
      tmp = this;
    }
    if (tmp.lowerBound instanceof SqlLiteral
        && tmp.upperBound instanceof SqlLiteral) {
      int lowerKey = RexWindowBound.create(tmp.lowerBound, null).getOrderKey();
      int upperKey = RexWindowBound.create(tmp.upperBound, null).getOrderKey();
      return lowerKey > -1 && lowerKey <= upperKey;
    }
    return false;
  }

  public void setRows(SqlLiteral isRows) {
    this.isRows = isRows;
  }

  public boolean isRows() {
    return isRows.booleanValue();
  }

  public SqlNodeList getOrderList() {
    return orderList;
  }

  public void setOrderList(SqlNodeList orderList) {
    this.orderList = orderList;
  }

  public SqlNodeList getPartitionList() {
    return partitionList;
  }

  public void setPartitionList(SqlNodeList partitionList) {
    this.partitionList = partitionList;
  }

  public SqlIdentifier getRefName() {
    return refName;
  }

  public void setWindowCall(SqlCall windowCall) {
    this.windowCall = windowCall;
    assert windowCall == null
        || windowCall.getOperator() instanceof SqlAggFunction;
  }

  public SqlCall getWindowCall() {
    return windowCall;
  }

  /** @see Util#deprecated(Object, boolean) */
  static void checkSpecialLiterals(SqlWindow window, SqlValidator validator) {
    final SqlNode lowerBound = window.getLowerBound();
    final SqlNode upperBound = window.getUpperBound();
    Object lowerLitType = null;
    Object upperLitType = null;
    SqlOperator lowerOp = null;
    SqlOperator upperOp = null;
    if (null != lowerBound) {
      if (lowerBound.getKind() == SqlKind.LITERAL) {
        lowerLitType = ((SqlLiteral) lowerBound).getValue();
        if (Bound.UNBOUNDED_FOLLOWING == lowerLitType) {
          throw validator.newValidationError(lowerBound,
              RESOURCE.badLowerBoundary());
        }
      } else if (lowerBound instanceof SqlCall) {
        lowerOp = ((SqlCall) lowerBound).getOperator();
      }
    }
    if (null != upperBound) {
      if (upperBound.getKind() == SqlKind.LITERAL) {
        upperLitType = ((SqlLiteral) upperBound).getValue();
        if (Bound.UNBOUNDED_PRECEDING == upperLitType) {
          throw validator.newValidationError(upperBound,
              RESOURCE.badUpperBoundary());
        }
      } else if (upperBound instanceof SqlCall) {
        upperOp = ((SqlCall) upperBound).getOperator();
      }
    }

    if (Bound.CURRENT_ROW == lowerLitType) {
      if (null != upperOp) {
        if (upperOp == PRECEDING_OPERATOR) {
          throw validator.newValidationError(upperBound,
              RESOURCE.currentRowPrecedingError());
        }
      }
    } else if (null != lowerOp) {
      if (lowerOp == FOLLOWING_OPERATOR) {
        if (null != upperOp) {
          if (upperOp == PRECEDING_OPERATOR) {
            throw validator.newValidationError(upperBound,
                RESOURCE.followingBeforePrecedingError());
          }
        } else if (null != upperLitType) {
          if (Bound.CURRENT_ROW == upperLitType) {
            throw validator.newValidationError(upperBound,
                RESOURCE.currentRowFollowingError());
          }
        }
      }
    }
  }

  public static SqlNode createCurrentRow(SqlParserPos pos) {
    return Bound.CURRENT_ROW.symbol(pos);
  }

  public static SqlNode createUnboundedFollowing(SqlParserPos pos) {
    return Bound.UNBOUNDED_FOLLOWING.symbol(pos);
  }

  public static SqlNode createUnboundedPreceding(SqlParserPos pos) {
    return Bound.UNBOUNDED_PRECEDING.symbol(pos);
  }

  public static SqlNode createFollowing(SqlNode e, SqlParserPos pos) {
    return FOLLOWING_OPERATOR.createCall(pos, e);
  }

  public static SqlNode createPreceding(SqlNode e, SqlParserPos pos) {
    return PRECEDING_OPERATOR.createCall(pos, e);
  }

  public static SqlNode createBound(SqlLiteral range) {
    return range;
  }

  /**
   * Returns whether an expression represents the "CURRENT ROW" bound.
   */
  public static boolean isCurrentRow(SqlNode node) {
    return (node instanceof SqlLiteral)
        && ((SqlLiteral) node).symbolValue(Bound.class) == Bound.CURRENT_ROW;
  }

  /**
   * Returns whether an expression represents the "UNBOUNDED PRECEDING" bound.
   */
  public static boolean isUnboundedPreceding(SqlNode node) {
    return (node instanceof SqlLiteral)
        && ((SqlLiteral) node).symbolValue(Bound.class) == Bound.UNBOUNDED_PRECEDING;
  }

  /**
   * Returns whether an expression represents the "UNBOUNDED FOLLOWING" bound.
   */
  public static boolean isUnboundedFollowing(SqlNode node) {
    return (node instanceof SqlLiteral)
        && ((SqlLiteral) node).symbolValue(Bound.class) == Bound.UNBOUNDED_FOLLOWING;
  }

  /**
   * Creates a new window by combining this one with another.
   *
   * <p>For example,
   *
   * <blockquote><pre>WINDOW (w PARTITION BY x ORDER BY y)
   *   overlay
   *   WINDOW w AS (PARTITION BY z)</pre></blockquote>
   *
   * <p>yields
   *
   * <blockquote><pre>WINDOW (PARTITION BY z ORDER BY y)</pre></blockquote>
   *
   * <p>Does not alter this or the other window.
   *
   * @return A new window
   */
  public SqlWindow overlay(SqlWindow that, SqlValidator validator) {
    // check 7.11 rule 10c
    final SqlNodeList partitions = getPartitionList();
    if (0 != partitions.size()) {
      throw validator.newValidationError(partitions.get(0),
          RESOURCE.partitionNotAllowed());
    }

    // 7.11 rule 10d
    final SqlNodeList baseOrder = getOrderList();
    final SqlNodeList refOrder = that.getOrderList();
    if ((0 != baseOrder.size()) && (0 != refOrder.size())) {
      throw validator.newValidationError(baseOrder.get(0),
          RESOURCE.orderByOverlap());
    }

    // 711 rule 10e
    final SqlNode lowerBound = that.getLowerBound();
    final SqlNode upperBound = that.getUpperBound();
    if ((null != lowerBound) || (null != upperBound)) {
      throw validator.newValidationError(that.isRows,
          RESOURCE.refWindowWithFrame());
    }

    SqlIdentifier declNameNew = declName;
    SqlIdentifier refNameNew = refName;
    SqlNodeList partitionListNew = partitionList;
    SqlNodeList orderListNew = orderList;
    SqlLiteral isRowsNew = isRows;
    SqlNode lowerBoundNew = lowerBound;
    SqlNode upperBoundNew = upperBound;
    SqlLiteral allowPartialNew = allowPartial;

    // Clear the reference window, because the reference is now resolved.
    // The overlaying window may have its own reference, of course.
    refNameNew = null;

    // Overlay other parameters.
    if (setOperand(partitionListNew, that.partitionList, validator)) {
      partitionListNew = that.partitionList;
    }
    if (setOperand(orderListNew, that.orderList, validator)) {
      orderListNew = that.orderList;
    }
    if (setOperand(lowerBoundNew, that.lowerBound, validator)) {
      lowerBoundNew = that.lowerBound;
    }
    if (setOperand(upperBoundNew, that.upperBound, validator)) {
      upperBoundNew = that.upperBound;
    }
    return new SqlWindow(
        SqlParserPos.ZERO,
        declNameNew,
        refNameNew,
        partitionListNew,
        orderListNew,
        isRowsNew,
        lowerBoundNew,
        upperBoundNew,
        allowPartialNew);
  }

  private static boolean setOperand(SqlNode clonedOperand, SqlNode thatOperand,
      SqlValidator validator) {
    if ((thatOperand != null) && !SqlNodeList.isEmptyList(thatOperand)) {
      if ((clonedOperand == null)
          || SqlNodeList.isEmptyList(clonedOperand)) {
        return true;
      } else {
        throw validator.newValidationError(clonedOperand,
            RESOURCE.cannotOverrideWindowAttribute());
      }
    }
    return false;
  }

  /**
   * Overridden method to specifically check only the right subtree of a window
   * definition.
   *
   * @param node The SqlWindow to compare to "this" window
   * @param litmus What to do if an error is detected (nodes are not equal)
   *
   * @return boolean true if all nodes in the subtree are equal
   */
  @Override public boolean equalsDeep(SqlNode node, Litmus litmus) {
    // This is the difference over super.equalsDeep.  It skips
    // operands[0] the declared name fo this window.  We only want
    // to check the window components.
    return node == this
        || node instanceof SqlWindow
        && SqlNode.equalDeep(
            Util.skip(getOperandList()),
            Util.skip(((SqlWindow) node).getOperandList()), litmus);
  }

  /**
   * Returns whether partial windows are allowed. If false, a partial window
   * (for example, a window of size 1 hour which has only 45 minutes of data
   * in it) will appear to windowed aggregate functions to be empty.
   */
  public boolean isAllowPartial() {
    // Default (and standard behavior) is to allow partial windows.
    return allowPartial == null
        || allowPartial.booleanValue();
  }

  @Override public void validate(SqlValidator validator,
      SqlValidatorScope scope) {
    SqlValidatorScope operandScope = scope; // REVIEW

    SqlIdentifier declName = this.declName;
    SqlIdentifier refName = this.refName;
    SqlNodeList partitionList = this.partitionList;
    SqlNodeList orderList = this.orderList;
    SqlLiteral isRows = this.isRows;
    SqlNode lowerBound = this.lowerBound;
    SqlNode upperBound = this.upperBound;
    SqlLiteral allowPartial = this.allowPartial;

    if (refName != null) {
      SqlWindow win = validator.resolveWindow(this, operandScope, false);
      partitionList = win.partitionList;
      orderList = win.orderList;
      isRows = win.isRows;
      lowerBound = win.lowerBound;
      upperBound = win.upperBound;
      allowPartial = win.allowPartial;
    }

    for (SqlNode partitionItem : partitionList) {
      try {
        partitionItem.accept(Util.OverFinder.INSTANCE);
      } catch (ControlFlowException e) {
        throw validator.newValidationError(this,
            RESOURCE.partitionbyShouldNotContainOver());
      }

      partitionItem.validateExpr(validator, operandScope);
    }

    for (SqlNode orderItem : orderList) {
      boolean savedColumnReferenceExpansion =
          validator.getColumnReferenceExpansion();
      validator.setColumnReferenceExpansion(false);
      try {
        orderItem.accept(Util.OverFinder.INSTANCE);
      } catch (ControlFlowException e) {
        throw validator.newValidationError(this,
            RESOURCE.orderbyShouldNotContainOver());
      }

      try {
        orderItem.validateExpr(validator, scope);
      } finally {
        validator.setColumnReferenceExpansion(
            savedColumnReferenceExpansion);
      }
    }

    // 6.10 rule 6a Function RANK & DENSE_RANK require ORDER BY clause
    if (orderList.size() == 0
        && !SqlValidatorUtil.containsMonotonic(scope)
        && windowCall != null
        && windowCall.getOperator().requiresOrder()) {
      throw validator.newValidationError(this, RESOURCE.funcNeedsOrderBy());
    }

    // Run framing checks if there are any
    if (upperBound != null || lowerBound != null) {
      // 6.10 Rule 6a RANK & DENSE_RANK do not allow ROWS or RANGE
      if (windowCall != null && !windowCall.getOperator().allowsFraming()) {
        throw validator.newValidationError(isRows, RESOURCE.rankWithFrame());
      }
      SqlTypeFamily orderTypeFam = null;

      // SQL03 7.10 Rule 11a
      if (orderList.size() > 0) {
        // if order by is a compound list then range not allowed
        if (orderList.size() > 1 && !isRows()) {
          throw validator.newValidationError(isRows,
              RESOURCE.compoundOrderByProhibitsRange());
        }

        // get the type family for the sort key for Frame Boundary Val.
        RelDataType orderType =
            validator.deriveType(
                operandScope,
                orderList.get(0));
        orderTypeFam = orderType.getSqlTypeName().getFamily();
      } else {
        // requires an ORDER BY clause if frame is logical(RANGE)
        // We relax this requirement if the table appears to be
        // sorted already
        if (!isRows() && !SqlValidatorUtil.containsMonotonic(scope)) {
          throw validator.newValidationError(this,
              RESOURCE.overMissingOrderBy());
        }
      }

      // Let the bounds validate themselves
      validateFrameBoundary(
          lowerBound,
          isRows(),
          orderTypeFam,
          validator,
          operandScope);
      validateFrameBoundary(
          upperBound,
          isRows(),
          orderTypeFam,
          validator,
          operandScope);

      // Validate across boundaries. 7.10 Rule 8 a-d
      checkSpecialLiterals(this, validator);
    } else if (orderList.size() == 0
        && !SqlValidatorUtil.containsMonotonic(scope)
        && windowCall != null
        && windowCall.getOperator().requiresOrder()) {
      throw validator.newValidationError(this, RESOURCE.overMissingOrderBy());
    }

    if (!isRows() && !isAllowPartial()) {
      throw validator.newValidationError(allowPartial,
          RESOURCE.cannotUseDisallowPartialWithRange());
    }
  }

  private void validateFrameBoundary(
      SqlNode bound,
      boolean isRows,
      SqlTypeFamily orderTypeFam,
      SqlValidator validator,
      SqlValidatorScope scope) {
    if (null == bound) {
      return;
    }
    bound.validate(validator, scope);
    switch (bound.getKind()) {
    case LITERAL:
      // is there really anything to validate here? this covers
      // "CURRENT_ROW","unbounded preceding" & "unbounded following"
      break;

    case OTHER:
    case FOLLOWING:
    case PRECEDING:
      assert bound instanceof SqlCall;
      final SqlNode boundVal = ((SqlCall) bound).operand(0);

      // SQL03 7.10 rule 11b Physical ROWS must be a numeric constant. JR:
      // actually it's SQL03 7.11 rule 11b "exact numeric with scale 0"
      // means not only numeric constant but exact numeric integral
      // constant. We also interpret the spec. to not allow negative
      // values, but allow zero.
      if (isRows) {
        if (boundVal instanceof SqlNumericLiteral) {
          final SqlNumericLiteral boundLiteral =
              (SqlNumericLiteral) boundVal;
          if ((!boundLiteral.isExact())
              || (boundLiteral.getScale() != 0)
              || (0 > boundLiteral.longValue(true))) {
            // true == throw if not exact (we just tested that - right?)
            throw validator.newValidationError(boundVal,
                RESOURCE.rowMustBeNonNegativeIntegral());
          }
        } else {
          // Allow expressions in ROWS clause
        }
      }

      // if this is a range spec check and make sure the boundary type
      // and order by type are compatible
      if (orderTypeFam != null && !isRows) {
        RelDataType bndType = validator.deriveType(scope, boundVal);
        SqlTypeFamily bndTypeFam = bndType.getSqlTypeName().getFamily();
        switch (orderTypeFam) {
        case NUMERIC:
          if (SqlTypeFamily.NUMERIC != bndTypeFam) {
            throw validator.newValidationError(boundVal,
                RESOURCE.orderByRangeMismatch());
          }
          break;
        case DATE:
        case TIME:
        case TIMESTAMP:
          if (SqlTypeFamily.INTERVAL_DAY_TIME != bndTypeFam
              && SqlTypeFamily.INTERVAL_YEAR_MONTH != bndTypeFam) {
            throw validator.newValidationError(boundVal,
                RESOURCE.orderByRangeMismatch());
          }
          break;
        default:
          throw validator.newValidationError(boundVal,
              RESOURCE.orderByDataTypeProhibitsRange());
        }
      }
      break;
    default:
      throw new AssertionError("Unexpected node type");
    }
  }

  /**
   * Creates a window <code>(RANGE <i>columnName</i> CURRENT ROW)</code>.
   *
   * @param columnName Order column
   */
  public SqlWindow createCurrentRowWindow(final String columnName) {
    return SqlWindow.create(
        null,
        null,
        new SqlNodeList(SqlParserPos.ZERO),
        new SqlNodeList(
            ImmutableList.of(
                new SqlIdentifier(columnName, SqlParserPos.ZERO)),
            SqlParserPos.ZERO),
        SqlLiteral.createBoolean(true, SqlParserPos.ZERO),
        SqlWindow.createCurrentRow(SqlParserPos.ZERO),
        SqlWindow.createCurrentRow(SqlParserPos.ZERO),
        SqlLiteral.createBoolean(true, SqlParserPos.ZERO),
        SqlParserPos.ZERO);
  }

  /**
   * Creates a window <code>(RANGE <i>columnName</i> UNBOUNDED
   * PRECEDING)</code>.
   *
   * @param columnName Order column
   */
  public SqlWindow createUnboundedPrecedingWindow(final String columnName) {
    return SqlWindow.create(
        null,
        null,
        new SqlNodeList(SqlParserPos.ZERO),
        new SqlNodeList(
            ImmutableList.of(
                new SqlIdentifier(columnName, SqlParserPos.ZERO)),
            SqlParserPos.ZERO),
        SqlLiteral.createBoolean(false, SqlParserPos.ZERO),
        SqlWindow.createUnboundedPreceding(SqlParserPos.ZERO),
        SqlWindow.createCurrentRow(SqlParserPos.ZERO),
        SqlLiteral.createBoolean(false, SqlParserPos.ZERO),
        SqlParserPos.ZERO);
  }

  /**
   * Fill in missing bounds. Default bounds are "BETWEEN UNBOUNDED PRECEDING
   * AND CURRENT ROW" when ORDER BY present and "BETWEEN UNBOUNDED PRECEDING
   * AND UNBOUNDED FOLLOWING" when no ORDER BY present.
   */
  public void populateBounds() {
    if (lowerBound == null && upperBound == null) {
      setLowerBound(
          SqlWindow.createUnboundedPreceding(getParserPosition()));
    }
    if (lowerBound == null) {
      setLowerBound(
          SqlWindow.createCurrentRow(getParserPosition()));
    }
    if (upperBound == null) {
      SqlParserPos pos = orderList.getParserPosition();
      setUpperBound(
          orderList.size() == 0
              ? SqlWindow.createUnboundedFollowing(pos)
              : SqlWindow.createCurrentRow(pos));
    }
  }

  /**
   * An enumeration of types of bounds in a window: <code>CURRENT ROW</code>,
   * <code>UNBOUNDED PRECEDING</code>, and <code>UNBOUNDED FOLLOWING</code>.
   */
  enum Bound {
    CURRENT_ROW("CURRENT ROW"),
    UNBOUNDED_PRECEDING("UNBOUNDED PRECEDING"),
    UNBOUNDED_FOLLOWING("UNBOUNDED FOLLOWING");

    private final String sql;

    Bound(String sql) {
      this.sql = sql;
    }

    public String toString() {
      return sql;
    }

    /**
     * Creates a parse-tree node representing an occurrence of this bound
     * type at a particular position in the parsed text.
     */
    public SqlNode symbol(SqlParserPos pos) {
      return SqlLiteral.createSymbol(this, pos);
    }
  }

  /** An operator describing a window specification. */
  private static class SqlWindowOperator extends SqlOperator {
    private static final SqlWindowOperator INSTANCE = new SqlWindowOperator();

    private SqlWindowOperator() {
      super("WINDOW", SqlKind.WINDOW, 2, true, null, null, null);
    }

    public SqlSyntax getSyntax() {
      return SqlSyntax.SPECIAL;
    }

    public SqlCall createCall(
        SqlLiteral functionQualifier,
        SqlParserPos pos,
        SqlNode... operands) {
      assert functionQualifier == null;
      assert operands.length == 8;
      return create(
          (SqlIdentifier) operands[0],
          (SqlIdentifier) operands[1],
          (SqlNodeList) operands[2],
          (SqlNodeList) operands[3],
          (SqlLiteral) operands[4],
          operands[5],
          operands[6],
          (SqlLiteral) operands[7],
          pos);
    }

    public <R> void acceptCall(
        SqlVisitor<R> visitor,
        SqlCall call,
        boolean onlyExpressions,
        SqlBasicVisitor.ArgHandler<R> argHandler) {
      if (onlyExpressions) {
        for (Ord<SqlNode> operand : Ord.zip(call.getOperandList())) {
          // if the second param is an Identifier then it's supposed to
          // be a name from a window clause and isn't part of the
          // group by check
          if (operand.e == null) {
            continue;
          }
          if (operand.i == 1 && operand.e instanceof SqlIdentifier) {
            // skip refName
            continue;
          }
          argHandler.visitChild(visitor, call, operand.i, operand.e);
        }
      } else {
        super.acceptCall(visitor, call, onlyExpressions, argHandler);
      }
    }

    public void unparse(
        SqlWriter writer,
        SqlCall call,
        int leftPrec,
        int rightPrec) {
      final SqlWindow window = (SqlWindow) call;
      final SqlWriter.Frame frame =
          writer.startList(SqlWriter.FrameTypeEnum.WINDOW, "(", ")");
      if (window.refName != null) {
        window.refName.unparse(writer, 0, 0);
      }
      if (window.partitionList.size() > 0) {
        writer.sep("PARTITION BY");
        final SqlWriter.Frame partitionFrame = writer.startList("", "");
        window.partitionList.unparse(writer, 0, 0);
        writer.endList(partitionFrame);
      }
      if (window.orderList.size() > 0) {
        writer.sep("ORDER BY");
        final SqlWriter.Frame orderFrame = writer.startList("", "");
        window.orderList.unparse(writer, 0, 0);
        writer.endList(orderFrame);
      }
      if (window.lowerBound == null) {
        // No ROWS or RANGE clause
      } else if (window.upperBound == null) {
        if (window.isRows()) {
          writer.sep("ROWS");
        } else {
          writer.sep("RANGE");
        }
        window.lowerBound.unparse(writer, 0, 0);
      } else {
        if (window.isRows()) {
          writer.sep("ROWS BETWEEN");
        } else {
          writer.sep("RANGE BETWEEN");
        }
        window.lowerBound.unparse(writer, 0, 0);
        writer.keyword("AND");
        window.upperBound.unparse(writer, 0, 0);
      }

      // ALLOW PARTIAL/DISALLOW PARTIAL
      if (window.allowPartial == null) {
        // do nothing
      } else if (window.isAllowPartial()) {
        // We could output "ALLOW PARTIAL", but this syntax is
        // non-standard. Omitting the clause has the same effect.
      } else {
        writer.keyword("DISALLOW PARTIAL");
      }

      writer.endList(frame);
    }
  }
}

// End SqlWindow.java
