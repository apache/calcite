/*
// Licensed to Julian Hyde under one or more contributor license
// agreements. See the NOTICE file distributed with this work for
// additional information regarding copyright ownership.
//
// Julian Hyde licenses this file to you under the Apache License,
// Version 2.0 (the "License"); you may not use this file except in
// compliance with the License. You may obtain a copy of the License at:
//
// http://www.apache.org/licenses/LICENSE-2.0
//
// Unless required by applicable law or agreed to in writing, software
// distributed under the License is distributed on an "AS IS" BASIS,
// WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
// See the License for the specific language governing permissions and
// limitations under the License.
*/
package org.eigenbase.sql;

import org.eigenbase.resource.*;
import org.eigenbase.sql.parser.*;
import org.eigenbase.sql.validate.*;
import org.eigenbase.util.*;

/**
 * SQL window specifcation.
 *
 * <p>For example, the query
 *
 * <blockquote>
 * <pre>SELECT sum(a) OVER (w ROWS 3 PRECEDING)
 * FROM t
 * WINDOW w AS (PARTITION BY x, y ORDER BY z),
 *     w1 AS (w ROWS 5 PRECEDING UNBOUNDED FOLLOWING)</pre>
 * </blockquote>
 *
 * declares windows w and w1, and uses a window in an OVER clause. It thus
 * contains 3 {@link SqlWindow} objects.</p>
 */
public class SqlWindow extends SqlCall {
  //~ Static fields/initializers ---------------------------------------------

  /** Describes the operands that are passed to a window call. */
  enum Operand {
    /** The operand that holds the name of the window being declared. */
    DECL_NAME,

    /** The operand that holds the name of the window being referenced, or
     * null. */
    REF_NAME,

    /** The operand that holds the list of partitioning columns. */
    PARTITION_LIST,

    /** The operand that holds the list of ordering columns. */
    ORDER_LIST,

    /** The operand that declares whether it is a physical (rows) or logical
     * (values) range. */
    IS_ROWS,

    /** The operand that holds the lower bound of the window. */
    LOWER_BOUND,

    /** The operand that holds the upper bound of the window. */
    UPPER_BOUND,

    /** The operand that declares whether to allow partial results.
     * It may be null. */
    ALLOW_PARTIAL;

    SqlNode get(SqlNode[] operands) {
      return operands[ordinal()];
    }

    void set(SqlNode[] operands, SqlNode value) {
      operands[ordinal()] = value;
    }
  }

  //~ Instance fields --------------------------------------------------------

  private SqlCall windowCall = null;

  //~ Constructors -----------------------------------------------------------

  /**
   * Creates a window.
   *
   * @pre operands[DeclName_OPERAND] == null ||
   * operands[DeclName_OPERAND].isSimple()
   * @pre operands[OrderList_OPERAND] != null
   * @pre operands[PartitionList_OPERAND] != null
   */
  public SqlWindow(
      SqlWindowOperator operator,
      SqlNode[] operands,
      SqlParserPos pos) {
    super(operator, operands, pos);
    assert operands.length == Operand.values().length;
    final SqlIdentifier declId =
        (SqlIdentifier) Operand.DECL_NAME.get(operands);
    assert declId == null || declId.isSimple() : declId;
    assert Operand.PARTITION_LIST.get(operands) != null;
    assert Operand.ORDER_LIST.get(operands) != null;
  }

  //~ Methods ----------------------------------------------------------------

  public void unparse(SqlWriter writer, int leftPrec, int rightPrec) {
    SqlIdentifier declName = (SqlIdentifier) Operand.DECL_NAME.get(operands);
    if (null != declName) {
      declName.unparse(writer, 0, 0);
      writer.keyword("AS");
    }

    // Override, so we don't print extra parentheses.
    getOperator().unparse(writer, operands, 0, 0);
  }

  public SqlIdentifier getDeclName() {
    return (SqlIdentifier) Operand.DECL_NAME.get(operands);
  }

  public void setDeclName(SqlIdentifier name) {
    Util.pre(
        name.isSimple(),
        "name.isSimple()");
    Operand.DECL_NAME.set(operands, name);
  }

  public SqlNode getLowerBound() {
    return Operand.LOWER_BOUND.get(operands);
  }

  public void setLowerBound(SqlNode bound) {
    Operand.LOWER_BOUND.set(operands, bound);
  }

  public SqlNode getUpperBound() {
    return Operand.UPPER_BOUND.get(operands);
  }

  public void setUpperBound(SqlNode bound) {
    Operand.UPPER_BOUND.set(operands, bound);
  }

  public boolean isRows() {
    return SqlLiteral.booleanValue(Operand.IS_ROWS.get(operands));
  }

  public SqlNodeList getOrderList() {
    return (SqlNodeList) Operand.ORDER_LIST.get(operands);
  }

  public SqlNodeList getPartitionList() {
    return (SqlNodeList) Operand.PARTITION_LIST.get(operands);
  }

  public SqlIdentifier getRefName() {
    return (SqlIdentifier) Operand.REF_NAME.get(operands);
  }

  public void setWindowCall(SqlCall windowCall) {
    this.windowCall = windowCall;
  }

  public SqlCall getWindowCall() {
    return windowCall;
  }

  /**
   * Creates a new window by combining this one with another.
   *
   * <p>For example,
   *
   * <pre>WINDOW (w PARTITION BY x ORDER BY y)
   *   overlay
   *   WINDOW w AS (PARTITION BY z)</pre>
   *
   * yields
   *
   * <pre>WINDOW (PARTITION BY z ORDER BY y)</pre>
   *
   * <p>Does not alter this or the other window.
   *
   * @return A new window
   */
  public SqlWindow overlay(SqlWindow that, SqlValidator validator) {
    // check 7.11 rule 10c
    final SqlNodeList partitions = getPartitionList();
    if (0 != partitions.size()) {
      throw validator.newValidationError(
          partitions.get(0),
          EigenbaseResource.instance().PartitionNotAllowed.ex());
    }

    // 7.11 rule 10d
    final SqlNodeList baseOrder = getOrderList();
    final SqlNodeList refOrder = that.getOrderList();
    if ((0 != baseOrder.size()) && (0 != refOrder.size())) {
      throw validator.newValidationError(
          baseOrder.get(0),
          EigenbaseResource.instance().OrderByOverlap.ex());
    }

    // 711 rule 10e
    final SqlNode lowerBound = that.getLowerBound();
    final SqlNode upperBound = that.getUpperBound();
    if ((null != lowerBound) || (null != upperBound)) {
      throw validator.newValidationError(Operand.IS_ROWS.get(that.operands),
          EigenbaseResource.instance().RefWindowWithFrame.ex());
    }

    final SqlNode[] newOperands = operands.clone();

    // Clear the reference window, because the reference is now resolved.
    // The overlaying window may have its own reference, of course.
    Operand.REF_NAME.set(newOperands, null);

    // Overlay other parameters.
    setOperand(
        newOperands,
        that.operands,
        Operand.PARTITION_LIST,
        validator);
    setOperand(newOperands, that.operands, Operand.ORDER_LIST, validator);
    setOperand(newOperands, that.operands, Operand.LOWER_BOUND, validator);
    setOperand(newOperands, that.operands, Operand.UPPER_BOUND, validator);
    return new SqlWindow(
        (SqlWindowOperator) getOperator(),
        newOperands,
        SqlParserPos.ZERO);
  }

  private static void setOperand(
      final SqlNode[] destOperands,
      SqlNode[] srcOperands,
      Operand i,
      SqlValidator validator) {
    final SqlNode thatOperand = i.get(srcOperands);
    if ((thatOperand != null) && !SqlNodeList.isEmptyList(thatOperand)) {
      final SqlNode clonedOperand = i.get(destOperands);
      if ((clonedOperand == null)
          || SqlNodeList.isEmptyList(clonedOperand)) {
        i.set(destOperands, thatOperand);
      } else {
        throw validator.newValidationError(
            clonedOperand,
            EigenbaseResource.instance().CannotOverrideWindowAttribute.ex());
      }
    }
  }

  /**
   * Overridden method to specfically check only the right subtree of a window
   * definition
   *
   * @param node The SqlWindow to compare to "this" window
   * @param fail Whether to throw if not equal
   * @return boolean true if all nodes in the subtree are equal
   */
  public boolean equalsDeep(SqlNode node, boolean fail) {
    if (!(node instanceof SqlWindow)) {
      assert !fail : this + "!=" + node;
      return false;
    }
    SqlCall that = (SqlCall) node;

    // Compare operators by name, not identity, because they may not
    // have been resolved yet.
    if (!this.getOperator().getName().equals(
        that.getOperator().getName())) {
      assert !fail : this + "!=" + node;
      return false;
    }
    if (this.operands.length != that.operands.length) {
      assert !fail : this + "!=" + node;
      return false;
    }

    // This is the difference over super.equalsDeep.  It skips
    // operands[0] the declared name fo this window.  We only want
    // to check the window components.
    for (int i = 1; i < this.operands.length; i++) {
      if (!SqlNode.equalDeep(this.operands[i], that.operands[i], fail)) {
        return false;
      }
    }
    return true;
  }

  public SqlWindowOperator.OffsetRange getOffsetAndRange() {
    return SqlWindowOperator.getOffsetAndRange(
        getLowerBound(),
        getUpperBound(),
        isRows());
  }

  /**
   * Returns whether partial windows are allowed. If false, a partial window
   * (for example, a window of size 1 hour which has only 45 minutes of data
   * in it) will appear to windowed aggregate functions to be empty.
   */
  public boolean isAllowPartial() {
    // Default (and standard behavior) is to allow partial windows.
    return Operand.ALLOW_PARTIAL.get(operands) == null
        || SqlLiteral.booleanValue(Operand.ALLOW_PARTIAL.get(operands));
  }
}

// End SqlWindow.java
