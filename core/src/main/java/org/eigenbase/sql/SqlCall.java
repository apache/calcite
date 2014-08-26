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
package org.eigenbase.sql;

import java.util.*;

import org.eigenbase.reltype.*;
import org.eigenbase.sql.parser.*;
import org.eigenbase.sql.util.*;
import org.eigenbase.sql.validate.*;

/**
 * A <code>SqlCall</code> is a call to an {@link SqlOperator operator}.
 * (Operators can be used to describe any syntactic construct, so in practice,
 * every non-leaf node in a SQL parse tree is a <code>SqlCall</code> of some
 * kind.)
 */
public abstract class SqlCall extends SqlNode {
  //~ Constructors -----------------------------------------------------------

  public SqlCall(SqlParserPos pos) {
    super(pos);
  }

  //~ Methods ----------------------------------------------------------------

  /**
   * Whether this call was created by expanding a parentheses-free call to
   * what was syntactically an identifier.
   */
  public boolean isExpanded() {
    return false;
  }

  /**
   * Changes the value of an operand. Allows some rewrite by
   * {@link SqlValidator}; use sparingly.
   *
   * @param i Operand index
   * @param operand Operand value
   */
  public void setOperand(int i, SqlNode operand) {
    throw new UnsupportedOperationException();
  }

  public abstract SqlOperator getOperator();

  public abstract List<SqlNode> getOperandList();

  @SuppressWarnings("unchecked")
  public <S extends SqlNode> S operand(int i) {
    return (S) getOperandList().get(i);
  }

  public int operandCount() {
    return getOperandList().size();
  }

  public SqlNode clone(SqlParserPos pos) {
    return getOperator().createCall(pos, getOperandList());
  }

  public void unparse(
      SqlWriter writer,
      int leftPrec,
      int rightPrec) {
    final SqlOperator operator = getOperator();
    if (leftPrec > operator.getLeftPrec()
        || (operator.getRightPrec() <= rightPrec && (rightPrec != 0))
        || writer.isAlwaysUseParentheses() && isA(SqlKind.EXPRESSION)) {
      final SqlWriter.Frame frame = writer.startList("(", ")");
      operator.unparse(writer, this, 0, 0);
      writer.endList(frame);
    } else {
      operator.unparse(writer, this, leftPrec, rightPrec);
    }
  }

  /**
   * Validates this call.
   *
   * <p>The default implementation delegates the validation to the operator's
   * {@link SqlOperator#validateCall}. Derived classes may override (as do,
   * for example {@link SqlSelect} and {@link SqlUpdate}).
   */
  public void validate(SqlValidator validator, SqlValidatorScope scope) {
    validator.validateCall(this, scope);
  }

  public void findValidOptions(
      SqlValidator validator,
      SqlValidatorScope scope,
      SqlParserPos pos,
      List<SqlMoniker> hintList) {
    for (SqlNode operand : getOperandList()) {
      if (operand instanceof SqlIdentifier) {
        SqlIdentifier id = (SqlIdentifier) operand;
        SqlParserPos idPos = id.getParserPosition();
        if (idPos.toString().equals(pos.toString())) {
          ((SqlValidatorImpl) validator).lookupNameCompletionHints(
              scope, id.names, pos, hintList);
          return;
        }
      }
    }
    // no valid options
  }

  public <R> R accept(SqlVisitor<R> visitor) {
    return visitor.visit(this);
  }

  public boolean equalsDeep(SqlNode node, boolean fail) {
    if (node == this) {
      return true;
    }
    if (!(node instanceof SqlCall)) {
      assert !fail : this + "!=" + node;
      return false;
    }
    SqlCall that = (SqlCall) node;

    // Compare operators by name, not identity, because they may not
    // have been resolved yet.
    if (!this.getOperator().getName().equals(that.getOperator().getName())) {
      assert !fail : this + "!=" + node;
      return false;
    }
    return equalDeep(this.getOperandList(), that.getOperandList(), fail);
  }

  /**
   * Returns a string describing the actual argument types of a call, e.g.
   * "SUBSTR(VARCHAR(12), NUMBER(3,2), INTEGER)".
   */
  protected String getCallSignature(
      SqlValidator validator,
      SqlValidatorScope scope) {
    List<String> signatureList = new ArrayList<String>();
    for (final SqlNode operand : getOperandList()) {
      final RelDataType argType = validator.deriveType(scope, operand);
      if (null == argType) {
        continue;
      }
      signatureList.add(argType.toString());
    }
    return SqlUtil.getOperatorSignature(getOperator(), signatureList);
  }

  public SqlMonotonicity getMonotonicity(SqlValidatorScope scope) {
    // Delegate to operator.
    return getOperator().getMonotonicity(this, scope);
  }

  /**
   * Test to see if it is the function COUNT(*)
   *
   * @return boolean true if function call to COUNT(*)
   */
  public boolean isCountStar() {
    if (getOperator().isName("COUNT") && operandCount() == 1) {
      final SqlNode parm = operand(0);
      if (parm instanceof SqlIdentifier) {
        SqlIdentifier id = (SqlIdentifier) parm;
        if (id.isStar() && id.names.size() == 1) {
          return true;
        }
      }
    }

    return false;
  }

  public SqlLiteral getFunctionQuantifier() {
    return null;
  }
}

// End SqlCall.java
