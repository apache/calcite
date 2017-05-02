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

import org.apache.calcite.sql.validate.SqlConformance;
import org.apache.calcite.util.Util;

/**
 * Enumeration of possible syntactic types of {@link SqlOperator operators}.
 */
public enum SqlSyntax {
  /**
   * Function syntax, as in "Foo(x, y)".
   */
  FUNCTION {
    public void unparse(
        SqlWriter writer,
        SqlOperator operator,
        SqlCall call,
        int leftPrec,
        int rightPrec) {
      SqlUtil.unparseFunctionSyntax(operator, writer, call);
    }
  },

  /**
   * Function syntax, as in "Foo(x, y)", but uses "*" if there are no arguments,
   * for example "COUNT(*)".
   */
  FUNCTION_STAR {
    public void unparse(
        SqlWriter writer,
        SqlOperator operator,
        SqlCall call,
        int leftPrec,
        int rightPrec) {
      SqlUtil.unparseFunctionSyntax(operator, writer, call);
    }
  },

  /**
   * Binary operator syntax, as in "x + y".
   */
  BINARY {
    public void unparse(
        SqlWriter writer,
        SqlOperator operator,
        SqlCall call,
        int leftPrec,
        int rightPrec) {
      SqlUtil.unparseBinarySyntax(operator, call, writer, leftPrec, rightPrec);
    }
  },

  /**
   * Prefix unary operator syntax, as in "- x".
   */
  PREFIX {
    public void unparse(
        SqlWriter writer,
        SqlOperator operator,
        SqlCall call,
        int leftPrec,
        int rightPrec) {
      assert call.operandCount() == 1;
      writer.keyword(operator.getName());
      call.operand(0).unparse(writer, operator.getLeftPrec(),
          operator.getRightPrec());
    }
  },

  /**
   * Postfix unary operator syntax, as in "x ++".
   */
  POSTFIX {
    public void unparse(
        SqlWriter writer,
        SqlOperator operator,
        SqlCall call,
        int leftPrec,
        int rightPrec) {
      assert call.operandCount() == 1;
      call.operand(0).unparse(writer, operator.getLeftPrec(),
          operator.getRightPrec());
      writer.keyword(operator.getName());
    }
  },

  /**
   * Special syntax, such as that of the SQL CASE operator, "CASE x WHEN 1
   * THEN 2 ELSE 3 END".
   */
  SPECIAL {
    public void unparse(
        SqlWriter writer,
        SqlOperator operator,
        SqlCall call,
        int leftPrec,
        int rightPrec) {
      // You probably need to override the operator's unparse
      // method.
      throw Util.needToImplement(this);
    }
  },

  /**
   * Function syntax which takes no parentheses if there are no arguments, for
   * example "CURRENTTIME".
   *
   * @see SqlConformance#allowNiladicParentheses()
   */
  FUNCTION_ID {
    public void unparse(
        SqlWriter writer,
        SqlOperator operator,
        SqlCall call,
        int leftPrec,
        int rightPrec) {
      SqlUtil.unparseFunctionSyntax(operator, writer, call);
    }
  },

  /**
   * Syntax of an internal operator, which does not appear in the SQL.
   */
  INTERNAL {
    public void unparse(
        SqlWriter writer,
        SqlOperator operator,
        SqlCall call,
        int leftPrec,
        int rightPrec) {
      throw new UnsupportedOperationException("Internal operator '"
          + operator + "' " + "cannot be un-parsed");
    }
  };

  /**
   * Converts a call to an operator of this syntax into a string.
   */
  public abstract void unparse(
      SqlWriter writer,
      SqlOperator operator,
      SqlCall call,
      int leftPrec,
      int rightPrec);
}

// End SqlSyntax.java
