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
package org.apache.calcite.sql.util;

import org.apache.calcite.sql.SqlCall;
import org.apache.calcite.sql.SqlDataTypeSpec;
import org.apache.calcite.sql.SqlDynamicParam;
import org.apache.calcite.sql.SqlIdentifier;
import org.apache.calcite.sql.SqlIntervalQualifier;
import org.apache.calcite.sql.SqlLiteral;
import org.apache.calcite.sql.SqlNode;
import org.apache.calcite.sql.SqlNodeList;

/**
 * Basic implementation of {@link SqlVisitor} which does nothing at each node.
 *
 * <p>This class is useful as a base class for classes which implement the
 * {@link SqlVisitor} interface. The derived class can override whichever
 * methods it chooses.
 *
 * @param <R> Return type
 */
public class SqlBasicVisitor<R> implements SqlVisitor<R> {
  //~ Methods ----------------------------------------------------------------

  public R visit(SqlLiteral literal) {
    return null;
  }

  public R visit(SqlCall call) {
    return call.getOperator().acceptCall(this, call);
  }

  public R visit(SqlNodeList nodeList) {
    R result = null;
    for (int i = 0; i < nodeList.size(); i++) {
      SqlNode node = nodeList.get(i);
      result = node.accept(this);
    }
    return result;
  }

  public R visit(SqlIdentifier id) {
    return null;
  }

  public R visit(SqlDataTypeSpec type) {
    return null;
  }

  public R visit(SqlDynamicParam param) {
    return null;
  }

  public R visit(SqlIntervalQualifier intervalQualifier) {
    return null;
  }

  //~ Inner Interfaces -------------------------------------------------------

  /** Argument handler.
   *
   * @param <R> result type */
  public interface ArgHandler<R> {
    /** Returns the result of visiting all children of a call to an operator,
     * then the call itself.
     *
     * <p>Typically the result will be the result of the last child visited, or
     * (if R is {@link Boolean}) whether all children were visited
     * successfully. */
    R result();

    /** Visits a particular operand of a call, using a given visitor. */
    R visitChild(
        SqlVisitor<R> visitor,
        SqlNode expr,
        int i,
        SqlNode operand);
  }

  //~ Inner Classes ----------------------------------------------------------

  /**
   * Default implementation of {@link ArgHandler} which merely calls
   * {@link SqlNode#accept} on each operand.
   *
   * @param <R> result type
   */
  public static class ArgHandlerImpl<R> implements ArgHandler<R> {
    private static final ArgHandler INSTANCE = new ArgHandlerImpl();

    @SuppressWarnings("unchecked")
    public static <R> ArgHandler<R> instance() {
      return INSTANCE;
    }

    public R result() {
      return null;
    }

    public R visitChild(
        SqlVisitor<R> visitor,
        SqlNode expr,
        int i,
        SqlNode operand) {
      if (operand == null) {
        return null;
      }
      return operand.accept(visitor);
    }
  }
}

// End SqlBasicVisitor.java
