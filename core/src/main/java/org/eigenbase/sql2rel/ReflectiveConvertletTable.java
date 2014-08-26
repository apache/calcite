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
package org.eigenbase.sql2rel;

import java.lang.reflect.*;

import java.util.*;

import org.eigenbase.rex.*;
import org.eigenbase.sql.*;
import org.eigenbase.sql.parser.*;
import org.eigenbase.util.*;

/**
 * Implementation of {@link SqlRexConvertletTable} which uses reflection to call
 * any method of the form <code>public RexNode convertXxx(ConvertletContext,
 * SqlNode)</code> or <code>public RexNode convertXxx(ConvertletContext,
 * SqlOperator, SqlCall)</code>.
 */
public class ReflectiveConvertletTable implements SqlRexConvertletTable {
  //~ Instance fields --------------------------------------------------------

  private final Map<Object, Object> map = new HashMap<Object, Object>();

  //~ Constructors -----------------------------------------------------------

  public ReflectiveConvertletTable() {
    for (final Method method : getClass().getMethods()) {
      registerNodeTypeMethod(method);
      registerOpTypeMethod(method);
    }
  }

  //~ Methods ----------------------------------------------------------------

  /**
   * Registers method if it: a. is public, and b. is named "convertXxx", and
   * c. has a return type of "RexNode" or a subtype d. has a 2 parameters with
   * types ConvertletContext and SqlNode (or a subtype) respectively.
   */
  private void registerNodeTypeMethod(final Method method) {
    if (!Modifier.isPublic(method.getModifiers())) {
      return;
    }
    if (!method.getName().startsWith("convert")) {
      return;
    }
    if (!RexNode.class.isAssignableFrom(method.getReturnType())) {
      return;
    }
    final Class[] parameterTypes = method.getParameterTypes();
    if (parameterTypes.length != 2) {
      return;
    }
    if (parameterTypes[0] != SqlRexContext.class) {
      return;
    }
    final Class parameterType = parameterTypes[1];
    if (!SqlNode.class.isAssignableFrom(parameterType)) {
      return;
    }
    map.put(
        parameterType,
        new SqlRexConvertlet() {
          public RexNode convertCall(
              SqlRexContext cx,
              SqlCall call) {
            try {
              return (RexNode) method.invoke(
                  ReflectiveConvertletTable.this,
                  cx,
                  call);
            } catch (IllegalAccessException e) {
              throw Util.newInternal(
                  e,
                  "while converting " + call);
            } catch (InvocationTargetException e) {
              throw Util.newInternal(
                  e,
                  "while converting " + call);
            }
          }
        });
  }

  /**
   * Registers method if it: a. is public, and b. is named "convertXxx", and
   * c. has a return type of "RexNode" or a subtype d. has a 3 parameters with
   * types: ConvertletContext; SqlOperator (or a subtype), SqlCall (or a
   * subtype).
   */
  private void registerOpTypeMethod(final Method method) {
    if (!Modifier.isPublic(method.getModifiers())) {
      return;
    }
    if (!method.getName().startsWith("convert")) {
      return;
    }
    if (!RexNode.class.isAssignableFrom(method.getReturnType())) {
      return;
    }
    final Class[] parameterTypes = method.getParameterTypes();
    if (parameterTypes.length != 3) {
      return;
    }
    if (parameterTypes[0] != SqlRexContext.class) {
      return;
    }
    final Class opClass = parameterTypes[1];
    if (!SqlOperator.class.isAssignableFrom(opClass)) {
      return;
    }
    final Class parameterType = parameterTypes[2];
    if (!SqlCall.class.isAssignableFrom(parameterType)) {
      return;
    }
    map.put(
        opClass,
        new SqlRexConvertlet() {
          public RexNode convertCall(
              SqlRexContext cx,
              SqlCall call) {
            try {
              return (RexNode) method.invoke(
                  ReflectiveConvertletTable.this,
                  cx,
                  call.getOperator(),
                  call);
            } catch (IllegalAccessException e) {
              throw Util.newInternal(
                  e,
                  "while converting " + call);
            } catch (InvocationTargetException e) {
              throw Util.newInternal(
                  e,
                  "while converting " + call);
            }
          }
        });
  }

  public SqlRexConvertlet get(SqlCall call) {
    SqlRexConvertlet convertlet;
    final SqlOperator op = call.getOperator();

    // Is there a convertlet for this operator
    // (e.g. SqlStdOperatorTable.plusOperator)?
    convertlet = (SqlRexConvertlet) map.get(op);
    if (convertlet != null) {
      return convertlet;
    }

    // Is there a convertlet for this class of operator
    // (e.g. SqlBinaryOperator)?
    Class<? extends Object> clazz = op.getClass();
    while (clazz != null) {
      convertlet = (SqlRexConvertlet) map.get(clazz);
      if (convertlet != null) {
        return convertlet;
      }
      clazz = clazz.getSuperclass();
    }

    // Is there a convertlet for this class of expression
    // (e.g. SqlCall)?
    clazz = call.getClass();
    while (clazz != null) {
      convertlet = (SqlRexConvertlet) map.get(clazz);
      if (convertlet != null) {
        return convertlet;
      }
      clazz = clazz.getSuperclass();
    }
    return null;
  }

  /**
   * Registers a convertlet for a given operator instance
   *
   * @param op         Operator instance, say {@link
   *                   org.eigenbase.sql.fun.SqlStdOperatorTable#MINUS}
   * @param convertlet Convertlet
   */
  protected void registerOp(SqlOperator op, SqlRexConvertlet convertlet) {
    map.put(op, convertlet);
  }

  /**
   * Registers that one operator is an alias for another.
   *
   * @param alias  Operator which is alias
   * @param target Operator to translate calls to
   */
  protected void addAlias(final SqlOperator alias, final SqlOperator target) {
    map.put(
        alias,
        new SqlRexConvertlet() {
          public RexNode convertCall(
              SqlRexContext cx,
              SqlCall call) {
            Util.permAssert(
                call.getOperator() == alias,
                "call to wrong operator");
            final SqlCall newCall =
                target.createCall(
                    SqlParserPos.ZERO,
                    call.getOperandList());
            return cx.convertExpression(newCall);
          }
        });
  }
}

// End ReflectiveConvertletTable.java
