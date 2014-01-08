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
package org.eigenbase.sql.util;

import java.lang.reflect.*;

import java.util.*;

import org.eigenbase.sql.*;
import org.eigenbase.util.*;

/**
 * ReflectiveSqlOperatorTable implements the {@link SqlOperatorTable } interface
 * by reflecting the public fields of a subclass.
 */
public abstract class ReflectiveSqlOperatorTable implements SqlOperatorTable {
  public static final String IS_NAME = "INFORMATION_SCHEMA";

  //~ Instance fields --------------------------------------------------------

  private final MultiMap<String, SqlOperator> operators =
      new MultiMap<String, SqlOperator>();

  private final Map<String, SqlOperator> mapNameToOp =
      new HashMap<String, SqlOperator>();

  //~ Constructors -----------------------------------------------------------

  protected ReflectiveSqlOperatorTable() {
  }

  //~ Methods ----------------------------------------------------------------

  /**
   * Performs post-constructor initialization of an operator table. It can't
   * be part of the constructor, because the subclass constructor needs to
   * complete first.
   */
  public final void init() {
    // Use reflection to register the expressions stored in public fields.
    for (Field field : getClass().getFields()) {
      try {
        if (SqlFunction.class.isAssignableFrom(field.getType())) {
          SqlFunction op = (SqlFunction) field.get(this);
          if (op != null) {
            register(op);
          }
        } else if (
            SqlOperator.class.isAssignableFrom(field.getType())) {
          SqlOperator op = (SqlOperator) field.get(this);
          register(op);
        }
      } catch (IllegalArgumentException e) {
        throw Util.newInternal(
            e,
            "Error while initializing operator table");
      } catch (IllegalAccessException e) {
        throw Util.newInternal(
            e,
            "Error while initializing operator table");
      }
    }
  }

  // implement SqlOperatorTable
  public List<SqlOperator> lookupOperatorOverloads(
      SqlIdentifier opName,
      SqlFunctionCategory category,
      SqlSyntax syntax) {
    // NOTE jvs 3-Mar-2005:  ignore category until someone cares

    List<SqlOperator> overloads = new ArrayList<SqlOperator>();
    String simpleName;
    if (opName.names.size() > 1) {
      if (opName.names.get(opName.names.size() - 2).equals(IS_NAME)) {
        // per SQL99 Part 2 Section 10.4 Syntax Rule 7.b.ii.1
        simpleName = Util.last(opName.names);
      } else {
        return overloads;
      }
    } else {
      simpleName = opName.getSimple();
    }
    final List<SqlOperator> list = operators.getMulti(simpleName);
    for (SqlOperator op : list) {
      if (op.getSyntax() == syntax) {
        overloads.add(op);
      } else if (syntax == SqlSyntax.Function
          && op instanceof SqlFunction) {
        // this special case is needed for operators like CAST,
        // which are treated as functions but have special syntax
        overloads.add(op);
      }
    }

    // REVIEW jvs 1-Jan-2005:  why is this extra lookup required?
    // Shouldn't it be covered by search above?
    SqlOperator extra;
    switch (syntax) {
    case Binary:
      extra = mapNameToOp.get(simpleName + ":BINARY");
      break;
    case Prefix:
      extra = mapNameToOp.get(simpleName + ":PREFIX");
      break;
    case Postfix:
      extra = mapNameToOp.get(simpleName + ":POSTFIX");
      break;
    default:
      extra = null;
      break;
    }

    if ((extra != null) && !overloads.contains(extra)) {
      overloads.add(extra);
    }

    return overloads;
  }

  public void register(SqlOperator op) {
    operators.putMulti(
        op.getName(),
        op);
    if (op instanceof SqlBinaryOperator) {
      mapNameToOp.put(op.getName() + ":BINARY", op);
    } else if (op instanceof SqlPrefixOperator) {
      mapNameToOp.put(op.getName() + ":PREFIX", op);
    } else if (op instanceof SqlPostfixOperator) {
      mapNameToOp.put(op.getName() + ":POSTFIX", op);
    }
  }

  /**
   * Registers a function in the table.
   *
   * @param function Function to register
   */
  public void register(SqlFunction function) {
    operators.putMulti(
        function.getName(),
        function);
    SqlFunctionCategory funcType = function.getFunctionType();
    assert (funcType != null) : "Function type for " + function.getName()
        + " not set";
  }

  // implement SqlOperatorTable
  public List<SqlOperator> getOperatorList() {
    List<SqlOperator> list = new ArrayList<SqlOperator>();

    Iterator<Map.Entry<String, SqlOperator>> it =
        operators.entryIterMulti();
    while (it.hasNext()) {
      list.add(it.next().getValue());
    }

    return list;
  }
}

// End ReflectiveSqlOperatorTable.java
