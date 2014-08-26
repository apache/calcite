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
package org.eigenbase.sql.util;

import java.lang.reflect.*;
import java.util.*;

import org.eigenbase.sql.*;
import org.eigenbase.util.*;

import com.google.common.collect.*;

/**
 * ReflectiveSqlOperatorTable implements the {@link SqlOperatorTable } interface
 * by reflecting the public fields of a subclass.
 */
public abstract class ReflectiveSqlOperatorTable implements SqlOperatorTable {
  public static final String IS_NAME = "INFORMATION_SCHEMA";

  //~ Instance fields --------------------------------------------------------

  private final Multimap<String, SqlOperator> operators = HashMultimap.create();

  private final Map<Pair<String, SqlSyntax>, SqlOperator> mapNameToOp =
      new HashMap<Pair<String, SqlSyntax>, SqlOperator>();

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
  public void lookupOperatorOverloads(SqlIdentifier opName,
      SqlFunctionCategory category,
      SqlSyntax syntax,
      List<SqlOperator> operatorList) {
    // NOTE jvs 3-Mar-2005:  ignore category until someone cares

    String simpleName;
    if (opName.names.size() > 1) {
      if (opName.names.get(opName.names.size() - 2).equals(IS_NAME)) {
        // per SQL99 Part 2 Section 10.4 Syntax Rule 7.b.ii.1
        simpleName = Util.last(opName.names);
      } else {
        return;
      }
    } else {
      simpleName = opName.getSimple();
    }

    // Always look up built-in operators case-insensitively. Even in sessions
    // with unquotedCasing=UNCHANGED and caseSensitive=true.
    final Collection<SqlOperator> list =
        operators.get(simpleName.toUpperCase());
    if (list.isEmpty()) {
      return;
    }
    for (SqlOperator op : list) {
      if (op.getSyntax() == syntax) {
        operatorList.add(op);
      } else if (syntax == SqlSyntax.FUNCTION
          && op instanceof SqlFunction) {
        // this special case is needed for operators like CAST,
        // which are treated as functions but have special syntax
        operatorList.add(op);
      }
    }

    // REVIEW jvs 1-Jan-2005:  why is this extra lookup required?
    // Shouldn't it be covered by search above?
    switch (syntax) {
    case BINARY:
    case PREFIX:
    case POSTFIX:
      SqlOperator extra = mapNameToOp.get(Pair.of(simpleName, syntax));
      // REVIEW: should only search operators added during this method?
      if (extra != null && !operatorList.contains(extra)) {
        operatorList.add(extra);
      }
      break;
    }
  }

  public void register(SqlOperator op) {
    operators.put(op.getName(), op);
    if (op instanceof SqlBinaryOperator) {
      mapNameToOp.put(Pair.of(op.getName(), SqlSyntax.BINARY), op);
    } else if (op instanceof SqlPrefixOperator) {
      mapNameToOp.put(Pair.of(op.getName(), SqlSyntax.PREFIX), op);
    } else if (op instanceof SqlPostfixOperator) {
      mapNameToOp.put(Pair.of(op.getName(), SqlSyntax.POSTFIX), op);
    }
  }

  /**
   * Registers a function in the table.
   *
   * @param function Function to register
   */
  public void register(SqlFunction function) {
    operators.put(function.getName(), function);
    SqlFunctionCategory funcType = function.getFunctionType();
    assert funcType != null
        : "Function type for " + function.getName() + " not set";
  }

  public List<SqlOperator> getOperatorList() {
    return ImmutableList.copyOf(operators.values());
  }
}

// End ReflectiveSqlOperatorTable.java
