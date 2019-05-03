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

import org.apache.calcite.sql.SqlFunction;
import org.apache.calcite.sql.SqlFunctionCategory;
import org.apache.calcite.sql.SqlIdentifier;
import org.apache.calcite.sql.SqlOperator;
import org.apache.calcite.sql.SqlOperatorTable;
import org.apache.calcite.sql.SqlSyntax;
import org.apache.calcite.sql.fun.SqlStdOperatorTable;
import org.apache.calcite.sql.validate.SqlNameMatcher;
import org.apache.calcite.util.Pair;
import org.apache.calcite.util.Util;

import com.google.common.collect.HashMultimap;
import com.google.common.collect.ImmutableList;
import com.google.common.collect.Multimap;

import java.lang.reflect.Field;
import java.util.Collection;
import java.util.List;
import java.util.Locale;

/**
 * ReflectiveSqlOperatorTable implements the {@link SqlOperatorTable} interface
 * by reflecting the public fields of a subclass.
 */
public abstract class ReflectiveSqlOperatorTable implements SqlOperatorTable {
  public static final String IS_NAME = "INFORMATION_SCHEMA";

  //~ Instance fields --------------------------------------------------------

  private final Multimap<CaseSensitiveKey, SqlOperator> caseSensitiveOperators =
      HashMultimap.create();

  private final Multimap<CaseInsensitiveKey, SqlOperator> caseInsensitiveOperators =
      HashMultimap.create();

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
      } catch (IllegalArgumentException | IllegalAccessException e) {
        Util.throwIfUnchecked(e.getCause());
        throw new RuntimeException(e.getCause());
      }
    }
  }

  // implement SqlOperatorTable
  public void lookupOperatorOverloads(SqlIdentifier opName,
      SqlFunctionCategory category, SqlSyntax syntax,
      List<SqlOperator> operatorList, SqlNameMatcher nameMatcher) {
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

    final Collection<SqlOperator> list =
        lookUpOperators(simpleName, syntax, nameMatcher);
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
      for (SqlOperator extra
          : lookUpOperators(simpleName, syntax, nameMatcher)) {
        // REVIEW: should only search operators added during this method?
        if (extra != null && !operatorList.contains(extra)) {
          operatorList.add(extra);
        }
      }
      break;
    }
  }

  /**
   * Look up operators based on case-sensitiveness.
   */
  private Collection<SqlOperator> lookUpOperators(String name, SqlSyntax syntax,
      SqlNameMatcher nameMatcher) {
    // Case sensitive only works for UDFs.
    // Always look up built-in operators case-insensitively. Even in sessions
    // with unquotedCasing=UNCHANGED and caseSensitive=true.
    if (nameMatcher.isCaseSensitive()
        && !(this instanceof SqlStdOperatorTable)) {
      return caseSensitiveOperators.get(new CaseSensitiveKey(name, syntax));
    } else {
      return caseInsensitiveOperators.get(new CaseInsensitiveKey(name, syntax));
    }
  }

  /**
   * Registers a function or operator in the table.
   */
  public void register(SqlOperator op) {
    // Register both for case-sensitive and case-insensitive look up.
    caseSensitiveOperators.put(new CaseSensitiveKey(op.getName(), op.getSyntax()), op);
    caseInsensitiveOperators.put(new CaseInsensitiveKey(op.getName(), op.getSyntax()), op);
  }

  public List<SqlOperator> getOperatorList() {
    return ImmutableList.copyOf(caseSensitiveOperators.values());
  }

  /** Key for looking up operators. The name is stored in upper-case because we
   * store case-insensitively, even in a case-sensitive session. */
  private static class CaseInsensitiveKey extends Pair<String, SqlSyntax> {
    CaseInsensitiveKey(String name, SqlSyntax syntax) {
      super(name.toUpperCase(Locale.ROOT), normalize(syntax));
    }
  }

  /** Key for looking up operators. The name kept as what it is to look up case-sensitively. */
  private static class CaseSensitiveKey extends Pair<String, SqlSyntax> {
    CaseSensitiveKey(String name, SqlSyntax syntax) {
      super(name, normalize(syntax));
    }
  }

  private static SqlSyntax normalize(SqlSyntax syntax) {
    switch (syntax) {
    case BINARY:
    case PREFIX:
    case POSTFIX:
      return syntax;
    default:
      return SqlSyntax.FUNCTION;
    }
  }
}

// End ReflectiveSqlOperatorTable.java
