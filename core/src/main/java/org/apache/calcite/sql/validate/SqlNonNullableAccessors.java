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
package org.apache.calcite.sql.validate;

import org.apache.calcite.sql.SqlAsofJoin;
import org.apache.calcite.sql.SqlCallBinding;
import org.apache.calcite.sql.SqlDelete;
import org.apache.calcite.sql.SqlJoin;
import org.apache.calcite.sql.SqlMerge;
import org.apache.calcite.sql.SqlNode;
import org.apache.calcite.sql.SqlNodeList;
import org.apache.calcite.sql.SqlOperatorBinding;
import org.apache.calcite.sql.SqlSelect;
import org.apache.calcite.sql.SqlUpdate;

import org.apiguardian.api.API;

import java.util.Objects;

import static java.util.Objects.requireNonNull;

/**
 * This class provides non-nullable accessors for common getters.
 */
public class SqlNonNullableAccessors {
  private SqlNonNullableAccessors() {
  }

  private static String safeToString(Object obj) {
    try {
      return Objects.toString(obj);
    } catch (Throwable e) {
      return "Error in toString: " + e;
    }
  }

  @API(since = "1.27", status = API.Status.EXPERIMENTAL)
  public static SqlSelect getSourceSelect(SqlUpdate statement) {
    return requireNonNull(statement.getSourceSelect(),
        () -> "sourceSelect of " + safeToString(statement));
  }

  @API(since = "1.27", status = API.Status.EXPERIMENTAL)
  public static SqlSelect getSourceSelect(SqlDelete statement) {
    return requireNonNull(statement.getSourceSelect(),
        () -> "sourceSelect of " + safeToString(statement));
  }

  @API(since = "1.27", status = API.Status.EXPERIMENTAL)
  public static SqlSelect getSourceSelect(SqlMerge statement) {
    return requireNonNull(statement.getSourceSelect(),
        () -> "sourceSelect of " + safeToString(statement));
  }

  @API(since = "1.27", status = API.Status.EXPERIMENTAL)
  public static SqlNode getCondition(SqlJoin join) {
    return requireNonNull(join.getCondition(),
        () -> "getCondition of " + safeToString(join));
  }

  public static SqlNode getMatchCondition(SqlAsofJoin join) {
    return requireNonNull(join.getMatchCondition(),
        () -> "getMatchCondition of " + safeToString(join));
  }

  @API(since = "1.27", status = API.Status.EXPERIMENTAL)
  static SqlNode getNode(ScopeChild child) {
    return requireNonNull(child.namespace.getNode(),
        () -> "child.namespace.getNode() of " + child.name);
  }

  @API(since = "1.27", status = API.Status.EXPERIMENTAL)
  public static SqlNodeList getSelectList(SqlSelect innerSelect) {
    return requireNonNull(innerSelect.getSelectList(),
        () -> "selectList of " + safeToString(innerSelect));
  }

  @API(since = "1.27", status = API.Status.EXPERIMENTAL)
  public static SqlValidatorTable getTable(SqlValidatorNamespace ns) {
    return requireNonNull(ns.getTable(),
        () -> "ns.getTable() for " + safeToString(ns));
  }

  @API(since = "1.27", status = API.Status.EXPERIMENTAL)
  public static SqlValidatorScope getScope(SqlCallBinding callBinding) {
    return requireNonNull(callBinding.getScope(),
        () -> "scope is null for " + safeToString(callBinding));
  }

  @API(since = "1.27", status = API.Status.EXPERIMENTAL)
  public static SqlValidatorNamespace getNamespace(SqlCallBinding callBinding) {
    return requireNonNull(
        callBinding.getValidator().getNamespace(callBinding.getCall()),
        () -> "scope is null for " + safeToString(callBinding));
  }

  @API(since = "1.27", status = API.Status.EXPERIMENTAL)
  public static <T extends Object> T getOperandLiteralValueOrThrow(SqlOperatorBinding opBinding,
      int ordinal, Class<T> clazz) {
    return requireNonNull(opBinding.getOperandLiteralValue(ordinal, clazz),
        () -> "expected non-null operand " + ordinal + " in " + safeToString(opBinding));
  }
}
