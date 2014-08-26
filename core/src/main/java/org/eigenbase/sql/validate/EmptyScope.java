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
package org.eigenbase.sql.validate;

import java.util.*;

import org.eigenbase.reltype.*;
import org.eigenbase.sql.*;

import static org.eigenbase.util.Static.RESOURCE;

/**
 * Deviant implementation of {@link SqlValidatorScope} for the top of the scope
 * stack.
 *
 * <p>It is convenient, because we never need to check whether a scope's parent
 * is null. (This scope knows not to ask about its parents, just like Adam.)
 */
class EmptyScope implements SqlValidatorScope {
  //~ Instance fields --------------------------------------------------------

  protected final SqlValidatorImpl validator;

  //~ Constructors -----------------------------------------------------------

  EmptyScope(SqlValidatorImpl validator) {
    this.validator = validator;
  }

  //~ Methods ----------------------------------------------------------------

  public SqlValidator getValidator() {
    return validator;
  }

  public SqlIdentifier fullyQualify(SqlIdentifier identifier) {
    return identifier;
  }

  public SqlNode getNode() {
    throw new UnsupportedOperationException();
  }

  public SqlValidatorNamespace resolve(
      String name,
      SqlValidatorScope[] ancestorOut,
      int[] offsetOut) {
    return null;
  }

  public SqlValidatorNamespace getTableNamespace(List<String> names) {
    SqlValidatorTable table = validator.catalogReader.getTable(names);
    return table != null
        ? new TableNamespace(validator, table)
        : null;
  }

  public void findAllColumnNames(List<SqlMoniker> result) {
  }

  public void findAllTableNames(List<SqlMoniker> result) {
  }

  public void findAliases(List<SqlMoniker> result) {
  }

  public RelDataType resolveColumn(String name, SqlNode ctx) {
    return null;
  }

  public SqlValidatorScope getOperandScope(SqlCall call) {
    return this;
  }

  public void validateExpr(SqlNode expr) {
    // valid
  }

  public String findQualifyingTableName(String columnName, SqlNode ctx) {
    throw validator.newValidationError(ctx,
        RESOURCE.columnNotFound(columnName));
  }

  public void addChild(SqlValidatorNamespace ns, String alias) {
    // cannot add to the empty scope
    throw new UnsupportedOperationException();
  }

  public SqlWindow lookupWindow(String name) {
    // No windows defined in this scope.
    return null;
  }

  public SqlMonotonicity getMonotonicity(SqlNode expr) {
    return
        ((expr instanceof SqlLiteral)
            || (expr instanceof SqlDynamicParam)
            || (expr instanceof SqlDataTypeSpec)) ? SqlMonotonicity.CONSTANT
            : SqlMonotonicity.NOT_MONOTONIC;
  }

  public SqlNodeList getOrderList() {
    // scope is not ordered
    return null;
  }
}

// End EmptyScope.java
