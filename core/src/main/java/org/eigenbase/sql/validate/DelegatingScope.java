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
import org.eigenbase.sql.parser.*;

import com.google.common.collect.ImmutableList;

import static org.eigenbase.util.Static.RESOURCE;

/**
 * A scope which delegates all requests to its parent scope. Use this as a base
 * class for defining nested scopes.
 */
public abstract class DelegatingScope implements SqlValidatorScope {
  //~ Instance fields --------------------------------------------------------

  /**
   * Parent scope. This is where to look next to resolve an identifier; it is
   * not always the parent object in the parse tree.
   *
   * <p>This is never null: at the top of the tree, it is an {@link
   * EmptyScope}.
   */
  protected final SqlValidatorScope parent;
  protected final SqlValidatorImpl validator;

  //~ Constructors -----------------------------------------------------------

  /**
   * Creates a <code>DelegatingScope</code>.
   *
   * @param parent Parent scope
   */
  DelegatingScope(SqlValidatorScope parent) {
    super();
    assert parent != null;
    this.validator = (SqlValidatorImpl) parent.getValidator();
    this.parent = parent;
  }

  //~ Methods ----------------------------------------------------------------

  public void addChild(SqlValidatorNamespace ns, String alias) {
    // By default, you cannot add to a scope. Derived classes can
    // override.
    throw new UnsupportedOperationException();
  }

  public SqlValidatorNamespace resolve(
      String name,
      SqlValidatorScope[] ancestorOut,
      int[] offsetOut) {
    return parent.resolve(name, ancestorOut, offsetOut);
  }

  protected void addColumnNames(
      SqlValidatorNamespace ns,
      List<SqlMoniker> colNames) {
    final RelDataType rowType;
    try {
      rowType = ns.getRowType();
    } catch (Error e) {
      // namespace is not good - bail out.
      return;
    }

    for (RelDataTypeField field : rowType.getFieldList()) {
      colNames.add(
          new SqlMonikerImpl(
              field.getName(),
              SqlMonikerType.COLUMN));
    }
  }

  public void findAllColumnNames(List<SqlMoniker> result) {
    parent.findAllColumnNames(result);
  }

  public void findAliases(List<SqlMoniker> result) {
    parent.findAliases(result);
  }

  public String findQualifyingTableName(String columnName, SqlNode ctx) {
    return parent.findQualifyingTableName(columnName, ctx);
  }

  public RelDataType resolveColumn(String name, SqlNode ctx) {
    return parent.resolveColumn(name, ctx);
  }

  public SqlValidatorNamespace getTableNamespace(List<String> names) {
    return parent.getTableNamespace(names);
  }

  public SqlValidatorScope getOperandScope(SqlCall call) {
    if (call instanceof SqlSelect) {
      return validator.getSelectScope((SqlSelect) call);
    }
    return this;
  }

  public SqlValidator getValidator() {
    return validator;
  }

  /**
   * Converts an identifier into a fully-qualified identifier. For example,
   * the "empno" in "select empno from emp natural join dept" becomes
   * "emp.empno".
   *
   * <p>If the identifier cannot be resolved, throws. Never returns null.
   */
  public SqlIdentifier fullyQualify(SqlIdentifier identifier) {
    if (identifier.isStar()) {
      return identifier;
    }

    String tableName;
    String columnName;

    switch (identifier.names.size()) {
    case 1:
      columnName = identifier.names.get(0);
      tableName =
          findQualifyingTableName(columnName, identifier);

      // todo: do implicit collation here
      final SqlParserPos pos = identifier.getParserPosition();
      SqlIdentifier expanded =
          new SqlIdentifier(
              ImmutableList.of(tableName, columnName),
              null,
              pos,
              ImmutableList.of(SqlParserPos.ZERO, pos));
      validator.setOriginal(expanded, identifier);
      return expanded;

    case 2:
      tableName = identifier.names.get(0);
      final SqlValidatorNamespace fromNs = resolve(tableName, null, null);
      if (fromNs == null) {
        throw validator.newValidationError(identifier.getComponent(0),
            RESOURCE.tableNameNotFound(tableName));
      }
      columnName = identifier.names.get(1);
      final RelDataType fromRowType = fromNs.getRowType();
      final RelDataTypeField field =
          validator.catalogReader.field(fromRowType, columnName);
      if (field != null) {
        return identifier; // it was fine already
      } else {
        throw validator.newValidationError(identifier.getComponent(1),
            RESOURCE.columnNotFoundInTable(columnName, tableName));
      }

    default:
      // NOTE jvs 26-May-2004:  lengths greater than 2 are possible
      // for row and structured types
      assert identifier.names.size() > 0;
      return identifier;
    }
  }

  public void validateExpr(SqlNode expr) {
    // Do not delegate to parent. An expression valid in this scope may not
    // be valid in the parent scope.
  }

  public SqlWindow lookupWindow(String name) {
    return parent.lookupWindow(name);
  }

  public SqlMonotonicity getMonotonicity(SqlNode expr) {
    return parent.getMonotonicity(expr);
  }

  public SqlNodeList getOrderList() {
    return parent.getOrderList();
  }

  /**
   * Returns the parent scope of this <code>DelegatingScope</code>.
   */
  public SqlValidatorScope getParent() {
    return parent;
  }
}

// End DelegatingScope.java
