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

import org.apache.calcite.rel.type.DynamicRecordType;
import org.apache.calcite.rel.type.RelDataType;
import org.apache.calcite.rel.type.RelDataTypeField;
import org.apache.calcite.sql.SqlCall;
import org.apache.calcite.sql.SqlIdentifier;
import org.apache.calcite.sql.SqlNode;
import org.apache.calcite.sql.SqlNodeList;
import org.apache.calcite.sql.SqlSelect;
import org.apache.calcite.sql.SqlWindow;
import org.apache.calcite.sql.parser.SqlParserPos;
import org.apache.calcite.util.Pair;

import com.google.common.collect.ImmutableList;

import java.util.Collection;
import java.util.List;

import static org.apache.calcite.util.Static.RESOURCE;

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
   * <p>This is never null: at the top of the tree, it is an
   * {@link EmptyScope}.
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
      List<String> names,
      SqlValidatorScope[] ancestorOut,
      int[] offsetOut) {
    return parent.resolve(names, ancestorOut, offsetOut);
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

  public void findAliases(Collection<SqlMoniker> result) {
    parent.findAliases(result);
  }

  public Pair<String, SqlValidatorNamespace>
  findQualifyingTableName(String columnName, SqlNode ctx) {
    return parent.findQualifyingTableName(columnName, ctx);
  }

  public RelDataType resolveColumn(String name, SqlNode ctx) {
    return parent.resolveColumn(name, ctx);
  }

  public RelDataType nullifyType(SqlNode node, RelDataType type) {
    return parent.nullifyType(node, type);
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
  public SqlQualified fullyQualify(SqlIdentifier identifier) {
    if (identifier.isStar()) {
      return SqlQualified.create(this, 1, null, identifier);
    }

    String columnName;
    switch (identifier.names.size()) {
    case 1: {
      columnName = identifier.names.get(0);
      final Pair<String, SqlValidatorNamespace> pair =
          findQualifyingTableName(columnName, identifier);
      final String tableName = pair.left;
      final SqlValidatorNamespace namespace = pair.right;

      final RelDataTypeField field =
          validator.catalogReader.field(namespace.getRowType(), columnName);

      checkAmbiguousUnresolvedStar(namespace.getRowType(), field, identifier, columnName);

      // todo: do implicit collation here
      final SqlParserPos pos = identifier.getParserPosition();
      SqlIdentifier expanded =
          new SqlIdentifier(
              ImmutableList.of(tableName, field.getName()),  // use resolved field name
              null,
              pos,
              ImmutableList.of(SqlParserPos.ZERO, pos));
      validator.setOriginal(expanded, identifier);
      return SqlQualified.create(this, 1, namespace, expanded);
    }

    default: {
      SqlValidatorNamespace fromNs = null;
      final int size = identifier.names.size();
      int i = size - 1;
      for (; i > 0; i--) {
        final SqlIdentifier prefix = identifier.getComponent(0, i);
        fromNs = resolve(prefix.names, null, null);
        if (fromNs != null) {
          break;
        }
      }
      if (fromNs == null || fromNs instanceof SchemaNamespace) {
        final SqlIdentifier prefix1 = identifier.skipLast(1);
        throw validator.newValidationError(prefix1,
            RESOURCE.tableNameNotFound(prefix1.toString()));
      }
      RelDataType fromRowType = fromNs.getRowType();
      for (int j = i; j < size; j++) {
        final SqlIdentifier last = identifier.getComponent(j);
        columnName = last.getSimple();
        final RelDataTypeField field =
            validator.catalogReader.field(fromRowType, columnName);
        if (field == null) {
          throw validator.newValidationError(last,
              RESOURCE.columnNotFoundInTable(columnName,
                  identifier.getComponent(0, j).toString()));
        }

        checkAmbiguousUnresolvedStar(fromRowType, field, identifier, columnName);

        // normalize case to match definition, in a copy of the identifier
        identifier = identifier.setName(j, field.getName());
        fromRowType = field.getType();
      }
      if (i > 1) {
        // Simplify overqualified identifiers.
        // For example, schema.emp.deptno becomes emp.deptno.
        //
        // It is safe to convert schema.emp or database.schema.emp to emp
        // because it would not have resolved if the FROM item had an alias. The
        // following query is invalid:
        //   SELECT schema.emp.deptno FROM schema.emp AS e
        identifier = identifier.getComponent(i - 1, identifier.names.size());
      }
      return SqlQualified.create(this, i, fromNs, identifier);
    }
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

  private void checkAmbiguousUnresolvedStar(RelDataType fromRowType, RelDataTypeField field,
      SqlIdentifier identifier, String columnName) {

    if (field != null
        && field.isDynamicStar()
        && !DynamicRecordType.isDynamicStarColName(columnName)) {
      // Make sure fromRowType only contains one star column.
      // Having more than one star columns implies ambiguous column.
      int count = 0;
      for (RelDataTypeField possibleStar : fromRowType.getFieldList()) {
        if (possibleStar.isDynamicStar()) {
          count++;
        }
      }

      if (count > 1) {
        throw validator.newValidationError(identifier,
            RESOURCE.columnAmbiguous(columnName));
      }
    }

  }

  /**
   * Returns the parent scope of this <code>DelegatingScope</code>.
   */
  public SqlValidatorScope getParent() {
    return parent;
  }
}

// End DelegatingScope.java
