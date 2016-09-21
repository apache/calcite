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
import org.apache.calcite.rel.type.StructKind;
import org.apache.calcite.sql.SqlCall;
import org.apache.calcite.sql.SqlIdentifier;
import org.apache.calcite.sql.SqlNode;
import org.apache.calcite.sql.SqlNodeList;
import org.apache.calcite.sql.SqlSelect;
import org.apache.calcite.sql.SqlWindow;
import org.apache.calcite.sql.parser.SqlParserPos;
import org.apache.calcite.util.Pair;

import com.google.common.collect.ImmutableList;
import com.google.common.collect.ImmutableMap;

import java.util.Collection;
import java.util.Collections;
import java.util.Comparator;
import java.util.List;
import java.util.Map;

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

  public void resolve(List<String> names, boolean deep, Resolved resolved) {
    parent.resolve(names, deep, resolved);
  }

  /** If a record type allows implicit references to fields, recursively looks
   * into the fields. Otherwise returns immediately. */
  void resolveInNamespace(SqlValidatorNamespace ns, List<String> names,
      Path path, Resolved resolved) {
    if (names.isEmpty()) {
      resolved.found(ns, this, path);
      return;
    }
    final RelDataType rowType = ns.getRowType();
    if (rowType.isStruct()) {
      final String name = names.get(0);
      final RelDataTypeField field0 =
          validator.catalogReader.field(rowType, name);
      if (field0 != null) {
        final SqlValidatorNamespace ns2 = ns.lookupChild(field0.getName());
        final Step path2 = path.add(rowType, field0.getIndex(),
            StructKind.FULLY_QUALIFIED);
        resolveInNamespace(ns2, names.subList(1, names.size()), path2,
            resolved);
      } else {
        for (RelDataTypeField field : rowType.getFieldList()) {
          switch (field.getType().getStructKind()) {
          case PEEK_FIELDS:
          case PEEK_FIELDS_DEFAULT:
            final Step path2 = path.add(rowType, field.getIndex(),
                field.getType().getStructKind());
            final SqlValidatorNamespace ns2 = ns.lookupChild(field.getName());
            resolveInNamespace(ns2, names, path2, resolved);
          }
        }
      }
    }
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

  protected Map<String, SqlValidatorNamespace>
  findQualifyingTables(String columnName) {
    return ImmutableMap.of();
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

    final SqlIdentifier previous = identifier;
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
      if (field != null) {
        if (hasAmbiguousUnresolvedStar(namespace.getRowType(), field,
            columnName)) {
          throw validator.newValidationError(identifier,
              RESOURCE.columnAmbiguous(columnName));
        }

        columnName = field.getName(); // use resolved field name
      }
      // todo: do implicit collation here
      final SqlParserPos pos = identifier.getParserPosition();
      identifier =
          new SqlIdentifier(ImmutableList.of(tableName, columnName), null,
              pos, ImmutableList.of(SqlParserPos.ZERO, pos));
    }
    // fall through
    default: {
      SqlValidatorNamespace fromNs = null;
      Path fromPath = null;
      final ResolvedImpl resolved = new ResolvedImpl();
      int size = identifier.names.size();
      int i = size - 1;
      for (; i > 0; i--) {
        final SqlIdentifier prefix = identifier.getComponent(0, i);
        resolved.clear();
        resolve(prefix.names, false, resolved);
        if (resolved.count() == 1) {
          final Resolve resolve = resolved.only();
          fromNs = resolve.namespace;
          fromPath = resolve.path;
          break;
        }
      }
      if (fromNs == null || fromNs instanceof SchemaNamespace) {
        // Look for a column not qualified by a table alias.
        columnName = identifier.names.get(0);
        final Map<String, SqlValidatorNamespace> map =
            findQualifyingTables(columnName);
        switch (map.size()) {
        default:
          final SqlIdentifier prefix1 = identifier.skipLast(1);
          throw validator.newValidationError(prefix1,
              RESOURCE.tableNameNotFound(prefix1.toString()));
        case 1: {
          final Map.Entry<String, SqlValidatorNamespace> entry =
              map.entrySet().iterator().next();
          final String tableName = entry.getKey();
          final SqlValidatorNamespace namespace = entry.getValue();
          fromNs = namespace;
          fromPath = resolved.emptyPath();

          // Adding table name is for RecordType column with StructKind.PEEK_FIELDS or
          // StructKind.PEEK_FIELDS only. Access to a field in a RecordType column of
          // other StructKind should always be qualified with table name.
          final RelDataTypeField field =
              validator.catalogReader.field(namespace.getRowType(), columnName);
          if (field != null) {
            switch (field.getType().getStructKind()) {
            case PEEK_FIELDS:
            case PEEK_FIELDS_DEFAULT:
              columnName = field.getName(); // use resolved field name
              resolve(ImmutableList.of(tableName), false, resolved);
              if (resolved.count() == 1) {
                final Resolve resolve = resolved.only();
                fromNs = resolve.namespace;
                fromPath = resolve.path;
                identifier = identifier
                    .setName(0, columnName)
                    .add(0, tableName, SqlParserPos.ZERO);
                ++i;
                ++size;
              }
            }
          }
        }
        }

        // Throw an error if the table was not found.
        // If one or more of the child namespaces allows peeking
        // (e.g. if they are Phoenix column families) then we relax the SQL
        // standard requirement that record fields are qualified by table alias.
        if (!hasLiberalChild()) {
          final SqlIdentifier prefix1 = identifier.skipLast(1);
          throw validator.newValidationError(prefix1,
              RESOURCE.tableNameNotFound(prefix1.toString()));
        }
      }

      // If a table alias is part of the identifier, make sure that the table
      // alias uses the same case as it was defined. For example, in
      //
      //    SELECT e.empno FROM Emp as E
      //
      // change "e.empno" to "E.empno".
      if (fromNs.getEnclosingNode() != null) {
        String alias =
            SqlValidatorUtil.getAlias(fromNs.getEnclosingNode(), -1);
        if (alias != null
            && i > 0
            && !alias.equals(identifier.names.get(i - 1))) {
          identifier = identifier.setName(i - 1, alias);
        }
      }
      RelDataType fromRowType = fromNs.getRowType();
      if (fromPath.stepCount() > 1) {
        for (Step p : fromPath.steps()) {
          fromRowType = fromRowType.getFieldList().get(p.i).getType();
        }
        ++i;
      }
      final SqlIdentifier suffix = identifier.getComponent(i, size);
      resolved.clear();
      resolveInNamespace(fromNs, suffix.names, resolved.emptyPath(), resolved);
      final Path path;
      switch (resolved.count()) {
      case 0:
        // Find the shortest suffix that also fails. Suppose we cannot resolve
        // "a.b.c"; we find we cannot resolve "a.b" but can resolve "a". So,
        // the error will be "Column 'a.b' not found".
        int k = size - 1;
        for (; k > i; --k) {
          SqlIdentifier suffix2 = identifier.getComponent(i, k);
          resolved.clear();
          resolveInNamespace(fromNs, suffix2.names, resolved.emptyPath(),
              resolved);
          if (resolved.count() > 0) {
            break;
          }
        }
        final SqlIdentifier prefix = identifier.getComponent(0, i);
        final SqlIdentifier suffix3 = identifier.getComponent(i, k + 1);
        throw validator.newValidationError(suffix3,
            RESOURCE.columnNotFoundInTable(suffix3.toString(), prefix.toString()));
      case 1:
        path = resolved.only().path;
        break;
      default:
        final Comparator<Resolve> c =
            new Comparator<Resolve>() {
              public int compare(Resolve o1, Resolve o2) {
                // Name resolution that uses fewer implicit steps wins.
                int c = Integer.compare(worstKind(o1.path), worstKind(o2.path));
                if (c != 0) {
                  return c;
                }
                // Shorter path wins
                return Integer.compare(o1.path.stepCount(), o2.path.stepCount());
              }

              private int worstKind(Path path) {
                int kind = -1;
                for (Step step : path.steps()) {
                  kind = Math.max(kind, step.kind.ordinal());
                }
                return kind;
              }
            };
        Collections.sort(resolved.resolves, c);
        if (c.compare(resolved.resolves.get(0), resolved.resolves.get(1)) == 0) {
          throw validator.newValidationError(suffix,
              RESOURCE.columnAmbiguous(suffix.toString()));
        }
        path = resolved.resolves.get(0).path;
      }

      // Normalize case to match definition, make elided fields explicit,
      // and check that references to dynamic stars ("**") are unambiguous.
      int k = i;
      for (Step step : path.steps()) {
        final RelDataTypeField field0 =
            step.rowType.getFieldList().get(step.i);
        final String fieldName = field0.getName();
        switch (step.kind) {
        case PEEK_FIELDS:
        case PEEK_FIELDS_DEFAULT:
          identifier = identifier.add(k, fieldName, SqlParserPos.ZERO);
          break;
        default:
          final String name = identifier.names.get(k);
          if (!fieldName.equals(name)) {
            identifier = identifier.setName(k, fieldName);
          }
          if (hasAmbiguousUnresolvedStar(step.rowType, field0, name)) {
            throw validator.newValidationError(identifier,
                RESOURCE.columnAmbiguous(name));
          }
        }
        ++k;
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

      if (!previous.equals(identifier)) {
        validator.setOriginal(identifier, previous);
      }
      return SqlQualified.create(this, i, fromNs, identifier);
    }
    }
  }

  protected boolean hasLiberalChild() {
    return false;
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

  /** Returns whether {@code rowType} contains more than one star column.
   * Having more than one star columns implies ambiguous column. */
  private boolean hasAmbiguousUnresolvedStar(RelDataType rowType,
      RelDataTypeField field, String columnName) {
    if (field.isDynamicStar()
        && !DynamicRecordType.isDynamicStarColName(columnName)) {
      int count = 0;
      for (RelDataTypeField possibleStar : rowType.getFieldList()) {
        if (possibleStar.isDynamicStar()) {
          if (++count > 1) {
            return true;
          }
        }
      }
    }
    return false;
  }

  /**
   * Returns the parent scope of this <code>DelegatingScope</code>.
   */
  public SqlValidatorScope getParent() {
    return parent;
  }
}

// End DelegatingScope.java
