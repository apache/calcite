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

import org.apache.calcite.linq4j.Ord;
import org.apache.calcite.rel.type.RelDataType;
import org.apache.calcite.rel.type.RelDataTypeField;
import org.apache.calcite.rel.type.StructKind;
import org.apache.calcite.sql.SqlNode;
import org.apache.calcite.util.Pair;
import org.apache.calcite.util.Util;

import com.google.common.collect.ImmutableList;

import java.util.ArrayList;
import java.util.Collection;
import java.util.HashMap;
import java.util.List;
import java.util.Map;

import static org.apache.calcite.util.Static.RESOURCE;

/**
 * Abstract base for a scope which is defined by a list of child namespaces and
 * which inherits from a parent scope.
 */
public abstract class ListScope extends DelegatingScope {
  //~ Instance fields --------------------------------------------------------

  /**
   * List of child {@link SqlValidatorNamespace} objects and their names.
   */
  protected final List<Pair<String, SqlValidatorNamespace>> children =
      new ArrayList<>();

  //~ Constructors -----------------------------------------------------------

  public ListScope(SqlValidatorScope parent) {
    super(parent);
  }

  //~ Methods ----------------------------------------------------------------

  public void addChild(SqlValidatorNamespace ns, String alias) {
    assert alias != null;
    children.add(Pair.of(alias, ns));
  }

  /**
   * Returns an immutable list of child namespaces.
   *
   * @return list of child namespaces
   */
  public List<SqlValidatorNamespace> getChildren() {
    return Pair.right(children);
  }

  private int findChild(List<String> names) {
    for (Ord<Pair<String, SqlValidatorNamespace>> child : Ord.zip(children)) {
      String lastName = Util.last(names);
      if (child.e.left != null) {
        if (!validator.catalogReader.matches(child.e.left, lastName)) {
          // Alias does not match last segment. Don't consider the
          // fully-qualified name. E.g.
          //    SELECT sales.emp.name FROM sales.emp AS otherAlias
          continue;
        }
        if (names.size() == 1) {
          return child.i;
        }
      }

      // Look up the 2 tables independently, in case one is qualified with
      // catalog & schema and the other is not.
      final SqlValidatorTable table = child.e.right.getTable();
      if (table != null) {
        final SqlValidatorTable table2 =
            validator.catalogReader.getTable(names);
        if (table2 != null
            && table.getQualifiedName().equals(table2.getQualifiedName())) {
          return child.i;
        }
      }
    }
    return -1;
  }

  public void findAllColumnNames(List<SqlMoniker> result) {
    for (Pair<String, SqlValidatorNamespace> pair : children) {
      addColumnNames(pair.right, result);
    }
    parent.findAllColumnNames(result);
  }

  public void findAliases(Collection<SqlMoniker> result) {
    for (Pair<String, SqlValidatorNamespace> pair : children) {
      result.add(new SqlMonikerImpl(pair.left, SqlMonikerType.TABLE));
    }
    parent.findAliases(result);
  }

  @Override public Pair<String, SqlValidatorNamespace>
  findQualifyingTableName(final String columnName, SqlNode ctx) {
    final Map<String, SqlValidatorNamespace> map =
        findQualifyingTables(columnName);
    switch (map.size()) {
    case 0:
      return parent.findQualifyingTableName(columnName, ctx);
    case 1:
      return Pair.of(map.entrySet().iterator().next());
    default:
      throw validator.newValidationError(ctx,
          RESOURCE.columnAmbiguous(columnName));
    }
  }

  @Override public Map<String, SqlValidatorNamespace>
  findQualifyingTables(String columnName) {
    final Map<String, SqlValidatorNamespace> map = new HashMap<>();
    for (Pair<String, SqlValidatorNamespace> child : children) {
      final ResolvedImpl resolved = new ResolvedImpl();
      resolve(ImmutableList.of(child.left, columnName), true, resolved);
      if (resolved.count() > 0) {
        map.put(child.getKey(), child.getValue());
      }
    }
    return map;
  }

  @Override protected boolean hasLiberalChild() {
    for (Pair<String, SqlValidatorNamespace> child : children) {
      final RelDataType rowType = child.right.getRowType();
      switch (rowType.getStructKind()) {
      case PEEK_FIELDS:
      case PEEK_FIELDS_DEFAULT:
        return true;
      }
      for (RelDataTypeField field : rowType.getFieldList()) {
        switch (field.getType().getStructKind()) {
        case PEEK_FIELDS:
        case PEEK_FIELDS_DEFAULT:
          return true;
        }
      }
    }
    return false;
  }

  @Override public void resolve(List<String> names, boolean deep,
      Resolved resolved) {
    // First resolve by looking through the child namespaces.
    final int i = findChild(names);
    if (i >= 0) {
      final Step path =
          resolved.emptyPath().add(null, i, StructKind.FULLY_QUALIFIED);
      resolved.found(children.get(i).right, this, path);
      return;
    }

    // Recursively look deeper into the record-valued fields of the namespace,
    // if it allows skipping fields.
    if (deep) {
      for (Ord<Pair<String, SqlValidatorNamespace>> child : Ord.zip(children)) {
        // If identifier starts with table alias, remove the alias.
        final List<String> names2 =
            validator.catalogReader.matches(child.e.left, names.get(0))
                ? names.subList(1, names.size())
                : names;
        resolveInNamespace(child.e.right, names2, resolved.emptyPath(),
            resolved);
      }
      if (resolved.count() > 0) {
        return;
      }
    }

    // Then call the base class method, which will delegate to the
    // parent scope.
    super.resolve(names, deep, resolved);
  }

  public RelDataType resolveColumn(String columnName, SqlNode ctx) {
    int found = 0;
    RelDataType type = null;
    for (Pair<String, SqlValidatorNamespace> pair : children) {
      SqlValidatorNamespace childNs = pair.right;
      final RelDataType childRowType = childNs.getRowType();
      final RelDataTypeField field =
          validator.catalogReader.field(childRowType, columnName);
      if (field != null) {
        found++;
        type = field.getType();
      }
    }
    switch (found) {
    case 0:
      return null;
    case 1:
      return type;
    default:
      throw validator.newValidationError(ctx,
          RESOURCE.columnAmbiguous(columnName));
    }
  }
}

// End ListScope.java
