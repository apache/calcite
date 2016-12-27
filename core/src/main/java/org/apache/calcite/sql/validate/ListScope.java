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

import org.apache.calcite.rel.type.RelDataType;
import org.apache.calcite.rel.type.RelDataTypeField;
import org.apache.calcite.rel.type.StructKind;
import org.apache.calcite.sql.SqlNode;
import org.apache.calcite.util.Pair;
import org.apache.calcite.util.Util;

import com.google.common.base.Preconditions;
import com.google.common.collect.ImmutableList;
import com.google.common.collect.Lists;

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
  public final List<ScopeChild> children = new ArrayList<>();

  //~ Constructors -----------------------------------------------------------

  public ListScope(SqlValidatorScope parent) {
    super(parent);
  }

  //~ Methods ----------------------------------------------------------------

  @Override public void addChild(SqlValidatorNamespace ns, String alias,
      boolean nullable) {
    Preconditions.checkNotNull(alias);
    children.add(new ScopeChild(children.size(), alias, ns, nullable));
  }

  /**
   * Returns an immutable list of child namespaces.
   *
   * @return list of child namespaces
   */
  public List<SqlValidatorNamespace> getChildren() {
    return Lists.transform(children, ScopeChild.NAMESPACE_FN);
  }

  /**
   * Returns an immutable list of child names.
   *
   * @return list of child namespaces
   */
  List<String> getChildNames() {
    return Lists.transform(children, ScopeChild.NAME_FN);
  }

  private int findChild(List<String> names) {
    for (ScopeChild child : children) {
      String lastName = Util.last(names);
      if (child.name != null) {
        if (!validator.catalogReader.matches(child.name, lastName)) {
          // Alias does not match last segment. Don't consider the
          // fully-qualified name. E.g.
          //    SELECT sales.emp.name FROM sales.emp AS otherAlias
          continue;
        }
        if (names.size() == 1) {
          return child.ordinal;
        }
      }

      // Look up the 2 tables independently, in case one is qualified with
      // catalog & schema and the other is not.
      final SqlValidatorTable table = child.namespace.getTable();
      if (table != null) {
        final SqlValidatorTable table2 =
            validator.catalogReader.getTable(names);
        if (table2 != null
            && table.getQualifiedName().equals(table2.getQualifiedName())) {
          return child.ordinal;
        }
      }
    }
    return -1;
  }

  public void findAllColumnNames(List<SqlMoniker> result) {
    for (ScopeChild child : children) {
      addColumnNames(child.namespace, result);
    }
    parent.findAllColumnNames(result);
  }

  public void findAliases(Collection<SqlMoniker> result) {
    for (ScopeChild child : children) {
      result.add(new SqlMonikerImpl(child.name, SqlMonikerType.TABLE));
    }
    parent.findAliases(result);
  }

  @Override public Pair<String, SqlValidatorNamespace>
  findQualifyingTableName(final String columnName, SqlNode ctx) {
    final Map<String, ScopeChild> map = findQualifyingTables(columnName);
    switch (map.size()) {
    case 0:
      return parent.findQualifyingTableName(columnName, ctx);
    case 1:
      final Map.Entry<String, ScopeChild> entry =
          map.entrySet().iterator().next();
      return Pair.of(entry.getKey(), entry.getValue().namespace);
    default:
      throw validator.newValidationError(ctx,
          RESOURCE.columnAmbiguous(columnName));
    }
  }

  @Override public Map<String, ScopeChild>
  findQualifyingTables(String columnName) {
    final Map<String, ScopeChild> map = new HashMap<>();
    for (ScopeChild child : children) {
      final ResolvedImpl resolved = new ResolvedImpl();
      resolve(ImmutableList.of(child.name, columnName), true, resolved);
      if (resolved.count() > 0) {
        map.put(child.name, child);
      }
    }
    return map;
  }

  @Override public void resolve(List<String> names, boolean deep,
      Resolved resolved) {
    // First resolve by looking through the child namespaces.
    final int i = findChild(names);
    if (i >= 0) {
      final Step path =
          resolved.emptyPath().add(null, i, StructKind.FULLY_QUALIFIED);
      final ScopeChild child = children.get(i);
      resolved.found(child.namespace, child.nullable, this, path);
      return;
    }

    // Recursively look deeper into the record-valued fields of the namespace,
    // if it allows skipping fields.
    if (deep) {
      for (ScopeChild child : children) {
        // If identifier starts with table alias, remove the alias.
        final List<String> names2 =
            validator.catalogReader.matches(child.name, names.get(0))
                ? names.subList(1, names.size())
                : names;
        resolveInNamespace(child.namespace, child.nullable, names2,
            resolved.emptyPath(), resolved);
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
    for (ScopeChild child : children) {
      SqlValidatorNamespace childNs = child.namespace;
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
