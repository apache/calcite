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

import com.google.common.collect.ImmutableList;

import org.checkerframework.checker.nullness.qual.Nullable;

import java.util.ArrayList;
import java.util.Collection;
import java.util.HashMap;
import java.util.List;
import java.util.Map;
import java.util.Objects;

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

  protected ListScope(SqlValidatorScope parent) {
    super(parent);
  }

  //~ Methods ----------------------------------------------------------------

  @Override public void addChild(SqlValidatorNamespace ns, String alias,
      boolean nullable) {
    Objects.requireNonNull(alias, "alias");
    children.add(new ScopeChild(children.size(), alias, ns, nullable));
  }

  /**
   * Returns an immutable list of child namespaces.
   *
   * @return list of child namespaces
   */
  public List<SqlValidatorNamespace> getChildren() {
    return Util.transform(children, scopeChild -> scopeChild.namespace);
  }

  /**
   * Returns an immutable list of child names.
   *
   * @return list of child namespaces
   */
  List<@Nullable String> getChildNames() {
    return Util.transform(children, scopeChild -> scopeChild.name);
  }

  /**
   * Whether the ith child namespace produces nullable result.
   *
   * For example, in below query,
   * <pre>
   *   SELECT *
   *   FROM EMPS
   *   LEFT OUTER JOIN DEPT
   * </pre>
   * the namespace which corresponding to 'DEPT' is nullable.
   *
   * @param i The child index.
   * @return Whether it's nullable.
   */
  public boolean isChildNullable(int i) {
    return children.get(i).nullable;
  }

  private @Nullable ScopeChild findChild(List<String> names,
      SqlNameMatcher nameMatcher) {
    for (ScopeChild child : children) {
      String lastName = Util.last(names);
      if (child.name != null) {
        if (!nameMatcher.matches(child.name, lastName)) {
          // Alias does not match last segment. Don't consider the
          // fully-qualified name. E.g.
          //    SELECT sales.emp.name FROM sales.emp AS otherAlias
          continue;
        }
        if (names.size() == 1) {
          return child;
        }
      }

      // Look up the 2 tables independently, in case one is qualified with
      // catalog & schema and the other is not.
      final SqlValidatorTable table = child.namespace.getTable();
      if (table != null) {
        final ResolvedImpl resolved = new ResolvedImpl();
        resolveTable(names, nameMatcher, Path.EMPTY, resolved);
        if (resolved.count() == 1) {
          Resolve only = resolved.only();
          List<String> qualifiedName = table.getQualifiedName();
          if (only.remainingNames.isEmpty()
              && only.namespace instanceof TableNamespace
              && Objects.equals(qualifiedName, getQualifiedName(only.namespace.getTable()))) {
            return child;
          }
        }
      }
    }
    return null;
  }

  private static @Nullable List<String> getQualifiedName(@Nullable SqlValidatorTable table) {
    return table == null ? null : table.getQualifiedName();
  }

  @Override public void findAllColumnNames(List<SqlMoniker> result) {
    for (ScopeChild child : children) {
      addColumnNames(child.namespace, result);
    }
    parent.findAllColumnNames(result);
  }

  @Override public void findAliases(Collection<SqlMoniker> result) {
    for (ScopeChild child : children) {
      result.add(new SqlMonikerImpl(child.name, SqlMonikerType.TABLE));
    }
    parent.findAliases(result);
  }

  @SuppressWarnings("deprecation")
  @Override public Pair<String, SqlValidatorNamespace>
  findQualifyingTableName(final String columnName, SqlNode ctx) {
    final SqlNameMatcher nameMatcher = validator.catalogReader.nameMatcher();
    final Map<String, ScopeChild> map =
        findQualifyingTableNames(columnName, ctx, nameMatcher);
    switch (map.size()) {
    case 0:
      throw validator.newValidationError(ctx,
          RESOURCE.columnNotFound(columnName));
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
  findQualifyingTableNames(String columnName, SqlNode ctx,
      SqlNameMatcher nameMatcher) {
    final Map<String, ScopeChild> map = new HashMap<>();
    for (ScopeChild child : children) {
      final ResolvedImpl resolved = new ResolvedImpl();
      resolve(ImmutableList.of(child.name, columnName), nameMatcher, true,
          resolved);
      if (resolved.count() > 0) {
        map.put(child.name, child);
      }
    }
    switch (map.size()) {
    case 0:
      return parent.findQualifyingTableNames(columnName, ctx, nameMatcher);
    default:
      return map;
    }
  }

  @Override public void resolve(List<String> names, SqlNameMatcher nameMatcher,
      boolean deep, Resolved resolved) {
    // First resolve by looking through the child namespaces.
    final ScopeChild child0 = findChild(names, nameMatcher);
    if (child0 != null) {
      final Step path =
          Path.EMPTY.plus(child0.namespace.getRowType(), child0.ordinal,
              child0.name, StructKind.FULLY_QUALIFIED);
      resolved.found(child0.namespace, child0.nullable, this, path, null);
      return;
    }

    // Recursively look deeper into the record-valued fields of the namespace,
    // if it allows skipping fields.
    if (deep) {
      for (ScopeChild child : children) {
        // If identifier starts with table alias, remove the alias.
        final List<String> names2 =
            nameMatcher.matches(child.name, names.get(0))
                ? names.subList(1, names.size())
                : names;
        resolveInNamespace(child.namespace, child.nullable, names2, nameMatcher,
            Path.EMPTY, resolved);
      }
      if (resolved.count() > 0) {
        return;
      }
    }

    // Then call the base class method, which will delegate to the
    // parent scope.
    super.resolve(names, nameMatcher, deep, resolved);
  }

  @Override public @Nullable RelDataType resolveColumn(String columnName, SqlNode ctx) {
    final SqlNameMatcher nameMatcher = validator.catalogReader.nameMatcher();
    int found = 0;
    RelDataType type = null;
    for (ScopeChild child : children) {
      SqlValidatorNamespace childNs = child.namespace;
      final RelDataType childRowType = childNs.getRowType();
      final RelDataTypeField field =
          nameMatcher.field(childRowType, columnName);
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
