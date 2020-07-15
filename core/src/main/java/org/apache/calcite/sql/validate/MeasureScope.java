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
import org.apache.calcite.sql.SqlIdentifier;
import org.apache.calcite.sql.SqlNode;
import org.apache.calcite.sql.SqlSelect;
import org.apache.calcite.sql.SqlUtil;

import com.google.common.collect.ImmutableList;

import org.checkerframework.checker.nullness.qual.Nullable;

import java.util.ArrayList;
import java.util.HashSet;
import java.util.List;
import java.util.Set;

import static org.apache.calcite.util.Static.RESOURCE;

import static java.util.stream.Collectors.joining;

/**
 * Scope for resolving identifiers within a SELECT item that is annotated
 * "AS MEASURE".
 *
 * <p>Scope includes the identifiers of SELECT plus all aliases. This allows
 * measures to reference each other and also reference other SELECT items.
 */
public class MeasureScope extends DelegatingScope {
  //~ Instance fields --------------------------------------------------------

  private final SqlSelect select;
  private final List<String> aliasList;
  private final Set<Integer> activeOrdinals = new HashSet<>();

  /**
   * Creates a MeasureScope.
   *
   * @param selectScope Parent scope
   * @param select      Enclosing SELECT node
   */
  MeasureScope(SqlValidatorScope selectScope,
      SqlSelect select) {
    super(selectScope);
    this.select = select;

    final List<String> aliasList = new ArrayList<>();
    for (SqlNode selectItem : select.getSelectList()) {
      aliasList.add(SqlValidatorUtil.alias(selectItem, aliasList.size()));
    }
    this.aliasList = ImmutableList.copyOf(aliasList);
  }

  @Override public SqlNode getNode() {
    return select;
  }

  @Override public void validateExpr(SqlNode expr) {
    SqlNode expanded = validator.extendedExpandGroupBy(expr, this, select);

    // expression needs to be valid in parent scope too
    parent.validateExpr(expanded);
  }

  @Override public @Nullable RelDataType resolveColumn(String name, SqlNode ctx) {
    final SqlNameMatcher matcher = validator.getCatalogReader().nameMatcher();
    final int aliasOrdinal = matcher.indexOf(aliasList, name);
    if (aliasOrdinal >= 0) {
      final SqlNode selectItem = select.getSelectList().get(aliasOrdinal);
      final SqlNode measure = SqlValidatorUtil.getMeasure(selectItem);
      if (measure != null) {
        try {
          if (activeOrdinals.add(aliasOrdinal)) {
            return validator.deriveType(this, measure);
          }
          final String dependentMeasures = activeOrdinals.stream().map(aliasList::get)
              .map(s -> "'" + s + "'")
              .collect(joining(", "));
          throw validator.newValidationError(ctx,
              RESOURCE.measureIsCyclic(name, dependentMeasures));
        } finally {
          activeOrdinals.remove(aliasOrdinal);
        }
      }
      final SqlNode expression = SqlUtil.stripAs(selectItem);
      return validator.deriveType(parent, expression);
    }
    return super.resolveColumn(name, ctx);
  }

  public @Nullable SqlNode lookupMeasure(String name) {
    final SqlNameMatcher matcher = validator.getCatalogReader().nameMatcher();
    final int aliasOrdinal = matcher.indexOf(aliasList, name);
    if (aliasOrdinal >= 0) {
      final SqlNode selectItem = select.getSelectList().get(aliasOrdinal);
      final @Nullable SqlNode measure = SqlValidatorUtil.getMeasure(selectItem);
      if (measure != null) {
        return measure;
      }
      return SqlUtil.stripAs(selectItem); // non-measure select item
    }
    return null;
  }

  @Override public SqlQualified fullyQualify(SqlIdentifier identifier) {
    // If it's a simple identifier, look for an alias.
    if (identifier.isSimple()) {
      @Nullable SqlQualified qualified = foo(this, select, identifier);
      if (qualified != null) {
        return qualified;
      }
    }
    return super.fullyQualify(identifier);
  }

  static @Nullable SqlQualified foo(DelegatingScope scope, SqlSelect select,
      SqlIdentifier identifier) {
    final String name = identifier.names.get(0);
    final SqlValidatorNamespace selectNs =
        scope.validator.getNamespace(select);

    final SqlNameMatcher nameMatcher =
        scope.validator.catalogReader.nameMatcher();
    final int aliasCount = OrderByScope.aliasCount(select, nameMatcher, name);
    if (aliasCount > 1) {
      // More than one column has this alias.
      throw scope.validator.newValidationError(identifier,
          RESOURCE.columnAmbiguous(name));
    }
    if (aliasCount == 1) {
      // if identifier is resolved to a dynamic star, use super.fullyQualify()
      // for such case.
      return SqlQualified.create(scope, 1, selectNs, identifier);
    }
    return null;
  }

}
