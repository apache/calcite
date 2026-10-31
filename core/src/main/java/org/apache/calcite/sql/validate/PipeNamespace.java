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
import org.apache.calcite.sql.SqlIdentifier;
import org.apache.calcite.sql.SqlNode;
import org.apache.calcite.sql.SqlPipeCall;
import org.apache.calcite.sql.SqlPipeQuery;
import org.apache.calcite.sql.SqlSelect;
import org.apache.calcite.sql.SqlUtil;

import com.google.common.collect.ImmutableList;

import org.checkerframework.checker.nullness.qual.Nullable;

import java.util.List;

import static java.util.Objects.requireNonNull;

/**
 * Namespace for a query written in SQL pipe syntax.
 */
public class PipeNamespace extends AbstractNamespace {
  private final SqlPipeQuery node;
  private final SqlSelect surrogateSelect;
  /** Scope that encloses the pipeline. */
  private final SqlValidatorScope scope;

  /** Scope children that an operator applied to this one can see. Computed
   * on demand, once this namespace has been validated. */
  private @Nullable List<ScopeChild> outChildren;

  //~ Constructors -----------------------------------------------------------

  PipeNamespace(SqlValidatorImpl validator, SqlPipeQuery node,
      SqlSelect surrogateSelect, SqlValidatorScope scope,
      SqlNode enclosingNode) {
    super(validator, enclosingNode);
    this.node = node;
    this.surrogateSelect = surrogateSelect;
    this.scope = scope;
  }

  //~ Methods ----------------------------------------------------------------

  @Override public SqlNode getNode() {
    return node;
  }

  /** Returns the surrogate SELECT of this namespace's query. */
  public SqlSelect getSurrogateSelect() {
    return surrogateSelect;
  }

  @Override protected RelDataType validateImpl(RelDataType targetRowType) {
    if (node instanceof SqlPipeCall) {
      // Scopes are built during registration, but which of the input's
      // tables this operator can see (and so which aliases resolve) is known
      // only once the input is validated. Replace the scope's children here
      // so this operator's expressions see the correct table aliases.
      final SqlNode input = ((SqlPipeCall) node).getQuery();
      validator.validateQuery(input, scope, validator.unknownType);
      final SelectScope selectScope =
          (SelectScope) validator.getFromScope(surrogateSelect);
      selectScope.replaceChildren(validator.pipeChildren(input));
    }
    validator.validateQuery(surrogateSelect, scope, targetRowType);
    filterRequirement = getSurrogateNamespace().getFilterRequirement();
    return getSurrogateNamespace().getRowType();
  }

  /** Returns the namespace of the surrogate SELECT. */
  private SqlValidatorNamespace getSurrogateNamespace() {
    return validator.getNamespaceOrThrow(surrogateSelect);
  }

  @Override public boolean supportsModality(SqlModality modality) {
    return getSurrogateNamespace().supportsModality(modality);
  }

  @Override public SqlMonotonicity getMonotonicity(String columnName) {
    return getSurrogateNamespace().getMonotonicity(columnName);
  }

  /** Returns the scope children that an operator applied to this query
   * can see. */
  List<ScopeChild> outChildren() {
    if (outChildren == null) {
      final SelectScope selectScope =
          (SelectScope) validator.getFromScope(surrogateSelect);
      outChildren = passesThrough(selectScope)
          ? ImmutableList.copyOf(selectScope.children)
          : validator.singleAnonymousScopeChild(node, this);
    }
    return outChildren;
  }

  /** Returns whether this query's columns are the same as those of the
   * children of the surrogate's scope. */
  private boolean passesThrough(SelectScope selectScope) {
    final List<SqlNode> items =
        requireNonNull(selectScope.getExpandedSelectList(),
            "expandedSelectList");
    int i = 0;
    for (ScopeChild child : selectScope.children) {
      for (RelDataTypeField field
          : child.namespace.getRowType().getFieldList()) {
        if (i >= items.size()) {
          return false;
        }
        final SqlNode item = SqlUtil.stripAs(items.get(i++));
        if (!(item instanceof SqlIdentifier)
            || !((SqlIdentifier) item).names.equals(
                ImmutableList.of(child.name, field.getName()))) {
          return false;
        }
      }
    }
    return i == items.size();
  }
}
