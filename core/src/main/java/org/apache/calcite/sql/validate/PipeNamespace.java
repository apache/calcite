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
import org.apache.calcite.sql.SqlPipeOperator;
import org.apache.calcite.sql.SqlSelect;
import org.apache.calcite.sql.SqlUtil;

import com.google.common.collect.ImmutableList;

import org.checkerframework.checker.nullness.qual.Nullable;

import java.util.List;

import static java.util.Objects.requireNonNull;

/**
 * Namespace offered by a query written in SQL pipe syntax.
 *
 * <p>The query is validated through its surrogate SELECT; see
 * {@link SqlPipeOperator#surrogateSelect()}.
 */
public class PipeNamespace extends AbstractNamespace {
  private final SqlNode node;
  /** This operator's surrogate SELECT. */
  private final SqlSelect select;
  /** Scope that the query is written in, and that its input and surrogate
   * are validated in. */
  private final SqlValidatorScope scope;

  /** Scope children that an operator applied to this one can see. Computed
   * on demand, once this namespace has been validated. */
  private @Nullable List<ScopeChild> outChildren;

  //~ Constructors -----------------------------------------------------------

  PipeNamespace(SqlValidatorImpl validator, SqlNode node,
      SqlSelect select, SqlValidatorScope scope,
      SqlNode enclosingNode) {
    super(validator, enclosingNode);
    this.node = node;
    this.select = select;
    this.scope = scope;
  }

  //~ Methods ----------------------------------------------------------------

  @Override public SqlNode getNode() {
    return node;
  }

  /** Returns this operator's surrogate SELECT. */
  public SqlSelect getSelect() {
    return select;
  }

  @Override protected RelDataType validateImpl(RelDataType targetRowType) {
    if (node instanceof SqlPipeOperator) {
      // Validate the input first, so its tables can take the placeholder's
      // place in the surrogate's scope.
      final SqlNode input = ((SqlPipeOperator) node).getQuery();
      validator.validateQuery(input, scope, validator.unknownType);
      final SelectScope selectScope =
          (SelectScope) validator.getFromScope(select);
      selectScope.replaceChildren(validator.pipeChildren(input));
    }
    validator.validateQuery(select, scope, targetRowType);
    filterRequirement = delegate().getFilterRequirement();
    return delegate().getRowType();
  }

  /** Returns the namespace of the surrogate SELECT, which answers
   * what this query is asked about its rows, such as their modality. */
  private SqlValidatorNamespace delegate() {
    return validator.getNamespaceOrThrow(select);
  }

  @Override public boolean supportsModality(SqlModality modality) {
    return delegate().supportsModality(modality);
  }

  @Override public SqlMonotonicity getMonotonicity(String columnName) {
    return delegate().getMonotonicity(columnName);
  }

  /** Returns the scope children that an operator applied to this one can
   * see. This will be the children of the surrogate's scope if this query
   * passes them through, otherwise it will be an anonymous table. */
  List<ScopeChild> outChildren() {
    if (outChildren == null) {
      final SelectScope selectScope =
          (SelectScope) validator.getFromScope(select);
      outChildren = passesThrough(selectScope)
          ? ImmutableList.copyOf(selectScope.children)
          : validator.anonymousPipeChild(node, this);
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
