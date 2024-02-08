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
import org.apache.calcite.rel.type.RelDataTypeFactory;
import org.apache.calcite.rel.type.RelDataTypeField;
import org.apache.calcite.sql.SqlCall;
import org.apache.calcite.sql.SqlIdentifier;
import org.apache.calcite.sql.SqlJoin;
import org.apache.calcite.sql.SqlKind;
import org.apache.calcite.sql.SqlNode;
import org.apache.calcite.sql.SqlOperatorTable;
import org.apache.calcite.sql.SqlSelect;
import org.apache.calcite.sql.SqlWithItem;
import org.apache.calcite.sql.util.SqlShuttle;
import org.apache.calcite.util.ImmutableBitSet;

import com.google.common.collect.ImmutableList;

import org.checkerframework.checker.nullness.qual.Nullable;

import java.util.ArrayList;
import java.util.HashSet;
import java.util.LinkedHashSet;
import java.util.List;
import java.util.Set;
import java.util.TreeSet;
import java.util.stream.Collectors;

import static java.util.Objects.requireNonNull;

/**
 * Default implementation of {@link AlwaysFilterValidator}.
 */
public class AlwaysFilterValidatorImpl extends SqlValidatorImpl
    implements AlwaysFilterValidator  {

  public AlwaysFilterValidatorImpl(SqlOperatorTable opTab,
      SqlValidatorCatalogReader catalogReader,
      RelDataTypeFactory typeFactory, Config config) {
    super(opTab, catalogReader, typeFactory, config, null);
  }

  @Override public SqlValidatorCatalogReader getCatalogReader() {
    return catalogReader;
  }

  @Override public SqlNode validate(SqlNode topNode) {
    SqlValidatorScope scope = new EmptyScope(this);
    scope = new CatalogScope(scope, ImmutableList.of("CATALOG"));
    if (topNode.isA(SqlKind.TOP_LEVEL)) {
      registerQuery(scope, null, topNode, topNode, null,
          false);
    }
    // The old way
    Set<String> alwaysFilterFields = new HashSet<>();
    topNode.validateAlwaysFilter(this, scope, alwaysFilterFields);

    // The new way
    final SqlValidatorNamespace namespace = getNamespace(topNode);
    requireNonNull(namespace, "namespace");
    final ImmutableBitSet mustFilterFields = namespace.getMustFilterFields();
    if (!mustFilterFields.isEmpty()) {
      final Set<String> alwaysFilterFields2 = new TreeSet<>();
      final List<String> fields = namespace.getRowType().getFieldNames();
      mustFilterFields.forEachInt(i ->
          alwaysFilterFields2.add(fields.get(i)));
      throw newAlwaysFilterValidationException(alwaysFilterFields2);
    }
    return topNode;
  }

  @Override public void validateQueryAlwaysFilter(SqlNode node,
      SqlValidatorScope scope, Set<String> alwaysFilterFields) {
    final SqlValidatorNamespace ns = getNamespaceOrThrow(node, scope);
    final RelDataType rowType = ns.getRowType();
    final ImmutableBitSet filterOrdinals = ns.getMustFilterFields();
    Set<String> alwaysFilterFields2 = new LinkedHashSet<>();
    filterOrdinals.forEachInt(i -> {
      final List<RelDataTypeField> fields = rowType.getFieldList();
      if (i < fields.size()) {
        alwaysFilterFields2.add(fields.get(i).getName().replaceAll("0*$", ""));
      }
    });
    Set<String> alwaysFilterFields3 = new TreeSet<>();
    ns.validateAlwaysFilter(alwaysFilterFields3);
    if(false)
    assert alwaysFilterFields3.equals(alwaysFilterFields2)
        : "different filter fields for node [" + node + "]\n"
        + "expected: " + alwaysFilterFields3 + "\n"
        + "actual:   " + alwaysFilterFields2 + "\n";
    alwaysFilterFields.addAll(alwaysFilterFields3);
  }

  @Override public void validateWithItemAlwaysFilter(SqlWithItem withItem,
      Set<String> alwaysFilterFields) {
    validateSelect((SqlSelect) withItem.query, alwaysFilterFields);
  }

  private static void removeIdentifier(Set<String> alwaysFilterFields,
      List<String> identifiers) {
    for (String identifier : identifiers) {
      alwaysFilterFields.remove(identifier);
    }
  }

  // TODO: javadoc
  // TODO: needs to be public?
  // TODO: make behavior deterministic (e.g. if Set is a HashSet)
  // TODO: throw a validator exception (with error position) and check that
  //   position in the tests
  // TODO: maybe just create the exception, don't throw (whether other code that
  //   creates validator exceptions does)
  public static RuntimeException newAlwaysFilterValidationException(
      Set<String> alwaysFilterFields) {
    throw new RuntimeException(
        "SQL statement did not contain filters on the following fields: "
            + alwaysFilterFields);
  }

  @Override public void validateSelect(SqlSelect select,
      Set<String> alwaysFilterFields) {
    final SelectScope fromScope = (SelectScope) getFromScope(select);
    SqlNode fromNode = select.getFrom();
    if (fromNode != null) {
      validateFromAlwaysFilter(select.getFrom(), fromScope, alwaysFilterFields);
      validateClause(select.getWhere(), alwaysFilterFields);
      validateClause(select.getHaving(), alwaysFilterFields);
    }
  }

  private void validateClause(@Nullable SqlNode node,
      Set<String> alwaysFilterFields) {
    if (node != null) {
      List<SqlIdentifier> sqlIdentifiers = collectSqlIdentifiers(node);
      List<String> identifierNames =
          sqlIdentifiers.stream()
              .map(i -> i.names.get(i.names.size() - 1))
              .collect(Collectors.toList());
      removeIdentifier(alwaysFilterFields, identifierNames);
    }
  }

  /** Collects all {@link org.apache.calcite.sql.SqlIdentifier} instances in an
   * expression. */
  private static List<SqlIdentifier> collectSqlIdentifiers(SqlNode node) {
    List<SqlIdentifier> list = new ArrayList<>();
    node.accept(
        new SqlShuttle() {
          @Override public @Nullable SqlNode visit(SqlIdentifier id) {
            list.add(id);
            return super.visit(id);
          }
        });
    return list;
  }

  @Override public void validateJoin(SqlJoin join, SqlValidatorScope scope,
      Set<String> alwaysFilterFields) {
    validateFromAlwaysFilter(join.getLeft(), scope, alwaysFilterFields);
    validateFromAlwaysFilter(join.getRight(), scope, alwaysFilterFields);
  }

  protected void validateFromAlwaysFilter(
      @Nullable SqlNode node,
      SqlValidatorScope scope,
      Set<String> alwaysFilterFields) {
    if (node != null) {
      switch (node.getKind()) {
      case AS:
      case TABLE_REF:
        validateFromAlwaysFilter(((SqlCall) node).operand(0), scope,
            alwaysFilterFields);
        return;
      case JOIN:
        validateJoin((SqlJoin) node, scope, alwaysFilterFields);
        return;
      default:
        validateQueryAlwaysFilter(node, scope, alwaysFilterFields);
      }
    }
  }
}
