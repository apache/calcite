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
import org.apache.calcite.sql.SqlCall;
import org.apache.calcite.sql.SqlIdentifier;
import org.apache.calcite.sql.SqlKind;
import org.apache.calcite.sql.SqlNode;
import org.apache.calcite.sql.SqlNodeList;
import org.apache.calcite.sql.parser.SqlParserPos;
import org.apache.calcite.util.Pair;

import com.google.common.collect.ImmutableList;

import org.checkerframework.checker.nullness.qual.MonotonicNonNull;
import org.checkerframework.checker.nullness.qual.Nullable;

import java.util.ArrayList;
import java.util.Collections;
import java.util.List;
import java.util.Objects;

import static org.apache.calcite.util.Static.RESOURCE;

/**
 * Namespace whose contents are defined by the type of an
 * {@link org.apache.calcite.sql.SqlIdentifier identifier}.
 */
public class IdentifierNamespace extends AbstractNamespace {
  //~ Instance fields --------------------------------------------------------

  private final SqlIdentifier id;
  private final SqlValidatorScope parentScope;
  public final @Nullable SqlNodeList extendList;

  /**
   * The underlying namespace. Often a {@link TableNamespace}.
   * Set on validate.
   */
  private @MonotonicNonNull SqlValidatorNamespace resolvedNamespace;

  /**
   * List of monotonic expressions. Set on validate.
   */
  private @Nullable List<Pair<SqlNode, SqlMonotonicity>> monotonicExprs;

  //~ Constructors -----------------------------------------------------------

  /**
   * Creates an IdentifierNamespace.
   *
   * @param validator     Validator
   * @param id            Identifier node (or "identifier EXTEND column-list")
   * @param extendList    Extension columns, or null
   * @param enclosingNode Enclosing node
   * @param parentScope   Parent scope which this namespace turns to in order to
   */
  IdentifierNamespace(SqlValidatorImpl validator, SqlIdentifier id,
      @Nullable SqlNodeList extendList, @Nullable SqlNode enclosingNode,
      SqlValidatorScope parentScope) {
    super(validator, enclosingNode);
    this.id = id;
    this.extendList = extendList;
    this.parentScope = Objects.requireNonNull(parentScope, "parentScope");
  }

  IdentifierNamespace(SqlValidatorImpl validator, SqlNode node,
      @Nullable SqlNode enclosingNode, SqlValidatorScope parentScope) {
    this(validator, split(node).left, split(node).right, enclosingNode,
        parentScope);
  }

  //~ Methods ----------------------------------------------------------------

  protected static Pair<SqlIdentifier, @Nullable SqlNodeList> split(SqlNode node) {
    switch (node.getKind()) {
    case EXTEND:
      final SqlCall call = (SqlCall) node;
      final SqlNode operand0 = call.operand(0);
      final SqlIdentifier identifier = operand0.getKind() == SqlKind.TABLE_REF
          ? ((SqlCall) operand0).operand(0)
          : (SqlIdentifier) operand0;
      return Pair.of(identifier, call.operand(1));
    case TABLE_REF:
      final SqlCall tableRef = (SqlCall) node;
      //noinspection ConstantConditions
      return Pair.of(tableRef.operand(0), null);
    default:
      //noinspection ConstantConditions
      return Pair.of((SqlIdentifier) node, null);
    }
  }

  private SqlValidatorNamespace resolveImpl(SqlIdentifier id) {
    final SqlNameMatcher nameMatcher = validator.catalogReader.nameMatcher();
    final SqlValidatorScope.ResolvedImpl resolved =
        new SqlValidatorScope.ResolvedImpl();
    final List<String> names = SqlIdentifier.toStar(id.names);
    try {
      parentScope.resolveTable(names, nameMatcher,
          SqlValidatorScope.Path.EMPTY, resolved);
    } catch (CyclicDefinitionException e) {
      if (e.depth == 1) {
        throw validator.newValidationError(id,
            RESOURCE.cyclicDefinition(id.toString(),
                SqlIdentifier.getString(e.path)));
      } else {
        throw new CyclicDefinitionException(e.depth - 1, e.path);
      }
    }
    SqlValidatorScope.Resolve previousResolve = null;
    if (resolved.count() == 1) {
      final SqlValidatorScope.Resolve resolve =
          previousResolve = resolved.only();
      if (resolve.remainingNames.isEmpty()) {
        return resolve.namespace;
      }
      // If we're not case sensitive, give an error.
      // If we're case sensitive, we'll shortly try again and give an error
      // then.
      if (!nameMatcher.isCaseSensitive()) {
        throw validator.newValidationError(id,
            RESOURCE.objectNotFoundWithin(resolve.remainingNames.get(0),
                SqlIdentifier.getString(resolve.path.stepNames())));
      }
    }

    // Failed to match.  If we're matching case-sensitively, try a more
    // lenient match. If we find something we can offer a helpful hint.
    if (nameMatcher.isCaseSensitive()) {
      final SqlNameMatcher liberalMatcher = SqlNameMatchers.liberal();
      resolved.clear();
      parentScope.resolveTable(names, liberalMatcher,
          SqlValidatorScope.Path.EMPTY, resolved);
      if (resolved.count() == 1) {
        final SqlValidatorScope.Resolve resolve = resolved.only();
        if (resolve.remainingNames.isEmpty()
            || previousResolve == null) {
          // We didn't match it case-sensitive, so they must have had the
          // right identifier, wrong case.
          //
          // If previousResolve is null, we matched nothing case-sensitive and
          // everything case-insensitive, so the mismatch must have been at
          // position 0.
          final int i = previousResolve == null ? 0
              : previousResolve.path.stepCount();
          final int offset = resolve.path.stepCount()
              + resolve.remainingNames.size() - names.size();
          final List<String> prefix =
              resolve.path.stepNames().subList(0, offset + i);
          final String next = resolve.path.stepNames().get(i + offset);
          if (prefix.isEmpty()) {
            throw validator.newValidationError(id,
                RESOURCE.objectNotFoundDidYouMean(names.get(i), next));
          } else {
            throw validator.newValidationError(id,
                RESOURCE.objectNotFoundWithinDidYouMean(names.get(i),
                    SqlIdentifier.getString(prefix), next));
          }
        } else {
          throw validator.newValidationError(id,
              RESOURCE.objectNotFoundWithin(resolve.remainingNames.get(0),
                  SqlIdentifier.getString(resolve.path.stepNames())));
        }
      }
    }
    throw validator.newValidationError(id,
        RESOURCE.objectNotFound(id.getComponent(0).toString()));
  }

  @Override public RelDataType validateImpl(RelDataType targetRowType) {
    resolvedNamespace = resolveImpl(id);
    if (resolvedNamespace instanceof TableNamespace) {
      SqlValidatorTable table = ((TableNamespace) resolvedNamespace).getTable();
      if (validator.config().identifierExpansion()) {
        // TODO:  expand qualifiers for column references also
        List<String> qualifiedNames = table.getQualifiedName();
        if (qualifiedNames != null) {
          // Assign positions to the components of the fully-qualified
          // identifier, as best we can. We assume that qualification
          // adds names to the front, e.g. FOO.BAR becomes BAZ.FOO.BAR.
          List<SqlParserPos> poses =
              new ArrayList<>(
                  Collections.nCopies(
                      qualifiedNames.size(), id.getParserPosition()));
          int offset = qualifiedNames.size() - id.names.size();

          // Test offset in case catalog supports fewer qualifiers than catalog
          // reader.
          if (offset >= 0) {
            for (int i = 0; i < id.names.size(); i++) {
              poses.set(i + offset, id.getComponentParserPosition(i));
            }
          }
          id.setNames(qualifiedNames, poses);
        }
      }
    }

    RelDataType rowType = resolvedNamespace.getRowType();

    if (extendList != null) {
      if (!(resolvedNamespace instanceof TableNamespace)) {
        throw new RuntimeException("cannot convert");
      }
      resolvedNamespace =
          ((TableNamespace) resolvedNamespace).extend(extendList);
      rowType = resolvedNamespace.getRowType();
    }

    // Build a list of monotonic expressions.
    final ImmutableList.Builder<Pair<SqlNode, SqlMonotonicity>> builder =
        ImmutableList.builder();
    List<RelDataTypeField> fields = rowType.getFieldList();
    for (RelDataTypeField field : fields) {
      final String fieldName = field.getName();
      final SqlMonotonicity monotonicity =
          resolvedNamespace.getMonotonicity(fieldName);
      if (monotonicity != null && monotonicity != SqlMonotonicity.NOT_MONOTONIC) {
        builder.add(
            Pair.of(new SqlIdentifier(fieldName, SqlParserPos.ZERO),
                monotonicity));
      }
    }
    monotonicExprs = builder.build();

    // Validation successful.
    return rowType;
  }

  public SqlIdentifier getId() {
    return id;
  }

  @Override public @Nullable SqlNode getNode() {
    return id;
  }

  @Override public SqlValidatorNamespace resolve() {
    assert resolvedNamespace != null : "must call validate first";
    return resolvedNamespace.resolve();
  }

  @Override public @Nullable SqlValidatorTable getTable() {
    return resolvedNamespace == null ? null : resolve().getTable();
  }

  @Override public List<Pair<SqlNode, SqlMonotonicity>> getMonotonicExprs() {
    List<Pair<SqlNode, SqlMonotonicity>> monotonicExprs = this.monotonicExprs;
    return monotonicExprs == null ? ImmutableList.of() : monotonicExprs;
  }

  @Override public SqlMonotonicity getMonotonicity(String columnName) {
    final SqlValidatorTable table = getTable();
    if (table == null) {
      return SqlMonotonicity.NOT_MONOTONIC;
    }
    return table.getMonotonicity(columnName);
  }

  @Override public boolean supportsModality(SqlModality modality) {
    final SqlValidatorTable table = getTable();
    if (table == null) {
      return modality == SqlModality.RELATION;
    }
    return table.supportsModality(modality);
  }
}
