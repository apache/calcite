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
package org.apache.calcite.sql2rel;

import org.apache.calcite.plan.RelOptCluster;
import org.apache.calcite.rel.RelNode;
import org.apache.calcite.rel.core.CorrelationId;
import org.apache.calcite.rel.logical.LogicalAggregate;
import org.apache.calcite.rel.logical.LogicalJoin;
import org.apache.calcite.rel.logical.LogicalMatch;
import org.apache.calcite.rel.type.RelDataType;
import org.apache.calcite.rel.type.RelDataTypeFactory;
import org.apache.calcite.rel.type.RelDataTypeField;
import org.apache.calcite.rex.RexBuilder;
import org.apache.calcite.rex.RexNode;
import org.apache.calcite.sql.validate.LateralScope;
import org.apache.calcite.sql.validate.ListScope;
import org.apache.calcite.sql.validate.SqlNameMatcher;
import org.apache.calcite.sql.validate.SqlQualified;
import org.apache.calcite.sql.validate.SqlValidatorNamespace;
import org.apache.calcite.sql.validate.SqlValidatorScope;
import org.apache.calcite.util.Pair;

import com.google.common.collect.ImmutableMap;
import com.google.common.collect.Iterables;

import org.checkerframework.checker.nullness.qual.Nullable;

import java.util.ArrayDeque;
import java.util.ArrayList;
import java.util.Deque;
import java.util.Iterator;
import java.util.List;
import java.util.Map;
import java.util.Objects;

import static java.util.Objects.requireNonNull;

/**
 * Resolves Sql identifiers.
 */
public class ColumnResolver {
  private final SqlNameMatcher nameMatcher;
  private final RelOptCluster relOptCluster;
  private final RexBuilder rexBuilder;
  private final Deque<CorrelateResolver> resolvers = new ArrayDeque<>();

  public ColumnResolver(SqlNameMatcher nameMatcher, RelOptCluster relOptCluster) {
    this.nameMatcher = nameMatcher;
    this.relOptCluster = relOptCluster;
    this.rexBuilder = relOptCluster.getRexBuilder();
  }

  public CorrelateResolver addResolver(SqlValidatorScope sqlValidatorScope,
      List<RelNode> childrenRel) {
    CorrelateResolver correlateResolver =
        new CorrelateResolver(relOptCluster, sqlValidatorScope, childrenRel);
    resolvers.add(correlateResolver);
    return correlateResolver;
  }

  public RexNode resolveTable(SqlValidatorScope scope, SqlQualified qualified,
      @Nullable List<RelNode> childrenRel, Map<RelNode, Integer> leaves) {
    final SqlValidatorScope.ResolvedImpl resolved =
        new SqlValidatorScope.ResolvedImpl();

    scope.resolve(qualified.prefix(), nameMatcher, false, resolved);
    if (resolved.count() != 1) {
      throw new AssertionError("no unique expression found for " + qualified
          + "; count is " + resolved.count());
    }
    final SqlValidatorScope.Resolve resolve = resolved.only();
    String fieldName = Iterables.getFirst(qualified.suffix(), null);
    assert fieldName != null;
    if (!(scope instanceof LateralScope) && resolve.scope == scope) {
      assert childrenRel != null && !childrenRel.isEmpty();
      return resolveCurrentScope(resolve, fieldName, childrenRel, leaves);
    }

    for (Iterator<CorrelateResolver> iter = resolvers.descendingIterator(); iter.hasNext();) {
      CorrelateResolver resolver = iter.next();
      RexNode rexNode = resolver.resolve(resolve, fieldName);
      if (null != rexNode) {
        return rexNode;
      }
    }

    throw new AssertionError("Failed to resolve " + qualified);
  }

  private RexNode resolveCurrentScope(SqlValidatorScope.Resolve resolve, String fieldName,
      List<RelNode> relNodeList, Map<RelNode, Integer> leaves) {
    int relIndex = resolve.path.steps().get(0).i;
    LookupContext lookupContext = new LookupContext(relNodeList, 0, leaves);
    Pair<RelNode, Integer> context = lookupContext.findRel(relIndex);
    //int offset = relNodeList.subList(0,relIndex)
    //    .stream().mapToInt(r -> r.getRowType().getFieldCount()).sum();
    //RexNode range = rexBuilder.makeRangeReference(relNode.getRowType(), offset, false);
    RexNode range = rexBuilder.makeRangeReference(context.left.getRowType(), context.right, false);
    RelDataType rowType = resolve.rowType();
    final RelDataTypeField field =
        requireNonNull(rowType.getField(fieldName, true, false),
            () -> "field " + fieldName);
    return rexBuilder.makeFieldAccess(range, field.getIndex());
  }

  /**
   * Resolves identifiers for correlated variables.
   */
  public static class CorrelateResolver {

    @Nullable public CorrelationId correlationId;
    @Nullable private RexNode correlateNode;
    @Nullable private Map<String, Integer> fields;
    private final RelOptCluster relOptCluster;
    private final SqlValidatorScope sqlValidatorScope;
    private final RelDataTypeFactory typeFactory;
    private final RexBuilder rexBuilder;
    private final List<RelNode> relNodeList;

    public CorrelateResolver(RelOptCluster relOptCluster,
        SqlValidatorScope sqlValidatorScope, List<RelNode> relNodeList) {
      this.relOptCluster = relOptCluster;
      this.sqlValidatorScope = Objects.requireNonNull(sqlValidatorScope, "sqlValidatorScope");
      this.relNodeList = relNodeList;
      this.typeFactory = relOptCluster.getTypeFactory();
      this.rexBuilder = relOptCluster.getRexBuilder();
    }

    public @Nullable RexNode resolve(SqlValidatorScope.Resolve resolve, String fieldName) {
      if (resolve.scope != sqlValidatorScope) {
        return null;
      } else if (correlationId == null) {
        correlationId = relOptCluster.createCorrel();
        correlateNode = createCorrel(resolve);
      }
      final int fieldIndex = requireNonNull(fields.get(fieldName), "field " + fieldName);
      return rexBuilder.makeFieldAccess(correlateNode, fieldIndex);
    }

    private RexNode createCorrel(SqlValidatorScope.Resolve resolve) {
      final RelDataTypeFactory.Builder builder = typeFactory.builder();
      final ListScope ancestorScope1 = (ListScope)
          requireNonNull(resolve.scope, "resolve.scope");
      final ImmutableMap.Builder<String, Integer> fields = ImmutableMap.builder();
      int i = 0;
      int offset = 0;
      for (SqlValidatorNamespace c : ancestorScope1.getChildren()) {
        if (ancestorScope1.isChildNullable(i)) {
          for (final RelDataTypeField f : c.getRowType().getFieldList()) {
            builder.add(f.getName(), typeFactory.createTypeWithNullability(f.getType(), true));
          }
        } else {
          builder.addAll(c.getRowType().getFieldList());
        }
        if (i == resolve.path.steps().get(0).i) {
          for (RelDataTypeField field : c.getRowType().getFieldList()) {
            fields.put(field.getName(), field.getIndex() + offset);
          }
        }
        ++i;
        offset += c.getRowType().getFieldCount();
      }
      this.fields = fields.build();
      return rexBuilder.makeCorrel(builder.uniquify().build(), correlationId);
    }
  }


  private static void flatten(
      List<RelNode> rels,
      int systemFieldCount,
      int[] start,
      List<Pair<RelNode, Integer>> relOffsetList,
      Map<RelNode, Integer> leaves) {
    for (RelNode rel : rels) {
      if (leaves.containsKey(rel)) {
        relOffsetList.add(
            Pair.of(rel, start[0]));
        start[0] += leaves.get(rel);
      } else if (rel instanceof LogicalMatch) {
        relOffsetList.add(
            Pair.of(rel, start[0]));
        start[0] += rel.getRowType().getFieldCount();
      } else {
        if (rel instanceof LogicalJoin
            || rel instanceof LogicalAggregate) {
          start[0] += systemFieldCount;
        }
        flatten(
            rel.getInputs(),
            systemFieldCount,
            start,
            relOffsetList,
            leaves);
      }
    }
  }

  /**
   * Context to find a relational expression to a field offset.
   */
  private static class LookupContext {
    private final List<Pair<RelNode, Integer>> relOffsetList =
        new ArrayList<>();

    /**
     * Creates a LookupContext with multiple input relational expressions.
     */
    LookupContext(List<RelNode> rels, int systemFieldCount, Map<RelNode, Integer> leaves) {
      flatten(rels, systemFieldCount, new int[]{0}, relOffsetList, leaves);
    }

    /**
     * Returns the relational expression with a given offset, and the
     * ordinal in the combined row of its first field.
     *
     * <p>For example, in {@code Emp JOIN Dept}, findRel(1) returns the
     * relational expression for {@code Dept} and offset 6 (because
     * {@code Emp} has 6 fields, therefore the first field of {@code Dept}
     * is field 6.
     *
     * @param offset Offset of relational expression in FROM clause
     * @return Relational expression and the ordinal of its first field
     */
    Pair<RelNode, Integer> findRel(int offset) {
      return relOffsetList.get(offset);
    }
  }
}
