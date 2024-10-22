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
package org.apache.calcite.plan;

import org.apache.calcite.config.CalciteSystemProperty;
import org.apache.calcite.rel.RelNode;
import org.apache.calcite.rel.RelShuttleImpl;
import org.apache.calcite.rel.core.Filter;
import org.apache.calcite.rel.core.Project;
import org.apache.calcite.rel.core.TableScan;
import org.apache.calcite.rel.logical.LogicalJoin;
import org.apache.calcite.rel.metadata.DefaultRelMetadataProvider;
import org.apache.calcite.rel.rules.CoreRules;
import org.apache.calcite.rex.RexNode;
import org.apache.calcite.rex.RexUtil;
import org.apache.calcite.schema.Table;
import org.apache.calcite.schema.impl.StarTable;
import org.apache.calcite.sql.SqlExplainFormat;
import org.apache.calcite.sql.SqlExplainLevel;
import org.apache.calcite.tools.Program;
import org.apache.calcite.tools.Programs;
import org.apache.calcite.util.Util;
import org.apache.calcite.util.mapping.Mappings;

import com.google.common.collect.ImmutableList;

import org.checkerframework.checker.nullness.qual.Nullable;

import java.util.ArrayList;
import java.util.List;
import java.util.Objects;

import static org.apache.calcite.linq4j.Nullness.castNonNull;

import static java.util.Objects.requireNonNull;

/**
 * Records that a particular query is materialized by a particular table.
 */
public class RelOptMaterialization {
  public final RelNode tableRel;
  public final @Nullable RelOptTable starRelOptTable;
  public final @Nullable StarTable starTable;
  public final List<String> qualifiedTableName;
  public final RelNode queryRel;

  /**
   * Creates a RelOptMaterialization.
   */
  public RelOptMaterialization(RelNode tableRel, RelNode queryRel,
      @Nullable RelOptTable starRelOptTable, List<String> qualifiedTableName) {
    this.queryRel = requireNonNull(queryRel, "queryRel");
    this.tableRel =
        RelOptUtil.createCastRel(requireNonNull(tableRel, "tableRel"),
            queryRel.getRowType(), false);
    this.starRelOptTable = starRelOptTable;
    if (starRelOptTable == null) {
      this.starTable = null;
    } else {
      this.starTable = starRelOptTable.unwrapOrThrow(StarTable.class);
    }
    this.qualifiedTableName = qualifiedTableName;
  }

  /**
   * Converts a relational expression to one that uses a
   * {@link org.apache.calcite.schema.impl.StarTable}.
   *
   * <p>The relational expression is already in leaf-join-form, per
   * {@link #toLeafJoinForm(org.apache.calcite.rel.RelNode)}.
   *
   * @return Rewritten expression, or null if expression cannot be rewritten
   * to use the star
   */
  public static @Nullable RelNode tryUseStar(RelNode rel,
      final RelOptTable starRelOptTable) {
    final StarTable starTable = starRelOptTable.unwrapOrThrow(StarTable.class);
    RelNode rel2 =
        rel.accept(new RelShuttleImpl() {
          @Override public RelNode visit(TableScan scan) {
            RelOptTable relOptTable = scan.getTable();
            final Table table = relOptTable.unwrap(Table.class);
            if (Objects.equals(table, starTable.tables.get(0))) {
              Mappings.TargetMapping mapping =
                  Mappings.createShiftMapping(
                      starRelOptTable.getRowType().getFieldCount(),
                      0, 0, relOptTable.getRowType().getFieldCount());

              final RelOptCluster cluster = scan.getCluster();
              final RelNode scan2 =
                  starRelOptTable.toRel(ViewExpanders.simpleContext(cluster));
              return RelOptUtil.createProject(scan2,
                  Mappings.asListNonNull(mapping.inverse()));
            }
            return scan;
          }

          @Override public RelNode visit(LogicalJoin join) {
            for (;;) {
              RelNode rel = super.visit(join);
              if (rel == join || !(rel instanceof LogicalJoin)) {
                return rel;
              }
              join = (LogicalJoin) rel;
              final ProjectFilterTable left =
                  ProjectFilterTable.of(join.getLeft());
              if (left != null) {
                final ProjectFilterTable right =
                    ProjectFilterTable.of(join.getRight());
                if (right != null) {
                  try {
                    match(left, right, join.getCluster());
                  } catch (Util.FoundOne e) {
                    return (RelNode) requireNonNull(e.getNode(),
                        "FoundOne.getNode");
                  }
                }
              }
            }
          }

          /** Throws a {@link org.apache.calcite.util.Util.FoundOne} containing
           * a {@link org.apache.calcite.rel.logical.LogicalTableScan} on
           * success.  (Yes, an exception for normal operation.) */
          private void match(ProjectFilterTable left, ProjectFilterTable right,
              RelOptCluster cluster) {
            final Mappings.TargetMapping leftMapping = left.mapping();
            final Mappings.TargetMapping rightMapping = right.mapping();
            final RelOptTable leftRelOptTable = left.getTable();
            final Table leftTable = leftRelOptTable.unwrap(Table.class);
            final int leftCount = leftRelOptTable.getRowType().getFieldCount();
            final RelOptTable rightRelOptTable = right.getTable();
            final Table rightTable = rightRelOptTable.unwrap(Table.class);
            if (leftTable instanceof StarTable
                && rightTable != null
                && ((StarTable) leftTable).tables.contains(rightTable)) {
              final int offset =
                  ((StarTable) leftTable).columnOffset(rightTable);
              Mappings.TargetMapping mapping =
                  Mappings.merge(leftMapping,
                      Mappings.offsetTarget(
                          Mappings.offsetSource(rightMapping, offset),
                          leftMapping.getTargetCount()));
              final RelNode project =
                  RelOptUtil.createProject(
                      leftRelOptTable.toRel(ViewExpanders.simpleContext(cluster)),
                      Mappings.asListNonNull(mapping.inverse()));
              final List<RexNode> conditions = new ArrayList<>();
              if (left.condition != null) {
                conditions.add(left.condition);
              }
              if (right.condition != null) {
                conditions.add(
                    RexUtil.apply(mapping,
                        RexUtil.shift(right.condition, offset)));
              }
              final RelNode filter =
                  RelOptUtil.createFilter(project, conditions);
              throw new Util.FoundOne(filter);
            }
            if (rightTable instanceof StarTable
                && leftTable != null
                && ((StarTable) rightTable).tables.contains(leftTable)) {
              final int offset =
                  ((StarTable) rightTable).columnOffset(leftTable);
              Mappings.TargetMapping mapping =
                  Mappings.merge(
                      Mappings.offsetSource(leftMapping, offset),
                      Mappings.offsetTarget(rightMapping, leftCount));
              final RelNode project =
                  RelOptUtil.createProject(
                      rightRelOptTable.toRel(ViewExpanders.simpleContext(cluster)),
                      Mappings.asListNonNull(mapping.inverse()));
              final List<RexNode> conditions = new ArrayList<>();
              if (left.condition != null) {
                conditions.add(
                    RexUtil.apply(mapping,
                        RexUtil.shift(left.condition, offset)));
              }
              if (right.condition != null) {
                conditions.add(RexUtil.apply(mapping, right.condition));
              }
              final RelNode filter =
                  RelOptUtil.createFilter(project, conditions);
              throw new Util.FoundOne(filter);
            }
          }
        });
    if (rel2 == rel) {
      // No rewrite happened.
      return null;
    }
    final Program program =
        Programs.hep(
            ImmutableList.of(CoreRules.PROJECT_FILTER_TRANSPOSE,
                CoreRules.AGGREGATE_PROJECT_MERGE,
                CoreRules.AGGREGATE_FILTER_TRANSPOSE),
        false,
        DefaultRelMetadataProvider.INSTANCE);
    return program.run(castNonNull(null), rel2, castNonNull(null),
        ImmutableList.of(),
        ImmutableList.of());
  }

  /** A table scan and optional project mapping and filter condition. */
  private static class ProjectFilterTable {
    final @Nullable RexNode condition;
    final Mappings.@Nullable TargetMapping mapping;
    final TableScan scan;

    private ProjectFilterTable(@Nullable RexNode condition,
        Mappings.@Nullable TargetMapping mapping, TableScan scan) {
      this.condition = condition;
      this.mapping = mapping;
      this.scan = requireNonNull(scan, "scan");
    }

    static @Nullable ProjectFilterTable of(RelNode node) {
      if (node instanceof Filter) {
        final Filter filter = (Filter) node;
        return of2(filter.getCondition(), filter.getInput());
      } else {
        return of2(null, node);
      }
    }

    private static @Nullable ProjectFilterTable of2(@Nullable RexNode condition, RelNode node) {
      if (node instanceof Project) {
        final Project project = (Project) node;
        return of3(condition, project.getMapping(), project.getInput());
      } else {
        return of3(condition, null, node);
      }
    }

    private static @Nullable ProjectFilterTable of3(@Nullable RexNode condition,
        Mappings.@Nullable TargetMapping mapping, RelNode node) {
      if (node instanceof TableScan) {
        return new ProjectFilterTable(condition, mapping,
            (TableScan) node);
      } else {
        return null;
      }
    }

    public Mappings.TargetMapping mapping() {
      return mapping != null
          ? mapping
          : Mappings.createIdentity(scan.getRowType().getFieldCount());
    }

    public RelOptTable getTable() {
      return scan.getTable();
    }
  }

  /**
   * Converts a relational expression to a form where
   * {@link org.apache.calcite.rel.logical.LogicalJoin}s are
   * as close to leaves as possible.
   */
  public static RelNode toLeafJoinForm(RelNode rel) {
    final Program program =
        Programs.hep(
            ImmutableList.of(CoreRules.JOIN_PROJECT_RIGHT_TRANSPOSE,
                CoreRules.JOIN_PROJECT_LEFT_TRANSPOSE,
                CoreRules.FILTER_INTO_JOIN,
                CoreRules.PROJECT_REMOVE,
                CoreRules.PROJECT_MERGE),
        false,
        DefaultRelMetadataProvider.INSTANCE);
    if (CalciteSystemProperty.DEBUG.value()) {
      System.out.println(
          RelOptUtil.dumpPlan("before", rel, SqlExplainFormat.TEXT,
              SqlExplainLevel.DIGEST_ATTRIBUTES));
    }
    final RelNode rel2 =
        program.run(castNonNull(null), rel, castNonNull(null),
            ImmutableList.of(), ImmutableList.of());
    if (CalciteSystemProperty.DEBUG.value()) {
      System.out.println(
          RelOptUtil.dumpPlan("after", rel2, SqlExplainFormat.TEXT,
              SqlExplainLevel.DIGEST_ATTRIBUTES));
    }
    return rel2;
  }
}
