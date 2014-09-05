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
package org.eigenbase.relopt;

import org.eigenbase.rel.*;
import org.eigenbase.rel.metadata.DefaultRelMetadataProvider;
import org.eigenbase.rel.rules.MergeProjectRule;
import org.eigenbase.rel.rules.PullUpProjectsAboveJoinRule;
import org.eigenbase.sql.SqlExplainLevel;
import org.eigenbase.util.Util;
import org.eigenbase.util.mapping.Mappings;

import net.hydromatic.optiq.Table;
import net.hydromatic.optiq.impl.StarTable;
import net.hydromatic.optiq.prepare.OptiqPrepareImpl;
import net.hydromatic.optiq.tools.Program;
import net.hydromatic.optiq.tools.Programs;

import com.google.common.collect.ImmutableList;

/**
 * Records that a particular query is materialized by a particular table.
 */
public class RelOptMaterialization {
  public final RelNode tableRel;
  public final RelOptTable starRelOptTable;
  public final StarTable starTable;
  public final RelOptTable table;
  public final RelNode queryRel;

  /**
   * Creates a RelOptMaterialization.
   */
  public RelOptMaterialization(RelNode tableRel, RelNode queryRel,
      RelOptTable starRelOptTable) {
    this.tableRel = tableRel;
    this.starRelOptTable = starRelOptTable;
    if (starRelOptTable == null) {
      this.starTable = null;
    } else {
      this.starTable = starRelOptTable.unwrap(StarTable.class);
      assert starTable != null;
    }
    this.table = tableRel.getTable();
    this.queryRel = queryRel;
  }

  /**
   * Converts a relational expression to one that uses a
   * {@link net.hydromatic.optiq.impl.StarTable}.
   * The relational expression is already in leaf-join-form, per
   * {@link #toLeafJoinForm(org.eigenbase.rel.RelNode)}.
   */
  public static RelNode tryUseStar(RelNode rel,
      final RelOptTable starRelOptTable) {
    final StarTable starTable = starRelOptTable.unwrap(StarTable.class);
    assert starTable != null;
    return rel.accept(
        new RelShuttleImpl() {
          @Override
          public RelNode visit(TableAccessRelBase scan) {
            RelOptTable relOptTable = scan.getTable();
            final Table table = relOptTable.unwrap(Table.class);
            if (table.equals(starTable.tables.get(0))) {
              Mappings.TargetMapping mapping =
                  Mappings.createShiftMapping(
                      starRelOptTable.getRowType().getFieldCount(),
                      0, 0, relOptTable.getRowType().getFieldCount());

              final RelOptCluster cluster = scan.getCluster();
              final RelNode scan2 =
                  starRelOptTable.toRel(RelOptUtil.getContext(cluster));
              return RelOptUtil.createProject(scan2,
                  Mappings.asList(mapping.inverse()));
            }
            return scan;
          }

          @Override
          public RelNode visit(JoinRel join) {
            for (;;) {
              RelNode rel = super.visit(join);
              if (rel == join || !(rel instanceof JoinRel)) {
                return rel;
              }
              join = (JoinRel) rel;
              final RelNode left = join.getLeft();
              final RelNode right = join.getRight();
              try {
                if (left instanceof TableAccessRelBase
                    && right instanceof TableAccessRelBase) {
                  match(left, null, right, null, join.getCluster());
                }
                if (isProjectedTable(left)
                    && right instanceof TableAccessRelBase) {
                  final ProjectRel leftProject = (ProjectRel) left;
                  match(leftProject.getChild(), leftProject.getMapping(), right,
                      null, join.getCluster());
                }
                if (left instanceof TableAccessRelBase
                    && isProjectedTable(right)) {
                  final ProjectRel rightProject = (ProjectRel) right;
                  match(left, null, rightProject.getChild(),
                      rightProject.getMapping(), join.getCluster());
                }
                if (isProjectedTable(left)
                    && isProjectedTable(right)) {
                  final ProjectRel leftProject = (ProjectRel) left;
                  final ProjectRel rightProject = (ProjectRel) right;
                  match(leftProject.getChild(), leftProject.getMapping(),
                      rightProject.getChild(), rightProject.getMapping(),
                      join.getCluster());
                }
              } catch (Util.FoundOne e) {
                return (RelNode) e.getNode();
              }
            }
          }

          private boolean isProjectedTable(RelNode rel) {
            return rel instanceof ProjectRel
                && ((ProjectRel) rel).isMapping()
                && ((ProjectRel) rel).getChild() instanceof TableAccessRelBase;
          }

          /** Throws a {@link org.eigenbase.util.Util.FoundOne} containing a
           * {@link org.eigenbase.rel.TableAccessRel} on success.
           * (Yes, an exception for normal operation.) */
          private void match(RelNode left, Mappings.TargetMapping leftMapping,
              RelNode right, Mappings.TargetMapping rightMapping,
              RelOptCluster cluster) {
            if (leftMapping == null) {
              leftMapping =
                  Mappings.createIdentity(left.getRowType().getFieldCount());
            }
            if (rightMapping == null) {
              rightMapping =
                  Mappings.createIdentity(right.getRowType().getFieldCount());
            }
            final RelOptTable leftRelOptTable = left.getTable();
            final Table leftTable = leftRelOptTable.unwrap(Table.class);
            final RelOptTable rightRelOptTable = right.getTable();
            final Table rightTable = rightRelOptTable.unwrap(Table.class);
            if (leftTable instanceof StarTable
                && ((StarTable) leftTable).tables.contains(rightTable)) {
              Mappings.TargetMapping mapping =
                  Mappings.merge(leftMapping,
                      Mappings.offset(rightMapping,
                          ((StarTable) leftTable).columnOffset(rightTable),
                          leftRelOptTable.getRowType().getFieldCount()));
              throw new Util.FoundOne(
                  RelOptUtil.createProject(
                      new TableAccessRel(cluster, leftRelOptTable),
                      Mappings.asList(mapping.inverse())));
            }
            if (rightTable instanceof StarTable
                && ((StarTable) rightTable).tables.contains(leftTable)) {
              assert false; // TODO:
              Mappings.TargetMapping mapping =
                  Mappings.append(leftMapping, rightMapping);
              throw new Util.FoundOne(
                  RelOptUtil.createProject(
                      new TableAccessRel(cluster, rightRelOptTable),
                      Mappings.asList(mapping.inverse())));
            }
          }
        });
  }

  /**
   * Converts a relational expression to a form where
   * {@link org.eigenbase.rel.JoinRel}s are
   * as close to leaves as possible.
   */
  public static RelNode toLeafJoinForm(RelNode rel) {
    final Program program = Programs.hep(
        ImmutableList.of(
            PullUpProjectsAboveJoinRule.RIGHT_PROJECT,
            PullUpProjectsAboveJoinRule.LEFT_PROJECT,
            MergeProjectRule.INSTANCE),
        false,
        new DefaultRelMetadataProvider());
    if (OptiqPrepareImpl.DEBUG) {
      System.out.println(
          RelOptUtil.dumpPlan(
              "before", rel, false, SqlExplainLevel.DIGEST_ATTRIBUTES));
    }
    final RelNode rel2 = program.run(null, rel, null);
    if (OptiqPrepareImpl.DEBUG) {
      System.out.println(
          RelOptUtil.dumpPlan(
              "after", rel2, false, SqlExplainLevel.DIGEST_ATTRIBUTES));
    }
    return rel2;
  }
}

// End RelOptMaterialization.java
