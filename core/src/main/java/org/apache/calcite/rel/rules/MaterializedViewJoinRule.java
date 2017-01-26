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
package org.apache.calcite.rel.rules;

import org.apache.calcite.plan.RelOptMaterialization;
import org.apache.calcite.plan.RelOptMaterializations;
import org.apache.calcite.plan.RelOptPlanner;
import org.apache.calcite.plan.RelOptRule;
import org.apache.calcite.plan.RelOptRuleCall;
import org.apache.calcite.plan.RelOptRuleOperand;
import org.apache.calcite.plan.RelOptTable;
import org.apache.calcite.plan.RelOptUtil;
import org.apache.calcite.plan.hep.HepPlanner;
import org.apache.calcite.plan.hep.HepProgram;
import org.apache.calcite.plan.hep.HepProgramBuilder;
import org.apache.calcite.plan.volcano.VolcanoPlanner;
import org.apache.calcite.rel.RelNode;
import org.apache.calcite.rel.core.Join;
import org.apache.calcite.rel.core.Project;
import org.apache.calcite.rel.core.RelFactories;
import org.apache.calcite.rel.core.TableScan;
import org.apache.calcite.rel.logical.LogicalProject;
import org.apache.calcite.rel.type.RelDataType;
import org.apache.calcite.rel.type.RelDataTypeField;
import org.apache.calcite.rex.RexCall;
import org.apache.calcite.rex.RexInputRef;
import org.apache.calcite.rex.RexNode;
import org.apache.calcite.rex.RexUtil;
import org.apache.calcite.sql.SqlKind;
import org.apache.calcite.sql.type.SqlTypeUtil;
import org.apache.calcite.tools.RelBuilderFactory;
import org.apache.calcite.util.ImmutableBitSet;
import org.apache.calcite.util.Pair;

import com.google.common.collect.ImmutableList;
import com.google.common.collect.ImmutableSet;
import com.google.common.collect.Lists;

import java.util.ArrayList;
import java.util.LinkedHashMap;
import java.util.List;
import java.util.Map;
import java.util.SortedMap;
import java.util.TreeMap;

/**
 * Planner rule that converts joins of multiple tables into a matching
 * materialized view
 */
public class MaterializedViewJoinRule extends RelOptRule {
  public static final MaterializedViewJoinRule INSTANCE_PROJECT =
      new MaterializedViewJoinRule(
          operand(LogicalProject.class,
              operand(Join.class,
                  operand(Project.class,
                      operand(TableScan.class, none())),
                  operand(Project.class,
                      operand(TableScan.class, none())))),
          RelFactories.LOGICAL_BUILDER,
          "MaterializedViewJoinRule(Project-Project)");

  public static final MaterializedViewJoinRule INSTANCE_TABLE_SCAN =
      new MaterializedViewJoinRule(
          operand(LogicalProject.class,
              operand(Join.class,
                  operand(TableScan.class, none()),
                  operand(TableScan.class, none()))),
          RelFactories.LOGICAL_BUILDER,
          "MaterializedViewJoinRule(TableScan-TableScan)");

  private final HepProgram multiJoinProgram = new HepProgramBuilder()
      .addRuleInstance(ProjectRemoveRule.INSTANCE)
      .addRuleInstance(ProjectJoinTransposeRule.INSTANCE)
      .addRuleInstance(JoinToMultiJoinRule.INSTANCE)
      .addRuleInstance(ProjectMultiJoinMergeRule.INSTANCE)
      .addRuleInstance(FilterMultiJoinMergeRule.INSTANCE)
      .build();

  //~ Constructors -----------------------------------------------------------

  /** Creates a MaterializedViewJoinRule. */
  protected MaterializedViewJoinRule(RelOptRuleOperand operand, RelBuilderFactory relBuilderFactory,
        String description) {
    super(operand, relBuilderFactory, description);
  }

  //~ Methods ----------------------------------------------------------------

  public void onMatch(RelOptRuleCall call) {
    final Project originalProject = call.rel(0);
    // Rebuild the tree
    final RelNode leftInput;
    final RelNode rightInput;
    if (call.getRelList().size() == 6) {
      leftInput = call.rel(2).copy(call.rel(2).getTraitSet(), ImmutableList.of(call.rel(3)));
      rightInput = call.rel(4).copy(call.rel(4).getTraitSet(), ImmutableList.of(call.rel(5)));
    } else {
      leftInput = call.rel(2);
      rightInput = call.rel(3);
    }
    final RelNode join = call.rel(1).copy(call.rel(1).getTraitSet(),
        ImmutableList.of(leftInput, rightInput));
    final RelNode project = call.rel(0).copy(call.rel(0).getTraitSet(), ImmutableList.of(join));

    // Convert the input expression into a MultiJoin
    RelOptPlanner planner = call.getPlanner();
    final HepPlanner hepPlanner =
        new HepPlanner(multiJoinProgram, planner.getContext());
    hepPlanner.setRoot(project);
    RelNode best = hepPlanner.findBestExp();

    if (best instanceof Project) {
      best = ((Project) best).getInput();
    }
    if (!(best instanceof MultiJoin)) {
      return;
    }
    apply(call, (MultiJoin) best, originalProject);
  }

  protected void apply(RelOptRuleCall call, MultiJoin join, Project originalProject) {
    if (!isSupportedJoin(join)) {
      return;
    }
    SortedMap<Integer, ImmutableBitSet> queryFilter = filterConditions(join);
    if (queryFilter == null) {
      return;
    }
    List<RelOptTable> queryTables = RelOptUtil.findAllTables(join);
    Map<Integer, Pair<RelOptTable, RexInputRef>> queryFields =
        originalFields(join, queryTables);
    if (queryFields == null) {
      return;
    }

    RelOptPlanner planner = call.getPlanner();
    List<RelOptMaterialization> materializations =
        planner instanceof VolcanoPlanner
            ? ((VolcanoPlanner) planner).getMaterializations()
            : ImmutableList.<RelOptMaterialization>of();
    if (!materializations.isEmpty()) {
      List<RelOptMaterialization> applicableMaterializations =
          RelOptMaterializations.getApplicableMaterializations(join, materializations);

      // Prepare a planner to convert views to MultiJoins
      HepPlanner hepPlanner =
          new HepPlanner(multiJoinProgram, planner.getContext());

      for (RelOptMaterialization materialization : applicableMaterializations) {
        // Skip over single table views
        RelNode target = materialization.queryRel;
        if (target instanceof TableScan
            || (target instanceof Project
                && ((Project) target).getInput() instanceof TableScan)) {
          continue;
        }

        // Convert the view into a MultiJoin
        hepPlanner.setRoot(target);
        target = hepPlanner.findBestExp();
        if (!(target instanceof Project)) { continue; }

        Project viewProject = (Project) target;
        if (!(viewProject.getInput() instanceof MultiJoin)) {
          continue;
        }
        MultiJoin viewJoin = (MultiJoin) viewProject.getInput();
        if (!isSupportedJoin(viewJoin)) {
          continue;
        }

        List<RelOptTable> viewTables = RelOptUtil.findAllTables(viewJoin);

        // Check that the same set of tables are in use
        if (queryTables.size() != viewTables.size()
            || !ImmutableSet.copyOf(queryTables).containsAll(viewTables)) {
          continue;
        }

        // Extra the conditions and field from the view and ensure
        // that they are all supported
        SortedMap<Integer, ImmutableBitSet> viewFilter = filterConditions(viewJoin);
        if (viewFilter == null) {
          continue;
        }
        Map<Integer, Pair<RelOptTable, RexInputRef>> viewFields =
            originalFields(viewJoin, viewTables);
        if (viewFields == null) {
          continue;
        }

        // If we fail to find one of the fields we are required
        // to project, we can't use this view
        List<RexNode> projects = materializedViewProjects(queryFields, queryFilter,
            viewFields, originalProject);
        if (projects.size() != originalProject.getNamedProjects().size()) {
          continue;
        }

        final RelNode newNode = originalProject.copy(originalProject.getTraitSet(),
                materialization.tableRel,
                projects, originalProject.getRowType());
        call.transformTo(newNode);
      }
    }
  }

  /**
   * Checks that the join consists of either table scans or projects of scans
   */
  private boolean isSimpleProjects(MultiJoin join) {
    for (RelNode input : join.getInputs()) {
      if (!(input instanceof TableScan)
            && !(input instanceof Project
                && ((Project) input).getInput() instanceof TableScan)) {
        return false;
      }
    }

    return true;
  }

  private boolean isSupportedJoin(MultiJoin join) {
    // We only support inner joins without post join filters over simple projects/scans
    return !join.containsOuter() && join.getPostJoinFilter() == null && isSimpleProjects(join);
  }

  /**
   * Produces a map from fields in a multijoin to references in the
   * original tables referenced in the join
   */
  private Map<Integer, Pair<RelOptTable, RexInputRef>> originalFields(MultiJoin join,
        List<RelOptTable> tables) {
    List<ImmutableBitSet> projFields = join.getProjFields();
    Map<Integer, Pair<RelOptTable, RexInputRef>> tableFields = new LinkedHashMap<>();
    List<RelNode> inputs = join.getInputs();
    int fieldNum = 0;
    for (int i = 0; i < projFields.size(); i++) {
      // Either get the project or construct a list projecting all fields
      List<RexNode> projects;
      if (inputs.get(i) instanceof Project) {
        projects = ((Project) inputs.get(i)).getProjects();
      } else {
        assert inputs.get(i) instanceof TableScan;
        List<RelDataTypeField> fields = inputs.get(i).getRowType().getFieldList();
        projects = new ArrayList<>();
        for (int j = 0; j < fields.size(); j++) {
          projects.add(new RexInputRef(j, fields.get(j).getType()));
        }
      }

      if (projFields.get(i) == null) { return null; }

      int bit = projFields.get(i).nextSetBit(0);
      while (bit != -1) {
        // We currently only support rewriting of views with simple field references
        if (!(projects.get(bit) instanceof RexInputRef)) {
          return null;
        }

        tableFields.put(fieldNum, Pair.of(tables.get(i), (RexInputRef) projects.get(bit)));
        fieldNum++;
        bit = projFields.get(i).nextSetBit(bit + 1);
      }
    }

    return tableFields;
  }

  /**
   * If the node represents a field reference, get its index
   */
  private Integer getFieldIndex(RexNode operand) {
    if (operand.isA(SqlKind.INPUT_REF)) {
      return ((RexInputRef) operand).getIndex();
    } else if (operand.isA(SqlKind.CAST)) {
      return getFieldIndex(((RexCall) operand).getOperands().get(0));
    } else {
      return null;
    }
  }

  /**
   * Construct a map of equivalence classes of all columns
   * in all tables used as input to the join
   */
  private SortedMap<Integer, ImmutableBitSet> filterConditions(MultiJoin join) {
    SortedMap<Integer, ImmutableBitSet> equiv = new TreeMap<>();
    RexNode filter = RexUtil.toCnf(join.getCluster().getRexBuilder(), join.getJoinFilter());
    for (RexNode conjunct : RelOptUtil.conjunctions(filter)) {
      List<RexNode> condition = RelOptUtil.disjunctions(conjunct);
      if (condition.size() == 1 && condition.get(0).isA(SqlKind.EQUALS)) {
        List<RexNode> operands = ((RexCall) condition.get(0)).getOperands();
        Integer index1 = getFieldIndex(operands.get(0));
        Integer index2 = getFieldIndex(operands.get(1));
        if (index1 == null || index2 == null) {
          // All operands to a condition must be field references or
          // simple casts of field references
          return null;
        }
        equiv.put(index1, ImmutableBitSet.of(index1, index2));
      } else {
        // We don't handle disjunctions or inequalities
        return null;
      }
    }

    equiv = ImmutableBitSet.closure(equiv);
    return equiv;
  }


  /**
   * Construct a list of projects we need on top of the materialized view
   */
  private List<RexNode> materializedViewProjects(
        Map<Integer, Pair<RelOptTable, RexInputRef>> queryFields,
        SortedMap<Integer, ImmutableBitSet> queryFilter,
        Map<Integer, Pair<RelOptTable, RexInputRef>> viewFields,
        Project originalProject) {
    List<Pair<RelOptTable, RexInputRef>> viewFieldList =
        Lists.newArrayList(viewFields.values().iterator());
    List<Pair<RelOptTable, RexInputRef>> queryFieldList =
        Lists.newArrayList(queryFields.values().iterator());
    List<RexNode> projects = new ArrayList<>();
    for (Map.Entry<Integer, Pair<RelOptTable, RexInputRef>> field
          : queryFields.entrySet()) {
      int fieldIndex = viewFieldList.indexOf(field.getValue());
      if (fieldIndex == -1) {
        // Check for equivalent fields in the view
        ImmutableBitSet queryEquiv = queryFilter.get(field.getKey());
        if (queryEquiv != null) {
          for (Integer index : queryEquiv) {
            fieldIndex = viewFieldList.indexOf(queryFieldList.get(index));
            if (fieldIndex != -1) {
              break;
            }
          }
        }
      }

      if (fieldIndex == -1) {
        break;
      }

      RelDataType type = field.getValue().right.getType();
      RelDataType originalType = originalProject.getProjects().get(projects.size()).getType();
      // if (originalType.equals(type)) {
      if (!SqlTypeUtil.canCastFrom(originalType, type, false)) {
        continue;
      }
      projects.add(new RexInputRef(fieldIndex, type));
    }

    return projects;
  }
}

// End MaterializedViewJoinRule.java
