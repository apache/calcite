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

import org.apache.calcite.plan.RelOptRule;
import org.apache.calcite.plan.RelOptRuleCall;
import org.apache.calcite.rel.RelCollations;
import org.apache.calcite.rel.RelFieldCollation;
import org.apache.calcite.rel.core.Window;
import org.apache.calcite.rel.logical.LogicalProject;
import org.apache.calcite.rel.logical.LogicalWindow;
import org.apache.calcite.rel.type.RelDataTypeField;
import org.apache.calcite.rel.type.RelRecordType;
import org.apache.calcite.rex.RexCall;
import org.apache.calcite.rex.RexInputRef;
import org.apache.calcite.rex.RexNode;
import org.apache.calcite.rex.RexShuttle;
import org.apache.calcite.sql.SqlAggFunction;
import org.apache.calcite.util.ImmutableBitSet;

import java.util.ArrayList;
import java.util.BitSet;
import java.util.List;

/**
 * Planner rule that pushes
 * a {@link org.apache.calcite.rel.logical.LogicalProject}
 * past a {@link org.apache.calcite.rel.logical.LogicalWindow}.
 */
public class ProjectWindowTransposeRule extends RelOptRule {
  /** The default instance of
   * {@link org.apache.calcite.rel.rules.ProjectWindowTransposeRule}. */
  public static final ProjectWindowTransposeRule INSTANCE = new ProjectWindowTransposeRule();

  private ProjectWindowTransposeRule() {
    super(
        operand(
            LogicalProject.class,
                operand(LogicalWindow.class,
                    any())));
  }

  @Override
  public void onMatch(RelOptRuleCall call) {
    final LogicalProject project = call.rel(0);
    final LogicalWindow window = call.rel(1);
    final List<RelDataTypeField> rowTypeWindowInput = window.getInput().getRowType().getFieldList();
    final int windowInputColumn = rowTypeWindowInput.size();

    // Record the window input columns which are actually referred
    // either in the LogicalProject above LogicalWindow or LogicalWindow itself
    final BitSet beReferred = findReference(project, window);

    // If all the the window input columns are referred,
    // it is impossible to trim anyone of them out
    if (beReferred.cardinality() == windowInputColumn) {
      return;
    }

    // Put a DrillProjectRel below LogicalWindow
    final List<RexNode> exps = new ArrayList<>();
    final List<RelDataTypeField> fields = new ArrayList<>();

    // Keep only the fields which are referred
    for  (int index = beReferred.nextSetBit(0); index >= 0;
        index = beReferred.nextSetBit(index + 1)) {
      final RelDataTypeField relDataTypeField = rowTypeWindowInput.get(index);
      exps.add(new RexInputRef(index, relDataTypeField.getType()));
      fields.add(relDataTypeField);
    }

    final LogicalProject projectBelowWindow = new LogicalProject(
        window.getCluster(),
        window.getTraitSet(),
        window.getInput(),
        exps,
        new RelRecordType(fields));

    // Create a new LogicalWindow with necessary inputs only
    final List<RelDataTypeField> inputWinFields = fields;
    final List<RelDataTypeField> outputWindFields = new ArrayList<>(inputWinFields);
    final List<Window.Group> groups = new ArrayList<>();

    // As the un-referred columns are trimmed by the LogicalProject,
    // the indices specified in LogicalWindow would need to be adjusted
    final RexShuttle indexAdjustment = new RexShuttle() {
      @Override
      public RexNode visitInputRef(RexInputRef inputRef) {
        final int index = inputRef.getIndex();
        final int newIndex = beReferred.get(0, index).cardinality();
        return new RexInputRef(newIndex, inputRef.getType());
      }

      @Override
      public RexNode visitCall(final RexCall call) {
        final List<RexNode> clonedOperands = visitList(call.operands, new boolean[]{false});
        if (call instanceof Window.RexWinAggCall) {
          return new Window.RexWinAggCall(
              (SqlAggFunction) call.getOperator(),
              call.getType(),
              clonedOperands,
              ((Window.RexWinAggCall) call).ordinal);
        } else {
          return call;
        }
      }
    };

    int aggCallIndex = windowInputColumn;
    for (Window.Group group : window.groups) {
      final ImmutableBitSet.Builder keys = ImmutableBitSet.builder();
      final List<RelFieldCollation> orderKeys = new ArrayList<>();
      final List<Window.RexWinAggCall> aggCalls = new ArrayList<>();

      // Adjust keys
      for (int index : group.keys) {
        keys.set(beReferred.get(0, index).cardinality());
      }

      // Adjust orderKeys
      for (RelFieldCollation relFieldCollation : group.orderKeys.getFieldCollations()) {
        final int index = relFieldCollation.getFieldIndex();
        orderKeys.add(relFieldCollation.copy(beReferred.get(0, index).cardinality()));
      }

      // Adjust Window Functions
      for (Window.RexWinAggCall rexWinAggCall : group.aggCalls) {
        aggCalls.add((Window.RexWinAggCall) rexWinAggCall.accept(indexAdjustment));

        final RelDataTypeField relDataTypeField =
            window.getRowType().getFieldList().get(aggCallIndex);
        outputWindFields.add(relDataTypeField);
        ++aggCallIndex;
      }

      groups.add(new Window.Group(
          keys.build(),
          group.isRows,
          group.lowerBound,
          group.upperBound,
          RelCollations.of(orderKeys),
          aggCalls));
    }

    final LogicalWindow newLogicalWindow = new LogicalWindow(
        window.getCluster(),
        window.getTraitSet(),
        projectBelowWindow,
        window.constants,
        new RelRecordType(outputWindFields),
        groups);

    // Modify the top LogicalProject
    final List<RexNode> topProjExps = new ArrayList<>();
    final RexShuttle topProjAdjustment = new RexShuttle() {
      @Override
      public RexNode visitInputRef(RexInputRef inputRef) {
        final int index = inputRef.getIndex();
        final int newIndex;
        if (index >= windowInputColumn) {
          newIndex = beReferred.cardinality() + (index - windowInputColumn);
        } else {
          newIndex = beReferred.get(0, index).cardinality();
        }
        return new RexInputRef(newIndex, inputRef.getType());
      }
    };

    for (RexNode rexNode : project.getChildExps()) {
      topProjExps.add(rexNode.accept(topProjAdjustment));
    }

    final LogicalProject newTopProj = project.copy(
        newLogicalWindow.getTraitSet(),
        newLogicalWindow,
        topProjExps,
        project.getRowType());

    if (ProjectRemoveRule.isTrivial(newTopProj)) {
      call.transformTo(newLogicalWindow);
    } else {
      call.transformTo(newTopProj);
    }
  }

  private BitSet findReference(final LogicalProject project, final LogicalWindow window) {
    final int windowInputColumn = window.getInput().getRowType().getFieldCount();
    final BitSet beReferred = new BitSet(window.getInput().getRowType().getFieldCount());

    final RexShuttle referenceFinder = new RexShuttle() {
      @Override
      public RexNode visitInputRef(RexInputRef inputRef) {
        final int index = inputRef.getIndex();
        if (index < windowInputColumn) {
          beReferred.set(index);
        }
        return inputRef;
      }
    };

    // Reference in LogicalProject
    for (RexNode rexNode : project.getChildExps()) {
      rexNode.accept(referenceFinder);
    }

    // Reference in LogicalWindow
    for (Window.Group group : window.groups) {
      // Reference in Partition-By
      for (int index : group.keys) {
        if (index < windowInputColumn) {
          beReferred.set(index);
        }
      }

      // Reference in Order-By
      for (RelFieldCollation relFieldCollation : group.orderKeys.getFieldCollations()) {
        if (relFieldCollation.getFieldIndex() < windowInputColumn) {
          beReferred.set(relFieldCollation.getFieldIndex());
        }
      }

      // Reference in Window Functions
      for (Window.RexWinAggCall rexWinAggCall : group.aggCalls) {
        rexWinAggCall.accept(referenceFinder);
      }
    }
    return beReferred;
  }
}

// End ProjectWindowTransposeRule.java
