package org.apache.calcite.adapter.graphql;

import org.apache.calcite.plan.RelOptCluster;
import org.apache.calcite.plan.RelOptCost;
import org.apache.calcite.plan.RelOptPlanner;
import org.apache.calcite.plan.RelTraitSet;
import org.apache.calcite.rel.RelNode;
import org.apache.calcite.rel.core.Project;
import org.apache.calcite.rel.metadata.RelMetadataQuery;
import org.apache.calcite.rel.type.RelDataType;
import org.apache.calcite.rex.RexInputRef;
import org.apache.calcite.rex.RexNode;

import com.google.common.collect.ImmutableList;
import com.google.common.collect.ImmutableSet;

import org.apache.calcite.util.Pair;

import org.apache.logging.log4j.LogManager;
import org.apache.logging.log4j.Logger;

import org.checkerframework.checker.nullness.qual.Nullable;

import java.util.ArrayList;
import java.util.List;

import static java.util.Objects.requireNonNull;

/**
 * Represents a relational algebra operation that projects a subset of fields from its input relational expression.
 * Extends {@link Project} class and implements {@link GraphQLRel} interface.
 */
class GraphQLProject extends Project implements GraphQLRel {
  private static final Logger LOGGER = LogManager.getLogger(GraphQLProject.class);

  final @Nullable List<Integer> projectedFields;

  /** Creates a GraphQLProject. */
  GraphQLProject(RelOptCluster cluster, RelTraitSet traitSet,
      RelNode input, List<? extends RexNode> projects, RelDataType rowType) {
    super(cluster, traitSet, ImmutableList.of(), input, projects, rowType, ImmutableSet.of());
    assert getConvention() == GraphQLRel.CONVENTION;
    assert getConvention() == input.getConvention();
    this.projectedFields = getProjectFields(projects);
    LOGGER.debug("Created GraphQLProject with projectedFields: {}", projectedFields);
  }

  @Override public RelOptCost computeSelfCost(RelOptPlanner planner,
      RelMetadataQuery mq) {
    final RelOptCost cost = super.computeSelfCost(planner, mq);
    return requireNonNull(cost, "cost").multiplyBy(0.1);
  }

  @Override public void implement(Implementor implementor) {
    LOGGER.debug("GraphQLProject.implement() called with projectedFields: {}", projectedFields);
    if (projectedFields != null) {
      LOGGER.debug("Setting projected fields before visiting input: {}", projectedFields);
      implementor.addProjectFields(projectedFields);
    }
    implementor.visitInput(0, getInput());
    for (Pair<RexNode, String> pair : getNamedProjects()) {
      assert pair.left != null;
      final String name = pair.right;
      final RexNode originalName = pair.left;
      LOGGER.debug("{} {}", name, originalName);
    }
  }

  @Override public Project copy(RelTraitSet traitSet, RelNode input,
      List<RexNode> projects, RelDataType rowType) {
    return new GraphQLProject(getCluster(), traitSet, input, projects,
        rowType);
  }

  static @Nullable List<Integer> getProjectFields(List<? extends RexNode> exps) {
    LOGGER.debug("getProjectFields called with expressions: {}", exps);
    final List<Integer> fields = new ArrayList<>();
    for (final RexNode exp : exps) {
      LOGGER.debug("Processing expression: {}", exp);
      if (exp instanceof RexInputRef) {
        fields.add(((RexInputRef) exp).getIndex());
        LOGGER.debug("Added field index: {}", ((RexInputRef) exp).getIndex());
      } else {
        LOGGER.debug("Expression is not RexInputRef, returning null");
        return null; // not a simple projection
      }
    }
    LOGGER.debug("Returning fields: {}", fields);
    return fields;
  }
}
