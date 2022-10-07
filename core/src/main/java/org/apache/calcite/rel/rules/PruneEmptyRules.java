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
import org.apache.calcite.plan.RelOptUtil;
import org.apache.calcite.plan.RelRule;
import org.apache.calcite.plan.RelTraitSet;
import org.apache.calcite.plan.hep.HepRelVertex;
import org.apache.calcite.plan.volcano.RelSubset;
import org.apache.calcite.rel.RelNode;
import org.apache.calcite.rel.SingleRel;
import org.apache.calcite.rel.core.Aggregate;
import org.apache.calcite.rel.core.Filter;
import org.apache.calcite.rel.core.Intersect;
import org.apache.calcite.rel.core.Join;
import org.apache.calcite.rel.core.JoinRelType;
import org.apache.calcite.rel.core.Minus;
import org.apache.calcite.rel.core.Project;
import org.apache.calcite.rel.core.Sort;
import org.apache.calcite.rel.core.Union;
import org.apache.calcite.rel.core.Values;
import org.apache.calcite.rel.logical.LogicalValues;
import org.apache.calcite.rex.RexDynamicParam;
import org.apache.calcite.rex.RexLiteral;
import org.apache.calcite.tools.RelBuilder;
import org.apache.calcite.tools.RelBuilderFactory;

import org.immutables.value.Value;

import java.util.Collections;
import java.util.List;
import java.util.function.Predicate;

import static com.google.common.collect.Iterables.concat;

/**
 * Collection of rules which remove sections of a query plan known never to
 * produce any rows.
 *
 * <p>Conventionally, the way to represent an empty relational expression is
 * with a {@link Values} that has no tuples.
 *
 * @see LogicalValues#createEmpty
 */
public abstract class PruneEmptyRules {
  //~ Static fields/initializers ---------------------------------------------

  /**
   * Abstract prune empty rule that implements SubstitutionRule interface.
   */
  protected abstract static class PruneEmptyRule
      extends RelRule<PruneEmptyRule.Config>
      implements SubstitutionRule {
    protected PruneEmptyRule(Config config) {
      super(config);
    }

    @Override public boolean autoPruneOld() {
      return true;
    }

    /** Rule configuration. */
    public interface Config extends RelRule.Config {
      @Override PruneEmptyRule toRule();
    }
  }

  /**
   * Rule that removes empty children of a
   * {@link org.apache.calcite.rel.core.Union}.
   *
   * <p>Examples:
   *
   * <ul>
   * <li>Union(Rel, Empty, Rel2) becomes Union(Rel, Rel2)
   * <li>Union(Rel, Empty, Empty) becomes Rel
   * <li>Union(Empty, Empty) becomes Empty
   * </ul>
   */
  public static final RelOptRule UNION_INSTANCE =
      ImmutableUnionEmptyPruneRuleConfig.of()
          .withOperandSupplier(b0 ->
              b0.operand(Union.class).unorderedInputs(b1 ->
                  b1.operand(Values.class)
                      .predicate(Values::isEmpty).noInputs()))
          .withDescription("Union")
          .toRule();


  /**
   * Rule that removes empty children of a
   * {@link org.apache.calcite.rel.core.Minus}.
   *
   * <p>Examples:
   *
   * <ul>
   * <li>Minus(Rel, Empty, Rel2) becomes Minus(Rel, Rel2)
   * <li>Minus(Empty, Rel) becomes Empty
   * </ul>
   */
  public static final RelOptRule MINUS_INSTANCE =
      ImmutableMinusEmptyPruneRuleConfig.of()
          .withOperandSupplier(b0 ->
              b0.operand(Minus.class).unorderedInputs(b1 ->
                  b1.operand(Values.class).predicate(Values::isEmpty)
                      .noInputs()))
          .withDescription("Minus")
          .toRule();

  /**
   * Rule that converts a
   * {@link org.apache.calcite.rel.core.Intersect} to
   * empty if any of its children are empty.
   *
   * <p>Examples:
   *
   * <ul>
   * <li>Intersect(Rel, Empty, Rel2) becomes Empty
   * <li>Intersect(Empty, Rel) becomes Empty
   * </ul>
   */
  public static final RelOptRule INTERSECT_INSTANCE =
      ImmutableIntersectEmptyPruneRuleConfig.of()
          .withOperandSupplier(b0 ->
              b0.operand(Intersect.class).unorderedInputs(b1 ->
                  b1.operand(Values.class).predicate(Values::isEmpty)
                      .noInputs()))
          .withDescription("Intersect")
          .toRule();

  private static boolean isEmpty(RelNode node) {
    if (node instanceof Values) {
      return ((Values) node).getTuples().isEmpty();
    }
    if (node instanceof HepRelVertex) {
      return isEmpty(((HepRelVertex) node).getCurrentRel());
    }
    // Note: relation input might be a RelSubset, so we just iterate over the relations
    // in order to check if the subset is equivalent to an empty relation.
    if (!(node instanceof RelSubset)) {
      return false;
    }
    RelSubset subset = (RelSubset) node;
    for (RelNode rel : subset.getRels()) {
      if (isEmpty(rel)) {
        return true;
      }
    }
    return false;
  }

  /**
   * Rule that converts a {@link org.apache.calcite.rel.core.Project}
   * to empty if its child is empty.
   *
   * <p>Examples:
   *
   * <ul>
   * <li>Project(Empty) becomes Empty
   * </ul>
   */
  public static final RelOptRule PROJECT_INSTANCE =
      ImmutableRemoveEmptySingleRuleConfig.of()
          .withDescription("PruneEmptyProject")
          .withOperandFor(Project.class, project -> true)
          .toRule();

  /**
   * Rule that converts a {@link org.apache.calcite.rel.core.Filter}
   * to empty if its child is empty.
   *
   * <p>Examples:
   *
   * <ul>
   * <li>Filter(Empty) becomes Empty
   * </ul>
   */
  public static final RelOptRule FILTER_INSTANCE =
      ImmutableRemoveEmptySingleRuleConfig.of()
          .withDescription("PruneEmptyFilter")
          .withOperandFor(Filter.class, singleRel -> true)
          .toRule();

  /**
   * Rule that converts a {@link org.apache.calcite.rel.core.Sort}
   * to empty if its child is empty.
   *
   * <p>Examples:
   *
   * <ul>
   * <li>Sort(Empty) becomes Empty
   * </ul>
   */
  public static final RelOptRule SORT_INSTANCE =
      ImmutableRemoveEmptySingleRuleConfig.of()
          .withDescription("PruneEmptySort")
          .withOperandFor(Sort.class, singleRel -> true)
          .toRule();

  /**
   * Rule that converts a {@link org.apache.calcite.rel.core.Sort}
   * to empty if it has {@code LIMIT 0}.
   *
   * <p>Examples:
   *
   * <ul>
   * <li>Sort[fetch=0] becomes Empty
   * </ul>
   */
  public static final RelOptRule SORT_FETCH_ZERO_INSTANCE =
      ImmutableSortFetchZeroRuleConfig.of()
          .withOperandSupplier(b ->
              b.operand(Sort.class).anyInputs())
          .withDescription("PruneSortLimit0")
          .toRule();

  /**
   * Rule that converts an {@link org.apache.calcite.rel.core.Aggregate}
   * to empty if its child is empty.
   *
   * <p>Examples:
   *
   * <ul>
   * <li>{@code Aggregate(key: [1, 3], Empty)} &rarr; {@code Empty}
   *
   * <li>{@code Aggregate(key: [], Empty)} is unchanged, because an aggregate
   * without a GROUP BY key always returns 1 row, even over empty input
   * </ul>
   *
   * @see AggregateValuesRule
   */
  public static final RelOptRule AGGREGATE_INSTANCE =
      ImmutableRemoveEmptySingleRuleConfig.of()
          .withDescription("PruneEmptyAggregate")
          .withOperandFor(Aggregate.class, Aggregate::isNotGrandTotal)
          .toRule();

  /**
   * Rule that converts a {@link org.apache.calcite.rel.core.Join}
   * to empty if its left child is empty.
   *
   * <p>Examples:
   *
   * <ul>
   * <li>Join(Empty, Scan(Dept), INNER) becomes Empty
   * <li>Join(Empty, Scan(Dept), LEFT) becomes Empty
   * <li>Join(Empty, Scan(Dept), SEMI) becomes Empty
   * <li>Join(Empty, Scan(Dept), ANTI) becomes Empty
   * </ul>
   */
  public static final RelOptRule JOIN_LEFT_INSTANCE =
      ImmutableJoinLeftEmptyRuleConfig.of()
          .withOperandSupplier(b0 ->
              b0.operand(Join.class).inputs(
                  b1 -> b1.operand(Values.class)
                      .predicate(Values::isEmpty).noInputs(),
                  b2 -> b2.operand(RelNode.class).anyInputs()))
          .withDescription("PruneEmptyJoin(left)")
          .toRule();

  /**
   * Rule that converts a {@link org.apache.calcite.rel.core.Join}
   * to empty if its right child is empty.
   *
   * <p>Examples:
   *
   * <ul>
   * <li>Join(Scan(Emp), Empty, INNER) becomes Empty
   * <li>Join(Scan(Emp), Empty, RIGHT) becomes Empty
   * <li>Join(Scan(Emp), Empty, SEMI) becomes Empty
   * <li>Join(Scan(Emp), Empty, ANTI) becomes Scan(Emp)
   * </ul>
   */
  public static final RelOptRule JOIN_RIGHT_INSTANCE =
      ImmutableJoinRightEmptyRuleConfig.of()
          .withOperandSupplier(b0 ->
              b0.operand(Join.class).inputs(
                  b1 -> b1.operand(RelNode.class).anyInputs(),
                  b2 -> b2.operand(Values.class).predicate(Values::isEmpty)
                      .noInputs()))
          .withDescription("PruneEmptyJoin(right)")
          .toRule();

  /** Planner rule that converts a single-rel (e.g. project, sort, aggregate or
   * filter) on top of the empty relational expression into empty. */
  public static class RemoveEmptySingleRule extends PruneEmptyRule {
    /** Creates a RemoveEmptySingleRule. */
    RemoveEmptySingleRule(RemoveEmptySingleRuleConfig config) {
      super(config);
    }

    @Deprecated // to be removed before 2.0
    public <R extends SingleRel> RemoveEmptySingleRule(Class<R> clazz,
        String description) {
      this(ImmutableRemoveEmptySingleRuleConfig.of().withDescription(description)
          .as(ImmutableRemoveEmptySingleRuleConfig.class)
          .withOperandFor(clazz, singleRel -> true));
    }

    @Deprecated // to be removed before 2.0
    public <R extends SingleRel> RemoveEmptySingleRule(Class<R> clazz,
        Predicate<R> predicate, RelBuilderFactory relBuilderFactory,
        String description) {
      this(ImmutableRemoveEmptySingleRuleConfig.of().withRelBuilderFactory(relBuilderFactory)
          .withDescription(description)
          .as(ImmutableRemoveEmptySingleRuleConfig.class)
          .withOperandFor(clazz, predicate));
    }

    @SuppressWarnings({"Guava", "UnnecessaryMethodReference"})
    @Deprecated // to be removed before 2.0
    public <R extends SingleRel> RemoveEmptySingleRule(Class<R> clazz,
        com.google.common.base.Predicate<R> predicate,
        RelBuilderFactory relBuilderFactory, String description) {
      this(ImmutableRemoveEmptySingleRuleConfig.of().withRelBuilderFactory(relBuilderFactory)
          .withDescription(description)
          .as(ImmutableRemoveEmptySingleRuleConfig.class)
          .withOperandFor(clazz, predicate::apply));
    }

    @Override public void onMatch(RelOptRuleCall call) {
      SingleRel singleRel = call.rel(0);
      RelNode emptyValues = call.builder().push(singleRel).empty().build();
      RelTraitSet traits = singleRel.getTraitSet();
      // propagate all traits (except convention) from the original singleRel into the empty values
      if (emptyValues.getConvention() != null) {
        traits = traits.replace(emptyValues.getConvention());
      }
      emptyValues = emptyValues.copy(traits, Collections.emptyList());
      call.transformTo(emptyValues);
    }

    /** Rule configuration. */
    @Value.Immutable
    public interface RemoveEmptySingleRuleConfig extends PruneEmptyRule.Config {
      @Override default RemoveEmptySingleRule toRule() {
        return new RemoveEmptySingleRule(this);
      }

      /** Defines an operand tree for the given classes. */
      default <R extends RelNode> RemoveEmptySingleRuleConfig withOperandFor(Class<R> relClass,
          Predicate<R> predicate) {
        return withOperandSupplier(b0 ->
            b0.operand(relClass).predicate(predicate).oneInput(b1 ->
                b1.operand(Values.class).predicate(Values::isEmpty).noInputs()))
            .as(RemoveEmptySingleRuleConfig.class);
      }
    }
  }

  /** Configuration for a rule that prunes empty inputs from a Minus. */
  @Value.Immutable
  public interface UnionEmptyPruneRuleConfig extends PruneEmptyRule.Config {
    @Override default PruneEmptyRule toRule() {
      return new PruneEmptyRule(this) {
        @Override public void onMatch(RelOptRuleCall call) {
          final Union union = call.rel(0);
          final List<RelNode> inputs = union.getInputs();
          assert inputs != null;
          final RelBuilder relBuilder = call.builder();
          int nonEmptyInputs = 0;
          for (RelNode input : inputs) {
            if (!isEmpty(input)) {
              relBuilder.push(input);
              nonEmptyInputs++;
            }
          }
          assert nonEmptyInputs < inputs.size()
              : "planner promised us at least one Empty child: "
              + RelOptUtil.toString(union);
          if (nonEmptyInputs == 0) {
            relBuilder.push(union).empty();
          } else {
            relBuilder.union(union.all, nonEmptyInputs);
            relBuilder.convert(union.getRowType(), true);
          }
          call.transformTo(relBuilder.build());
        }
      };
    }
  }

  /** Configuration for a rule that prunes empty inputs from a Minus. */
  @Value.Immutable
  public interface MinusEmptyPruneRuleConfig extends PruneEmptyRule.Config {
    @Override default PruneEmptyRule toRule() {
      return new PruneEmptyRule(this) {
        @Override public void onMatch(RelOptRuleCall call) {
          final Minus minus = call.rel(0);
          final List<RelNode> inputs = minus.getInputs();
          assert inputs != null;
          int nonEmptyInputs = 0;
          final RelBuilder relBuilder = call.builder();
          for (RelNode input : inputs) {
            if (!isEmpty(input)) {
              relBuilder.push(input);
              nonEmptyInputs++;
            } else if (nonEmptyInputs == 0) {
              // If the first input of Minus is empty, the whole thing is
              // empty.
              break;
            }
          }
          assert nonEmptyInputs < inputs.size()
              : "planner promised us at least one Empty child: "
              + RelOptUtil.toString(minus);
          if (nonEmptyInputs == 0) {
            relBuilder.push(minus).empty();
          } else {
            relBuilder.minus(minus.all, nonEmptyInputs);
            relBuilder.convert(minus.getRowType(), true);
          }
          call.transformTo(relBuilder.build());
        }
      };
    }
  }


  /** Configuration for a rule that prunes an Intersect if any of its inputs
   * is empty. */
  @Value.Immutable
  public interface IntersectEmptyPruneRuleConfig extends PruneEmptyRule.Config {
    @Override default PruneEmptyRule toRule() {
      return new PruneEmptyRule(this) {
        @Override public void onMatch(RelOptRuleCall call) {
          Intersect intersect = call.rel(0);
          final RelBuilder relBuilder = call.builder();
          relBuilder.push(intersect).empty();
          call.transformTo(relBuilder.build());
        }
      };
    }
  }

  /** Configuration for a rule that prunes a Sort if it has limit 0. */
  @Value.Immutable
  public interface SortFetchZeroRuleConfig extends PruneEmptyRule.Config {
    @Override default PruneEmptyRule toRule() {
      return new PruneEmptyRule(this) {
        @Override public void onMatch(RelOptRuleCall call) {
          Sort sort = call.rel(0);
          if (sort.fetch != null
              && !(sort.fetch instanceof RexDynamicParam)
              && RexLiteral.intValue(sort.fetch) == 0) {
            RelNode emptyValues = call.builder().push(sort).empty().build();
            RelTraitSet traits = sort.getTraitSet();
            // propagate all traits (except convention) from the original sort into the empty values
            if (emptyValues.getConvention() != null) {
              traits = traits.replace(emptyValues.getConvention());
            }
            emptyValues = emptyValues.copy(traits, Collections.emptyList());
            call.transformTo(emptyValues);
          }
        }

      };
    }
  }

  /** Configuration for rule that prunes a join it its left input is
   * empty. */
  @Value.Immutable
  public interface JoinLeftEmptyRuleConfig extends PruneEmptyRule.Config {
    @Override default PruneEmptyRule toRule() {
      return new PruneEmptyRule(this) {
        @Override public void onMatch(RelOptRuleCall call) {
          final Join join = call.rel(0);
          final Values empty = call.rel(1);
          final RelNode right = call.rel(2);
          final RelBuilder relBuilder = call.builder();
          if (join.getJoinType().generatesNullsOnLeft()) {
            // If "emp" is empty, "select * from emp right join dept" will have
            // the same number of rows as "dept", and null values for the
            // columns from "emp". The left side of the join can be removed.
            final List<RexLiteral> nullLiterals =
                Collections.nCopies(empty.getRowType().getFieldCount(),
                    relBuilder.literal(null));
            call.transformTo(
                relBuilder.push(right)
                    .project(concat(nullLiterals, relBuilder.fields()))
                    .convert(join.getRowType(), true)
                    .build());
            return;
          }
          call.transformTo(relBuilder.push(join).empty().build());
        }
      };
    }
  }

  /** Configuration for rule that prunes a join it its right input is
   * empty. */
  @Value.Immutable
  public interface JoinRightEmptyRuleConfig extends PruneEmptyRule.Config {
    @Override default PruneEmptyRule toRule() {
      return new PruneEmptyRule(this) {
        @Override public void onMatch(RelOptRuleCall call) {
          final Join join = call.rel(0);
          final RelNode left = call.rel(1);
          final Values empty = call.rel(2);
          final RelBuilder relBuilder = call.builder();
          if (join.getJoinType().generatesNullsOnRight()) {
            // If "dept" is empty, "select * from emp left join dept" will have
            // the same number of rows as "emp", and null values for the
            // columns from "dept". The right side of the join can be removed.
            final List<RexLiteral> nullLiterals =
                Collections.nCopies(empty.getRowType().getFieldCount(),
                    relBuilder.literal(null));
            call.transformTo(
                relBuilder.push(left)
                    .project(concat(relBuilder.fields(), nullLiterals))
                    .convert(join.getRowType(), true)
                    .build());
            return;
          }
          if (join.getJoinType() == JoinRelType.ANTI) {
            // In case of anti join: Join(X, Empty, ANTI) becomes X
            call.transformTo(join.getLeft());
            return;
          }
          call.transformTo(relBuilder.push(join).empty().build());
        }
      };
    }
  }
}
