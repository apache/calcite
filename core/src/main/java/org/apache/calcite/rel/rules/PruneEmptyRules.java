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
import org.apache.calcite.rel.core.Correlate;
import org.apache.calcite.rel.core.Filter;
import org.apache.calcite.rel.core.Intersect;
import org.apache.calcite.rel.core.Join;
import org.apache.calcite.rel.core.JoinRelType;
import org.apache.calcite.rel.core.Minus;
import org.apache.calcite.rel.core.Project;
import org.apache.calcite.rel.core.Sort;
import org.apache.calcite.rel.core.TableScan;
import org.apache.calcite.rel.core.Union;
import org.apache.calcite.rel.core.Values;
import org.apache.calcite.rel.logical.LogicalValues;
import org.apache.calcite.rel.metadata.RelMdUtil;
import org.apache.calcite.rel.type.RelDataType;
import org.apache.calcite.rex.RexDynamicParam;
import org.apache.calcite.rex.RexLiteral;
import org.apache.calcite.tools.RelBuilder;
import org.apache.calcite.tools.RelBuilderFactory;

import org.immutables.value.Value;

import java.util.Collections;
import java.util.List;
import java.util.function.Predicate;

import static com.google.common.collect.Iterables.concat;

import static java.util.Objects.requireNonNull;

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
  public static final RelOptRule UNION_INSTANCE = UnionEmptyPruneRuleConfig.DEFAULT.toRule();


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
  public static final RelOptRule MINUS_INSTANCE = MinusEmptyPruneRuleConfig.DEFAULT.toRule();

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
      IntersectEmptyPruneRuleConfig.DEFAULT.toRule();

  private static boolean isEmpty(RelNode node) {
    if (node instanceof Values) {
      return ((Values) node).getTuples().isEmpty();
    }
    if (node instanceof HepRelVertex) {
      return isEmpty(node.stripped());
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
   * Rule that converts a {@link org.apache.calcite.rel.core.TableScan}
   * to empty if the table has no rows in it.
   *
   * <p>The rule exploits the
   * {@link org.apache.calcite.rel.metadata.RelMdMaxRowCount} to derive if the
   * table is empty or not.
   */
  public static final RelOptRule EMPTY_TABLE_INSTANCE =
      ImmutableZeroMaxRowsRuleConfig.DEFAULT.toRule();

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
      RemoveEmptySingleRule.RemoveEmptySingleRuleConfig.PROJECT.toRule();

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
      RemoveEmptySingleRule.RemoveEmptySingleRuleConfig.FILTER.toRule();

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
      RemoveEmptySingleRule.RemoveEmptySingleRuleConfig.SORT.toRule();

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
      SortFetchZeroRuleConfig.DEFAULT.toRule();

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
      RemoveEmptySingleRule.RemoveEmptySingleRuleConfig.AGGREGATE.toRule();

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
      JoinLeftEmptyRuleConfig.DEFAULT.toRule();

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
      JoinRightEmptyRuleConfig.DEFAULT.toRule();

  /**
   * Rule that converts a {@link Correlate} to empty if its right child is empty.
   *
   * <p>Examples:
   * <ul>
   * <li>Correlate(Scan(Emp), Empty, INNER) becomes Empty</li>
   * <li>Correlate(Scan(Emp), Empty, SEMI) becomes Empty</li>
   * <li>Correlate(Scan(Emp), Empty, ANTI) becomes Scan(Emp)</li>
   * <li>Correlate(Scan(Emp), Empty, LEFT) becomes Project(Scan(Emp)) where the Project adds
   * additional typed null columns to match the join type output.</li>
   * </ul>
   */
  public static final RelOptRule CORRELATE_RIGHT_INSTANCE =
      CorrelateRightEmptyRuleConfig.DEFAULT.toRule();

  /**
   * Rule that converts a {@link Correlate} to empty if its left child is empty.
   */
  public static final RelOptRule CORRELATE_LEFT_INSTANCE =
      CorrelateLeftEmptyRuleConfig.DEFAULT.toRule();

  /**
   * Rule that converts a relation into empty.
   *
   * <p>The users can control the application of the rule by:
   *
   * <ul>
   * <li>calling the appropriate constructor and passing the necessary
   * configuration;
   *
   * <li>extending the class through inheritance and overriding
   * {@link RemoveEmptySingleRule#matches(RelOptRuleCall)}).
   * </ul>
   *
   * <p>When using the deprecated constructors it is only possible to convert
   * relations which strictly have a single input ({@link SingleRel}). */
  public static class RemoveEmptySingleRule extends PruneEmptyRule {
    /** Creates a RemoveEmptySingleRule. */
    RemoveEmptySingleRule(PruneEmptyRule.Config config) {
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
      RelNode singleRel = call.rel(0);
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
      RemoveEmptySingleRuleConfig PROJECT = ImmutableRemoveEmptySingleRuleConfig.of()
          .withDescription("PruneEmptyProject")
          .withOperandFor(Project.class, project -> true);
      RemoveEmptySingleRuleConfig FILTER = ImmutableRemoveEmptySingleRuleConfig.of()
          .withDescription("PruneEmptyFilter")
          .withOperandFor(Filter.class, singleRel -> true);
      RemoveEmptySingleRuleConfig SORT = ImmutableRemoveEmptySingleRuleConfig.of()
          .withDescription("PruneEmptySort")
          .withOperandFor(Sort.class, singleRel -> true);
      RemoveEmptySingleRuleConfig AGGREGATE = ImmutableRemoveEmptySingleRuleConfig.of()
          .withDescription("PruneEmptyAggregate")
          .withOperandFor(Aggregate.class, Aggregate::isNotGrandTotal);

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
    UnionEmptyPruneRuleConfig DEFAULT = ImmutableUnionEmptyPruneRuleConfig.of()
        .withOperandSupplier(b0 ->
            b0.operand(Union.class).unorderedInputs(b1 ->
                b1.operand(Values.class)
                    .predicate(Values::isEmpty).noInputs()))
        .withDescription("Union");

    @Override default PruneEmptyRule toRule() {
      return new PruneEmptyRule(this) {
        @Override public void onMatch(RelOptRuleCall call) {
          final Union union = call.rel(0);
          final List<RelNode> inputs = requireNonNull(union.getInputs());
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
          } else if (nonEmptyInputs == 1 && !union.all) {
            relBuilder.distinct();
            relBuilder.convert(union.getRowType(), true);
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
    MinusEmptyPruneRuleConfig DEFAULT = ImmutableMinusEmptyPruneRuleConfig.of()
        .withOperandSupplier(
            b0 -> b0.operand(Minus.class).unorderedInputs(
                b1 -> b1.operand(Values.class).predicate(Values::isEmpty).noInputs()))
        .withDescription("Minus");

    @Override default PruneEmptyRule toRule() {
      return new PruneEmptyRule(this) {
        @Override public void onMatch(RelOptRuleCall call) {
          final Minus minus = call.rel(0);
          final List<RelNode> inputs = requireNonNull(minus.getInputs());
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
          } else if (nonEmptyInputs == 1 && !minus.all) {
            relBuilder.distinct();
            relBuilder.convert(minus.getRowType(), true);
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
    IntersectEmptyPruneRuleConfig DEFAULT = ImmutableIntersectEmptyPruneRuleConfig.of()
        .withOperandSupplier(b0 ->
            b0.operand(Intersect.class).unorderedInputs(b1 ->
                b1.operand(Values.class).predicate(Values::isEmpty)
                    .noInputs()))
        .withDescription("Intersect");

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
    SortFetchZeroRuleConfig DEFAULT = ImmutableSortFetchZeroRuleConfig.of()
        .withOperandSupplier(b -> b.operand(Sort.class).anyInputs())
        .withDescription("PruneSortLimit0");

    @Override default PruneEmptyRule toRule() {
      return new RemoveEmptySingleRule(this) {
        @Override public boolean matches(final RelOptRuleCall call) {
          Sort sort = call.rel(0);
          return sort.fetch != null
              && !(sort.fetch instanceof RexDynamicParam)
              && RexLiteral.intValue(sort.fetch) == 0;
        }

      };
    }
  }

  /** Configuration for rule that prunes a join it its left input is
   * empty. */
  @Value.Immutable
  public interface JoinLeftEmptyRuleConfig extends PruneEmptyRule.Config {
    JoinLeftEmptyRuleConfig DEFAULT = ImmutableJoinLeftEmptyRuleConfig.of()
        .withOperandSupplier(b0 ->
            b0.operand(Join.class).inputs(
                b1 -> b1.operand(Values.class).predicate(Values::isEmpty).noInputs(),
                b2 -> b2.operand(RelNode.class).anyInputs()))
        .withDescription("PruneEmptyJoin(left)");

    @Override default PruneEmptyRule toRule() {
      return new PruneEmptyRule(this) {
        @Override public void onMatch(RelOptRuleCall call) {
          final Join join = call.rel(0);
          final RelNode right = call.rel(2);
          final RelBuilder relBuilder = call.builder();
          if (join.getJoinType().generatesNullsOnLeft()) {
            // If "emp" is empty, "select * from emp right join dept" will have
            // the same number of rows as "dept", and null values for the
            // columns from "emp". The left side of the join can be removed.
            call.transformTo(padWithNulls(relBuilder, right, join.getRowType(), true));
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
    JoinRightEmptyRuleConfig DEFAULT = ImmutableJoinRightEmptyRuleConfig.of()
        .withOperandSupplier(b0 ->
            b0.operand(Join.class).inputs(
                b1 -> b1.operand(RelNode.class).anyInputs(),
                b2 -> b2.operand(Values.class).predicate(Values::isEmpty).noInputs()))
        .withDescription("PruneEmptyJoin(right)");

    @Override default PruneEmptyRule toRule() {
      return new PruneEmptyRule(this) {
        @Override public void onMatch(RelOptRuleCall call) {
          final Join join = call.rel(0);
          final RelNode left = call.rel(1);
          final RelBuilder relBuilder = call.builder();
          if (join.getJoinType().generatesNullsOnRight()) {
            // If "dept" is empty, "select * from emp left join dept" will have
            // the same number of rows as "emp", and null values for the
            // columns from "dept". The right side of the join can be removed.
            call.transformTo(padWithNulls(relBuilder, left, join.getRowType(), false));
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

  private static RelNode padWithNulls(RelBuilder builder, RelNode input, RelDataType resultType,
      boolean leftPadding) {
    int padding = resultType.getFieldCount() - input.getRowType().getFieldCount();
    List<RexLiteral> nullLiterals = Collections.nCopies(padding, builder.literal(null));
    builder.push(input);
    if (leftPadding) {
      builder.project(concat(nullLiterals, builder.fields()));
    } else {
      builder.project(concat(builder.fields(), nullLiterals));
    }
    return builder.convert(resultType, true).build();
  }

  /** Configuration for rule that prunes a correlate if its left input is empty. */
  @Value.Immutable
  public interface CorrelateLeftEmptyRuleConfig extends PruneEmptyRule.Config {
    CorrelateLeftEmptyRuleConfig DEFAULT = ImmutableCorrelateLeftEmptyRuleConfig.of()
        .withOperandSupplier(b0 ->
            b0.operand(Correlate.class).inputs(
                b1 -> b1.operand(Values.class).predicate(Values::isEmpty).noInputs(),
                b2 -> b2.operand(RelNode.class).anyInputs()))
        .withDescription("PruneEmptyCorrelate(left)");
    @Override default PruneEmptyRule toRule() {
      return new RemoveEmptySingleRule(this);
    }
  }

  /** Configuration for rule that prunes a correlate if its right input is empty. */
  @Value.Immutable
  public interface CorrelateRightEmptyRuleConfig extends PruneEmptyRule.Config {
    CorrelateRightEmptyRuleConfig DEFAULT = ImmutableCorrelateRightEmptyRuleConfig.of()
        .withOperandSupplier(b0 ->
            b0.operand(Correlate.class).inputs(
                b1 -> b1.operand(RelNode.class).anyInputs(),
                b2 -> b2.operand(Values.class).predicate(Values::isEmpty).noInputs()))
        .withDescription("PruneEmptyCorrelate(right)");

    @Override default PruneEmptyRule toRule() {
      return new PruneEmptyRule(this) {
        @Override public void onMatch(RelOptRuleCall call) {
          final Correlate corr = call.rel(0);
          final RelNode left = call.rel(1);
          RelBuilder b = call.builder();
          final RelNode newRel;
          switch (corr.getJoinType()) {
          case LEFT:
            newRel = padWithNulls(b, left, corr.getRowType(), false);
            break;
          case INNER:
          case SEMI:
            newRel = b.push(corr).empty().build();
            break;
          case ANTI:
            newRel = left;
            break;
          default:
            throw new IllegalStateException("Correlate does not support " + corr.getJoinType());
          }
          call.transformTo(newRel);
        }
      };
    }
  }

  /** Configuration for rule that transforms an empty relational expression into
   * an empty values.
   *
   * <p>It relies on {@link org.apache.calcite.rel.metadata.RelMdMaxRowCount} to
   * derive if the relation is empty or not. If the stats are not available then
   * the rule is a noop. */
  @Value.Immutable
  public interface ZeroMaxRowsRuleConfig extends PruneEmptyRule.Config {
    ZeroMaxRowsRuleConfig DEFAULT = ImmutableZeroMaxRowsRuleConfig.of()
        .withOperandSupplier(b0 -> b0.operand(TableScan.class).noInputs())
        .withDescription("PruneZeroRowsTable");

    @Override default PruneEmptyRule toRule() {
      return new RemoveEmptySingleRule(this) {
        @Override public boolean matches(RelOptRuleCall call) {
          RelNode node = call.rel(0);
          return RelMdUtil.isRelDefinitelyEmpty(call.getMetadataQuery(), node);
        }
      };
    }
  }
}
