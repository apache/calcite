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
package org.apache.calcite.rel.metadata;

import org.apache.calcite.adapter.enumerable.EnumerableCorrelate;
import org.apache.calcite.adapter.enumerable.EnumerableHashJoin;
import org.apache.calcite.adapter.enumerable.EnumerableMergeJoin;
import org.apache.calcite.adapter.enumerable.EnumerableMergeUnion;
import org.apache.calcite.adapter.enumerable.EnumerableNestedLoopJoin;
import org.apache.calcite.adapter.jdbc.JdbcToEnumerableConverter;
import org.apache.calcite.linq4j.Ord;
import org.apache.calcite.plan.RelOptTable;
import org.apache.calcite.plan.hep.HepRelVertex;
import org.apache.calcite.plan.volcano.RelSubset;
import org.apache.calcite.rel.RelCollation;
import org.apache.calcite.rel.RelCollationTraitDef;
import org.apache.calcite.rel.RelCollations;
import org.apache.calcite.rel.RelFieldCollation;
import org.apache.calcite.rel.RelNode;
import org.apache.calcite.rel.core.Calc;
import org.apache.calcite.rel.core.Filter;
import org.apache.calcite.rel.core.Join;
import org.apache.calcite.rel.core.JoinRelType;
import org.apache.calcite.rel.core.Match;
import org.apache.calcite.rel.core.Project;
import org.apache.calcite.rel.core.Sort;
import org.apache.calcite.rel.core.SortExchange;
import org.apache.calcite.rel.core.TableModify;
import org.apache.calcite.rel.core.TableScan;
import org.apache.calcite.rel.core.Values;
import org.apache.calcite.rel.core.Window;
import org.apache.calcite.rel.type.RelDataType;
import org.apache.calcite.rex.RexCall;
import org.apache.calcite.rex.RexCallBinding;
import org.apache.calcite.rex.RexInputRef;
import org.apache.calcite.rex.RexLiteral;
import org.apache.calcite.rex.RexNode;
import org.apache.calcite.rex.RexProgram;
import org.apache.calcite.sql.validate.SqlMonotonicity;
import org.apache.calcite.util.ImmutableBitSet;
import org.apache.calcite.util.ImmutableIntList;
import org.apache.calcite.util.Pair;
import org.apache.calcite.util.Util;

import com.google.common.collect.ImmutableList;
import com.google.common.collect.LinkedListMultimap;
import com.google.common.collect.Multimap;
import com.google.common.collect.Ordering;

import org.checkerframework.checker.nullness.qual.Nullable;

import java.util.ArrayList;
import java.util.Collection;
import java.util.HashMap;
import java.util.List;
import java.util.Map;
import java.util.NavigableSet;
import java.util.Objects;
import java.util.SortedSet;
import java.util.TreeSet;
import java.util.stream.Collectors;

/**
 * RelMdCollation supplies a default implementation of
 * {@link org.apache.calcite.rel.metadata.RelMetadataQuery#collations}
 * for the standard logical algebra.
 */
public class RelMdCollation
    implements MetadataHandler<BuiltInMetadata.Collation> {
  public static final RelMetadataProvider SOURCE =
      ReflectiveRelMetadataProvider.reflectiveSource(
          new RelMdCollation(), BuiltInMetadata.Collation.Handler.class);

  //~ Constructors -----------------------------------------------------------

  private RelMdCollation() {}

  //~ Methods ----------------------------------------------------------------

  @Override public MetadataDef<BuiltInMetadata.Collation> getDef() {
    return BuiltInMetadata.Collation.DEF;
  }

  /** Catch-all implementation for
   * {@link BuiltInMetadata.Collation#collations()},
   * invoked using reflection, for any relational expression not
   * handled by a more specific method.
   *
   * <p>{@link org.apache.calcite.rel.core.Union},
   * {@link org.apache.calcite.rel.core.Intersect},
   * {@link org.apache.calcite.rel.core.Minus},
   * {@link org.apache.calcite.rel.core.Join},
   * {@link org.apache.calcite.rel.core.Correlate}
   * do not in general return sorted results
   * (but implementations using particular algorithms may).
   *
   * @param rel Relational expression
   * @return Relational expression's collations
   *
   * @see org.apache.calcite.rel.metadata.RelMetadataQuery#collations(RelNode)
   */
  public @Nullable ImmutableList<RelCollation> collations(RelNode rel,
      RelMetadataQuery mq) {
    return null;
  }

  private static <E> @Nullable ImmutableList<E> copyOf(@Nullable Collection<? extends E> values) {
    return values == null ? null : ImmutableList.copyOf(values);
  }

  public @Nullable ImmutableList<RelCollation> collations(Window rel,
      RelMetadataQuery mq) {
    return copyOf(window(mq, rel.getInput(), rel.groups));
  }

  public @Nullable ImmutableList<RelCollation> collations(Match rel,
      RelMetadataQuery mq) {
    return copyOf(
        match(mq, rel.getInput(), rel.getRowType(), rel.getPattern(),
            rel.isStrictStart(), rel.isStrictEnd(),
            rel.getPatternDefinitions(), rel.getMeasures(), rel.getAfter(),
            rel.getSubsets(), rel.isAllRows(), rel.getPartitionKeys(),
            rel.getOrderKeys(), rel.getInterval()));
  }

  public @Nullable ImmutableList<RelCollation> collations(Filter rel,
      RelMetadataQuery mq) {
    return mq.collations(rel.getInput());
  }

  public @Nullable ImmutableList<RelCollation> collations(TableModify rel,
      RelMetadataQuery mq) {
    return mq.collations(rel.getInput());
  }

  public @Nullable ImmutableList<RelCollation> collations(TableScan scan,
      RelMetadataQuery mq) {
    final BuiltInMetadata.Collation.Handler handler =
        scan.getTable().unwrap(BuiltInMetadata.Collation.Handler.class);
    if (handler != null) {
      return handler.collations(scan, mq);
    }
    return copyOf(table(scan.getTable()));
  }

  public @Nullable ImmutableList<RelCollation> collations(EnumerableMergeJoin join,
      RelMetadataQuery mq) {
    // In general a join is not sorted. But a merge join preserves the sort
    // order of the left and right sides.
    return copyOf(
        RelMdCollation.mergeJoin(mq, join.getLeft(), join.getRight(),
            join.analyzeCondition().leftKeys, join.analyzeCondition().rightKeys,
            join.getJoinType()));
  }

  public @Nullable ImmutableList<RelCollation> collations(EnumerableHashJoin join,
      RelMetadataQuery mq) {
    return copyOf(
        RelMdCollation.enumerableHashJoin(mq, join.getLeft(), join.getRight(), join.getJoinType()));
  }

  public @Nullable ImmutableList<RelCollation> collations(EnumerableNestedLoopJoin join,
      RelMetadataQuery mq) {
    return copyOf(
        RelMdCollation.enumerableNestedLoopJoin(mq, join.getLeft(), join.getRight(),
            join.getJoinType()));
  }

  public @Nullable ImmutableList<RelCollation> collations(EnumerableMergeUnion mergeUnion,
      RelMetadataQuery mq) {
    final RelCollation collation = mergeUnion.getTraitSet().getCollation();
    if (collation == null) {
      // should not happen
      return null;
    }
    // MergeUnion guarantees order, like a sort
    return copyOf(RelMdCollation.sort(collation));
  }

  public @Nullable ImmutableList<RelCollation> collations(EnumerableCorrelate join,
      RelMetadataQuery mq) {
    return copyOf(
        RelMdCollation.enumerableCorrelate(mq, join.getLeft(), join.getRight(),
            join.getJoinType()));
  }

  public @Nullable ImmutableList<RelCollation> collations(Sort sort,
      RelMetadataQuery mq) {
    return copyOf(
        RelMdCollation.sort(sort.getCollation()));
  }

  public @Nullable ImmutableList<RelCollation> collations(SortExchange sort,
      RelMetadataQuery mq) {
    return copyOf(
        RelMdCollation.sort(sort.getCollation()));
  }

  public @Nullable ImmutableList<RelCollation> collations(Project project,
      RelMetadataQuery mq) {
    return copyOf(
        project(mq, project.getInput(), project.getProjects()));
  }

  public @Nullable ImmutableList<RelCollation> collations(Calc calc,
      RelMetadataQuery mq) {
    return copyOf(calc(mq, calc.getInput(), calc.getProgram()));
  }

  public @Nullable ImmutableList<RelCollation> collations(Values values,
      RelMetadataQuery mq) {
    return copyOf(
        values(mq, values.getRowType(), values.getTuples()));
  }

  public @Nullable ImmutableList<RelCollation> collations(JdbcToEnumerableConverter rel,
      RelMetadataQuery mq) {
    return mq.collations(rel.getInput());
  }

  public @Nullable ImmutableList<RelCollation> collations(HepRelVertex rel,
      RelMetadataQuery mq) {
    return mq.collations(rel.getCurrentRel());
  }

  public @Nullable ImmutableList<RelCollation> collations(RelSubset rel,
      RelMetadataQuery mq) {
    return copyOf(
        Objects.requireNonNull(
            rel.getTraitSet().getTraits(RelCollationTraitDef.INSTANCE)));
  }

  // Helper methods

  /** Helper method to determine a
   * {@link org.apache.calcite.rel.core.TableScan}'s collation. */
  public static @Nullable List<RelCollation> table(RelOptTable table) {
    return table.getCollationList();
  }

  /** Helper method to determine a
   * {@link org.apache.calcite.rel.core.Snapshot}'s collation. */
  public static @Nullable List<RelCollation> snapshot(RelMetadataQuery mq, RelNode input) {
    return mq.collations(input);
  }

  /** Helper method to determine a
   * {@link org.apache.calcite.rel.core.Sort}'s collation. */
  public static List<RelCollation> sort(RelCollation collation) {
    return ImmutableList.of(collation);
  }

  /** Helper method to determine a
   * {@link org.apache.calcite.rel.core.Filter}'s collation. */
  public static @Nullable List<RelCollation> filter(RelMetadataQuery mq, RelNode input) {
    return mq.collations(input);
  }

  /** Helper method to determine a
   * limit's collation. */
  public static @Nullable List<RelCollation> limit(RelMetadataQuery mq, RelNode input) {
    return mq.collations(input);
  }

  /** Helper method to determine a
   * {@link org.apache.calcite.rel.core.Calc}'s collation. */
  public static @Nullable List<RelCollation> calc(RelMetadataQuery mq, RelNode input,
      RexProgram program) {
    final List<RexNode> projects =
        program
            .getProjectList()
            .stream()
            .map(program::expandLocalRef)
            .collect(Collectors.toList());
    return project(mq, input, projects);
  }

  /** Helper method to determine a {@link Project}'s collation. */
  public static @Nullable List<RelCollation> project(RelMetadataQuery mq,
      RelNode input, List<? extends RexNode> projects) {
    final NavigableSet<RelCollation> collations = new TreeSet<>();
    final List<RelCollation> inputCollations = mq.collations(input);
    if (inputCollations == null || inputCollations.isEmpty()) {
      return ImmutableList.of();
    }
    final Multimap<Integer, Integer> targets = LinkedListMultimap.create();
    final Map<Integer, SqlMonotonicity> targetsWithMonotonicity =
        new HashMap<>();
    for (Ord<RexNode> project : Ord.<RexNode>zip(projects)) {
      if (project.e instanceof RexInputRef) {
        targets.put(((RexInputRef) project.e).getIndex(), project.i);
      } else if (project.e instanceof RexCall) {
        final RexCall call = (RexCall) project.e;
        final RexCallBinding binding =
            RexCallBinding.create(input.getCluster().getTypeFactory(), call, inputCollations);
        targetsWithMonotonicity.put(project.i, call.getOperator().getMonotonicity(binding));
      }
    }
    final List<RelFieldCollation> fieldCollations = new ArrayList<>();
  loop:
    for (RelCollation ic : inputCollations) {
      if (ic.getFieldCollations().isEmpty()) {
        continue;
      }
      fieldCollations.clear();
      for (RelFieldCollation ifc : ic.getFieldCollations()) {
        final Collection<Integer> integers = targets.get(ifc.getFieldIndex());
        if (integers.isEmpty()) {
          continue loop; // cannot do this collation
        }
        fieldCollations.add(ifc.withFieldIndex(integers.iterator().next()));
      }
      assert !fieldCollations.isEmpty();
      collations.add(RelCollations.of(fieldCollations));
    }

    final List<RelFieldCollation> fieldCollationsForRexCalls =
        new ArrayList<>();
    for (Map.Entry<Integer, SqlMonotonicity> entry
        : targetsWithMonotonicity.entrySet()) {
      final SqlMonotonicity value = entry.getValue();
      switch (value) {
      case NOT_MONOTONIC:
      case CONSTANT:
        break;
      default:
        fieldCollationsForRexCalls.add(
            new RelFieldCollation(entry.getKey(),
                RelFieldCollation.Direction.of(value)));
        break;
      }
    }

    if (!fieldCollationsForRexCalls.isEmpty()) {
      collations.add(RelCollations.of(fieldCollationsForRexCalls));
    }

    return copyOf(collations);
  }

  /** Helper method to determine a
   * {@link org.apache.calcite.rel.core.Window}'s collation.
   *
   * <p>A Window projects the fields of its input first, followed by the output
   * from each of its windows. Assuming (quite reasonably) that the
   * implementation does not re-order its input rows, then any collations of its
   * input are preserved. */
  public static @Nullable List<RelCollation> window(RelMetadataQuery mq, RelNode input,
      ImmutableList<Window.Group> groups) {
    return mq.collations(input);
  }

  /** Helper method to determine a
   * {@link org.apache.calcite.rel.core.Match}'s collation. */
  public static @Nullable List<RelCollation> match(RelMetadataQuery mq, RelNode input,
       RelDataType rowType, RexNode pattern,
       boolean strictStart, boolean strictEnd,
       Map<String, RexNode> patternDefinitions, Map<String, RexNode> measures,
       RexNode after, Map<String, ? extends SortedSet<String>> subsets,
       boolean allRows, ImmutableBitSet partitionKeys, RelCollation orderKeys,
       @Nullable RexNode interval) {
    return mq.collations(input);
  }

  /** Helper method to determine a
   * {@link org.apache.calcite.rel.core.Values}'s collation.
   *
   * <p>We actually under-report the collations. A Values with 0 or 1 rows - an
   * edge case, but legitimate and very common - is ordered by every permutation
   * of every subset of the columns.
   *
   * <p>So, our algorithm aims to:<ul>
   *   <li>produce at most N collations (where N is the number of columns);
   *   <li>make each collation as long as possible;
   *   <li>do not repeat combinations already emitted -
   *       if we've emitted {@code (a, b)} do not later emit {@code (b, a)};
   *   <li>probe the actual values and make sure that each collation is
   *      consistent with the data
   * </ul>
   *
   * <p>So, for an empty Values with 4 columns, we would emit
   * {@code (a, b, c, d), (b, c, d), (c, d), (d)}. */
  public static List<RelCollation> values(RelMetadataQuery mq,
      RelDataType rowType, ImmutableList<ImmutableList<RexLiteral>> tuples) {
    Util.discard(mq); // for future use
    final List<RelCollation> list = new ArrayList<>();
    final int n = rowType.getFieldCount();
    final List<Pair<RelFieldCollation, Ordering<List<RexLiteral>>>> pairs =
        new ArrayList<>();
  outer:
    for (int i = 0; i < n; i++) {
      pairs.clear();
      for (int j = i; j < n; j++) {
        final RelFieldCollation fieldCollation = new RelFieldCollation(j);
        Ordering<List<RexLiteral>> comparator = comparator(fieldCollation);
        Ordering<List<RexLiteral>> ordering;
        if (pairs.isEmpty()) {
          ordering = comparator;
        } else {
          ordering = Util.last(pairs).right.compound(comparator);
        }
        pairs.add(Pair.of(fieldCollation, ordering));
        if (!ordering.isOrdered(tuples)) {
          if (j == i) {
            continue outer;
          }
          pairs.remove(pairs.size() - 1);
        }
      }
      if (!pairs.isEmpty()) {
        list.add(RelCollations.of(Pair.left(pairs)));
      }
    }
    return list;
  }

  public static Ordering<List<RexLiteral>> comparator(
      RelFieldCollation fieldCollation) {
    final int nullComparison = fieldCollation.nullDirection.nullComparison;
    final int x = fieldCollation.getFieldIndex();
    switch (fieldCollation.direction) {
    case ASCENDING:
      return new Ordering<List<RexLiteral>>() {
        @Override public int compare(List<RexLiteral> o1, List<RexLiteral> o2) {
          final Comparable c1 = o1.get(x).getValueAs(Comparable.class);
          final Comparable c2 = o2.get(x).getValueAs(Comparable.class);
          return RelFieldCollation.compare(c1, c2, nullComparison);
        }
      };
    default:
      return new Ordering<List<RexLiteral>>() {
        @Override public int compare(List<RexLiteral> o1, List<RexLiteral> o2) {
          final Comparable c1 = o1.get(x).getValueAs(Comparable.class);
          final Comparable c2 = o2.get(x).getValueAs(Comparable.class);
          return RelFieldCollation.compare(c2, c1, -nullComparison);
        }
      };
    }
  }

  /** Helper method to determine a {@link Join}'s collation assuming that it
   * uses a merge-join algorithm.
   *
   * <p>If the inputs are sorted on other keys <em>in addition to</em> the join
   * key, the result preserves those collations too.
   * @deprecated Use {@link #mergeJoin(RelMetadataQuery, RelNode, RelNode, ImmutableIntList, ImmutableIntList, JoinRelType)} */
  @Deprecated // to be removed before 2.0
  public static @Nullable List<RelCollation> mergeJoin(RelMetadataQuery mq,
      RelNode left, RelNode right,
      ImmutableIntList leftKeys, ImmutableIntList rightKeys) {
    return mergeJoin(mq, left, right, leftKeys, rightKeys, JoinRelType.INNER);
  }

  /** Helper method to determine a {@link Join}'s collation assuming that it
   * uses a merge-join algorithm.
   *
   * <p>If the inputs are sorted on other keys <em>in addition to</em> the join
   * key, the result preserves those collations too. */
  public static @Nullable List<RelCollation> mergeJoin(RelMetadataQuery mq,
      RelNode left, RelNode right,
      ImmutableIntList leftKeys, ImmutableIntList rightKeys, JoinRelType joinType) {
    assert EnumerableMergeJoin.isMergeJoinSupported(joinType)
        : "EnumerableMergeJoin unsupported for join type " + joinType;

    final ImmutableList<RelCollation> leftCollations = mq.collations(left);
    if (!joinType.projectsRight()) {
      return leftCollations;
    }
    if (leftCollations == null) {
      return null;
    }

    final ImmutableList<RelCollation> rightCollations = mq.collations(right);
    if (rightCollations == null) {
      return leftCollations;
    }

    final ImmutableList.Builder<RelCollation> builder = ImmutableList.builder();
    builder.addAll(leftCollations);

    final int leftFieldCount = left.getRowType().getFieldCount();
    for (RelCollation collation : rightCollations) {
      builder.add(RelCollations.shift(collation, leftFieldCount));
    }
    return builder.build();
  }

  /**
   * Returns the collation of {@link EnumerableHashJoin} based on its inputs and the join type.
   */
  public static @Nullable List<RelCollation> enumerableHashJoin(RelMetadataQuery mq,
      RelNode left, RelNode right, JoinRelType joinType) {
    if (joinType == JoinRelType.SEMI) {
      return enumerableSemiJoin(mq, left, right);
    } else {
      return enumerableJoin0(mq, left, right, joinType);
    }
  }

  /**
   * Returns the collation of {@link EnumerableNestedLoopJoin}
   * based on its inputs and the join type.
   */
  public static @Nullable List<RelCollation> enumerableNestedLoopJoin(RelMetadataQuery mq,
      RelNode left, RelNode right, JoinRelType joinType) {
    return enumerableJoin0(mq, left, right, joinType);
  }

  public static @Nullable List<RelCollation> enumerableCorrelate(RelMetadataQuery mq,
      RelNode left, RelNode right, JoinRelType joinType) {
    // The current implementation always preserve the sort order of the left input
    return mq.collations(left);
  }

  public static @Nullable List<RelCollation> enumerableSemiJoin(RelMetadataQuery mq,
      RelNode left, RelNode right) {
    // The current implementation always preserve the sort order of the left input
    return mq.collations(left);
  }

  @SuppressWarnings("unused")
  public static @Nullable List<RelCollation> enumerableBatchNestedLoopJoin(RelMetadataQuery mq,
      RelNode left, RelNode right, JoinRelType joinType) {
    // The current implementation always preserve the sort order of the left input
    return mq.collations(left);
  }

  @SuppressWarnings("unused")
  private static @Nullable List<RelCollation> enumerableJoin0(RelMetadataQuery mq,
      RelNode left, RelNode right, JoinRelType joinType) {
    // The current implementation can preserve the sort order of the left input if one of the
    // following conditions hold:
    // (i) join type is INNER or LEFT;
    // (ii) RelCollation always orders nulls last.
    final ImmutableList<RelCollation> leftCollations = mq.collations(left);
    if (leftCollations == null) {
      return null;
    }
    switch (joinType) {
    case SEMI:
    case ANTI:
    case INNER:
    case LEFT:
      return leftCollations;
    case RIGHT:
    case FULL:
      for (RelCollation collation : leftCollations) {
        for (RelFieldCollation field : collation.getFieldCollations()) {
          if (!(RelFieldCollation.NullDirection.LAST == field.nullDirection)) {
            return null;
          }
        }
      }
      return leftCollations;
    default:
      break;
    }
    return null;
  }
}
