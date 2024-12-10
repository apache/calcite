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

import org.apache.calcite.linq4j.Linq4j;
import org.apache.calcite.plan.RelOptPredicateList;
import org.apache.calcite.rel.RelNode;
import org.apache.calcite.rel.SingleRel;
import org.apache.calcite.rel.core.Aggregate;
import org.apache.calcite.rel.core.AggregateCall;
import org.apache.calcite.rel.core.Calc;
import org.apache.calcite.rel.core.Correlate;
import org.apache.calcite.rel.core.Filter;
import org.apache.calcite.rel.core.Intersect;
import org.apache.calcite.rel.core.Join;
import org.apache.calcite.rel.core.JoinInfo;
import org.apache.calcite.rel.core.Minus;
import org.apache.calcite.rel.core.Project;
import org.apache.calcite.rel.core.Sort;
import org.apache.calcite.rel.core.TableModify;
import org.apache.calcite.rel.core.TableScan;
import org.apache.calcite.rel.core.Union;
import org.apache.calcite.rel.core.Values;
import org.apache.calcite.rex.RexInputRef;
import org.apache.calcite.rex.RexLiteral;
import org.apache.calcite.rex.RexNode;
import org.apache.calcite.rex.RexProgram;
import org.apache.calcite.util.ImmutableBitSet;
import org.apache.calcite.util.ImmutableIntList;
import org.apache.calcite.util.Util;

import com.google.common.collect.ImmutableList;
import com.google.common.collect.ImmutableMultimap;
import com.google.common.collect.ImmutableSet;
import com.google.common.collect.Multimap;
import com.google.common.collect.Sets;

import org.checkerframework.checker.nullness.qual.Nullable;

import java.util.ArrayList;
import java.util.HashSet;
import java.util.List;
import java.util.Set;
import java.util.stream.Collectors;

import static com.google.common.base.Preconditions.checkArgument;

import static org.apache.calcite.rel.metadata.RelMdColumnUniqueness.PASSTHROUGH_AGGREGATIONS;
import static org.apache.calcite.rel.metadata.RelMdColumnUniqueness.getConstantColumnSet;

/**
 * RelMdUniqueKeys supplies a default implementation of
 * {@link RelMetadataQuery#getUniqueKeys} for the standard logical algebra.
 */
public class RelMdUniqueKeys
    implements MetadataHandler<BuiltInMetadata.UniqueKeys> {
  public static final RelMetadataProvider SOURCE =
      ReflectiveRelMetadataProvider.reflectiveSource(
          new RelMdUniqueKeys(), BuiltInMetadata.UniqueKeys.Handler.class);

  //~ Constructors -----------------------------------------------------------

  private RelMdUniqueKeys() {}

  //~ Methods ----------------------------------------------------------------

  @Override public MetadataDef<BuiltInMetadata.UniqueKeys> getDef() {
    return BuiltInMetadata.UniqueKeys.DEF;
  }

  public @Nullable Set<ImmutableBitSet> getUniqueKeys(Filter rel, RelMetadataQuery mq,
      boolean ignoreNulls) {
    Set<ImmutableBitSet> uniqueKeys = mq.getUniqueKeys(rel.getInput(), ignoreNulls);
    if (uniqueKeys == null) {
      return null;
    }
    // Remove constant columns from each key
    RelOptPredicateList predicates = mq.getPulledUpPredicates(rel);
    if (RelOptPredicateList.isEmpty(predicates)) {
      return uniqueKeys;
    } else {
      ImmutableBitSet constantColumns = getConstantColumnSet(predicates);
      return uniqueKeys.stream()
          .map(key -> key.except(constantColumns))
          .collect(Collectors.toSet());
    }
  }

  public @Nullable Set<ImmutableBitSet> getUniqueKeys(Sort rel, RelMetadataQuery mq,
      boolean ignoreNulls) {
    Double maxRowCount = mq.getMaxRowCount(rel);
    if (maxRowCount != null && maxRowCount <= 1.0d) {
      return ImmutableSet.of(ImmutableBitSet.of());
    }
    return mq.getUniqueKeys(rel.getInput(), ignoreNulls);
  }

  public @Nullable Set<ImmutableBitSet> getUniqueKeys(Correlate rel, RelMetadataQuery mq,
      boolean ignoreNulls) {
    return mq.getUniqueKeys(rel.getLeft(), ignoreNulls);
  }

  public @Nullable Set<ImmutableBitSet> getUniqueKeys(TableModify rel, RelMetadataQuery mq,
      boolean ignoreNulls) {
    return mq.getUniqueKeys(rel.getInput(), ignoreNulls);
  }

  public Set<ImmutableBitSet> getUniqueKeys(Project rel, RelMetadataQuery mq,
      boolean ignoreNulls) {
    return getProjectUniqueKeys(rel, mq, ignoreNulls, rel.getProjects());
  }

  public @Nullable Set<ImmutableBitSet> getUniqueKeys(Calc rel, RelMetadataQuery mq,
      boolean ignoreNulls) {
    RexProgram program = rel.getProgram();
    return getProjectUniqueKeys(rel, mq, ignoreNulls,
        Util.transform(program.getProjectList(), program::expandLocalRef));
  }

  private static Set<ImmutableBitSet> getProjectUniqueKeys(SingleRel rel, RelMetadataQuery mq,
      boolean ignoreNulls, List<RexNode> projExprs) {
    // LogicalProject maps a set of rows to a different set;
    // Without knowledge of the mapping function(whether it
    // preserves uniqueness), it is only safe to derive uniqueness
    // info from the child of a project when the mapping is f(a) => a.
    //
    // Further more, the unique bitset coming from the child needs
    // to be mapped to match the output of the project.

    // Single input can be mapped to multiple outputs
    ImmutableMultimap.Builder<Integer, Integer> inToOutPosBuilder = ImmutableMultimap.builder();
    ImmutableBitSet.Builder mappedInColumnsBuilder = ImmutableBitSet.builder();

    // Build an input to output position map.
    for (int i = 0; i < projExprs.size(); i++) {
      RexNode projExpr = projExprs.get(i);
      if (projExpr instanceof RexInputRef) {
        int inputIndex = ((RexInputRef) projExpr).getIndex();
        inToOutPosBuilder.put(inputIndex, i);
        mappedInColumnsBuilder.set(inputIndex);
      }
    }
    ImmutableBitSet inColumnsUsed = mappedInColumnsBuilder.build();

    if (inColumnsUsed.isEmpty()) {
      // if there's no RexInputRef in the projected expressions
      // return empty set.
      return ImmutableSet.of();
    }

    Set<ImmutableBitSet> childUniqueKeySet =
        mq.getUniqueKeys(rel.getInput(), ignoreNulls);

    if (childUniqueKeySet == null) {
      return ImmutableSet.of();
    }

    Multimap<Integer, Integer> mapInToOutPos = inToOutPosBuilder.build();

    ImmutableSet.Builder<ImmutableBitSet> resultBuilder = ImmutableSet.builder();
    // Now add to the projUniqueKeySet the child keys that are fully
    // projected.
    for (ImmutableBitSet colMask : childUniqueKeySet) {
      if (!inColumnsUsed.contains(colMask)) {
        // colMask contains a column that is not projected as RexInput => the key is not unique
        continue;
      }
      // colMask is mapped to output project, however, the column can be mapped more than once:
      // select key1, key1, val1, val2, key2 from ...
      // the resulting unique keys would be {{0},{4}}, {{1},{4}}

      Iterable<List<Integer>> product = Linq4j.product(Util.transform(colMask, mapInToOutPos::get));

      resultBuilder.addAll(Util.transform(product, ImmutableBitSet::of));
    }
    return resultBuilder.build();
  }

  public @Nullable Set<ImmutableBitSet> getUniqueKeys(Join rel, RelMetadataQuery mq,
      boolean ignoreNulls) {
    if (!rel.getJoinType().projectsRight()) {
      // only return the unique keys from the LHS since a semijoin only
      // returns the LHS
      return mq.getUniqueKeys(rel.getLeft(), ignoreNulls);
    }

    final RelNode left = rel.getLeft();
    final RelNode right = rel.getRight();

    // first add the different combinations of concatenated unique keys
    // from the left and the right, adjusting the right hand side keys to
    // reflect the addition of the left hand side
    //
    // NOTE zfong 12/18/06 - If the number of tables in a join is large,
    // the number of combinations of unique key sets will explode.  If
    // that is undesirable, use RelMetadataQuery.areColumnsUnique() as
    // an alternative way of getting unique key information.

    final Set<ImmutableBitSet> retSet = new HashSet<>();
    final Set<ImmutableBitSet> leftSet = mq.getUniqueKeys(left, ignoreNulls);
    Set<ImmutableBitSet> rightSet = null;

    final Set<ImmutableBitSet> tmpRightSet = mq.getUniqueKeys(right, ignoreNulls);
    int nFieldsOnLeft = left.getRowType().getFieldCount();

    if (tmpRightSet != null) {
      rightSet = new HashSet<>();
      for (ImmutableBitSet colMask : tmpRightSet) {
        ImmutableBitSet.Builder tmpMask = ImmutableBitSet.builder();
        for (int bit : colMask) {
          tmpMask.set(bit + nFieldsOnLeft);
        }
        rightSet.add(tmpMask.build());
      }

      if (leftSet != null) {
        for (ImmutableBitSet colMaskRight : rightSet) {
          for (ImmutableBitSet colMaskLeft : leftSet) {
            retSet.add(colMaskLeft.union(colMaskRight));
          }
        }
      }
    }

    // locate the columns that participate in equijoins
    final JoinInfo joinInfo = rel.analyzeCondition();

    // determine if either or both the LHS and RHS are unique on the
    // equijoin columns
    final Boolean leftUnique =
        mq.areColumnsUnique(left, joinInfo.leftSet(), ignoreNulls);
    final Boolean rightUnique =
        mq.areColumnsUnique(right, joinInfo.rightSet(), ignoreNulls);

    // if the right hand side is unique on its equijoin columns, then we can
    // add the unique keys from left if the left hand side is not null
    // generating
    if ((rightUnique != null)
        && rightUnique
        && (leftSet != null)
        && !rel.getJoinType().generatesNullsOnLeft()) {
      ImmutableBitSet leftConstants =
          getConstantJoinKeys(joinInfo.leftKeys, joinInfo.rightKeys, right, mq);
      leftSet.stream()
          .map(set -> set.except(leftConstants))
          .forEach(retSet::add);
    }

    // same as above except left and right are reversed
    if ((leftUnique != null)
        && leftUnique
        && (rightSet != null)
        && !rel.getJoinType().generatesNullsOnRight()) {
      ImmutableBitSet rightConstants =
          getConstantJoinKeys(joinInfo.rightKeys, joinInfo.leftKeys, left, mq);
      rightSet.stream()
          .map(set -> set.except(rightConstants))
          .forEach(retSet::add);
    }

    // Remove sets that are supersets of other sets
    final Set<ImmutableBitSet> reducedSet = new HashSet<>();
    for (ImmutableBitSet bigger : retSet) {
      if (retSet.stream()
          .filter(smaller -> !bigger.equals(smaller))
          .noneMatch(bigger::contains)) {
        reducedSet.add(bigger);
      }
    }

    return reducedSet;
  }

  /**
   * Return the keys that are constant by virtue of equality with a constant
   * (literal or scalar query result) on the other side of a join.
   */
  private static ImmutableBitSet getConstantJoinKeys(ImmutableIntList keys,
      ImmutableIntList otherKeys, RelNode otherRel, RelMetadataQuery mq) {
    Double maxRowCount = mq.getMaxRowCount(otherRel);
    ImmutableBitSet otherConstants;
    if (maxRowCount != null && maxRowCount <= 1.0d) {
      // In a single row solution, every column is constant
      int size = otherRel.getRowType().getFieldList().size();
      otherConstants = ImmutableBitSet.range(size);
    } else {
      otherConstants = getConstantColumnSet(mq.getPulledUpPredicates(otherRel));
    }
    ImmutableBitSet.Builder builder = ImmutableBitSet.builder();
    for (int i = 0; i < keys.size(); i++) {
      if (otherConstants.get(otherKeys.get(i))) {
        builder.set(keys.get(i));
      }
    }
    return builder.build();
  }

  public Set<ImmutableBitSet> getUniqueKeys(Aggregate rel, RelMetadataQuery mq,
      boolean ignoreNulls) {
    if (Aggregate.isSimple(rel)) {
      final ImmutableBitSet groupKeys = rel.getGroupSet();
      RelOptPredicateList pulledUpPredicates = mq.getPulledUpPredicates(rel);
      ImmutableBitSet reducedGroupKeys =
          groupKeys.except(getConstantColumnSet(pulledUpPredicates));

      final Set<ImmutableBitSet> preciseUniqueKeys;
      final Set<ImmutableBitSet> inputUniqueKeys =
          mq.getUniqueKeys(rel.getInput(), ignoreNulls);
      if (inputUniqueKeys == null) {
        preciseUniqueKeys = ImmutableSet.of(reducedGroupKeys);
      } else {
        // Try to find more precise unique keys.
        final Set<ImmutableBitSet> keysInGroupBy = inputUniqueKeys.stream()
            .filter(reducedGroupKeys::contains).collect(Collectors.toSet());
        preciseUniqueKeys = keysInGroupBy.isEmpty()
            ? ImmutableSet.of(reducedGroupKeys)
            : keysInGroupBy;
      }

      // If an input's unique column(s) value is returned (passed through) by an aggregation
      // function, then the result of the function(s) is also unique.
      final ImmutableSet.Builder<ImmutableBitSet> keysBuilder = ImmutableSet.builder();
      if (inputUniqueKeys != null) {
        for (ImmutableBitSet inputKey : inputUniqueKeys) {
          Iterable<List<Integer>> product =
              Linq4j.product(Util.transform(inputKey, i -> getPassedThroughCols(i, rel)));
          keysBuilder.addAll(Util.transform(product, ImmutableBitSet::of));
        }
      }

      return filterSupersets(Sets.union(preciseUniqueKeys, keysBuilder.build()));
    } else if (ignoreNulls) {
      // group by keys form a unique key
      return ImmutableSet.of(rel.getGroupSet());
    } else {
      // If the aggregate has grouping sets, all group by keys might be null which means group by
      // keys do not form a unique key.
      return ImmutableSet.of();
    }
  }

  /**
   * Returns the subset of the supplied keys that are not a superset of the
   * other keys.  Given {@code {0},{1},{1,2}}, returns {@code {0},{1}}.
   */
  private static Set<ImmutableBitSet> filterSupersets(
      Set<ImmutableBitSet> uniqueKeys) {
    Set<ImmutableBitSet> minimalKeys = new HashSet<>();
    outer:
    for (ImmutableBitSet candidateKey : uniqueKeys) {
      for (ImmutableBitSet possibleSubset : uniqueKeys) {
        if (!candidateKey.equals(possibleSubset)
            && candidateKey.contains(possibleSubset)) {
          continue outer;
        }
      }
      minimalKeys.add(candidateKey);
    }
    return minimalKeys;
  }

  /**
   * Given a column in the input of an Aggregate rel, returns the mappings from the input column to
   * the output of the aggregations. A mapping for the column exists if it is part of a simple
   * group by and/or it is "passed through" unmodified by a
   * {@link RelMdColumnUniqueness#PASSTHROUGH_AGGREGATIONS pass-through aggregation function}.
   */
  private static ImmutableBitSet getPassedThroughCols(Integer inputColumn,
      Aggregate rel) {
    checkArgument(Aggregate.isSimple(rel));
    final ImmutableBitSet.Builder builder = ImmutableBitSet.builder();
    if (rel.getGroupSet().get(inputColumn)) {
      builder.set(inputColumn);
    }
    final int aggCallsOffset = rel.getGroupSet().length();
    for (int i = 0, size = rel.getAggCallList().size(); i < size; i++) {
      AggregateCall call = rel.getAggCallList().get(i);
      if (PASSTHROUGH_AGGREGATIONS.contains(call.getAggregation().getKind())
          && call.getArgList().get(0).equals(inputColumn)) {
        builder.set(i + aggCallsOffset);
      }
    }
    return builder.build();
  }

  public Set<ImmutableBitSet> getUniqueKeys(Union rel, RelMetadataQuery mq,
      boolean ignoreNulls) {
    if (!rel.all) {
      return ImmutableSet.of(
          ImmutableBitSet.range(rel.getRowType().getFieldCount()));
    }
    return ImmutableSet.of();
  }

  /**
   * Any unique key of any input of Intersect is an unique key of the Intersect.
   */
  public Set<ImmutableBitSet> getUniqueKeys(Intersect rel,
      RelMetadataQuery mq, boolean ignoreNulls) {
    ImmutableSet.Builder<ImmutableBitSet> keys = new ImmutableSet.Builder<>();
    for (RelNode input : rel.getInputs()) {
      Set<ImmutableBitSet> uniqueKeys = mq.getUniqueKeys(input, ignoreNulls);
      if (uniqueKeys != null) {
        keys.addAll(uniqueKeys);
      }
    }
    ImmutableSet<ImmutableBitSet> uniqueKeys = keys.build();
    if (!uniqueKeys.isEmpty()) {
      return uniqueKeys;
    }

    if (!rel.all) {
      return ImmutableSet.of(
          ImmutableBitSet.range(rel.getRowType().getFieldCount()));
    }
    return ImmutableSet.of();
  }

  /**
   * The unique keys of Minus are precisely the unique keys of its first input.
   */
  public Set<ImmutableBitSet> getUniqueKeys(Minus rel,
      RelMetadataQuery mq, boolean ignoreNulls) {
    Set<ImmutableBitSet> uniqueKeys = mq.getUniqueKeys(rel.getInput(0), ignoreNulls);
    if (uniqueKeys != null) {
      return uniqueKeys;
    }

    if (!rel.all) {
      return ImmutableSet.of(
          ImmutableBitSet.range(rel.getRowType().getFieldCount()));
    }
    return ImmutableSet.of();
  }

  public @Nullable Set<ImmutableBitSet> getUniqueKeys(TableScan rel, RelMetadataQuery mq,
      boolean ignoreNulls) {
    final BuiltInMetadata.UniqueKeys.Handler handler =
        rel.getTable().unwrap(BuiltInMetadata.UniqueKeys.Handler.class);
    if (handler != null) {
      return handler.getUniqueKeys(rel, mq, ignoreNulls);
    }

    final List<ImmutableBitSet> keys = rel.getTable().getKeys();
    if (keys == null) {
      return null;
    }
    for (ImmutableBitSet key : keys) {
      assert rel.getTable().isKey(key);
    }
    return ImmutableSet.copyOf(keys);
  }

  public @Nullable Set<ImmutableBitSet> getUniqueKeys(Values rel, RelMetadataQuery mq,
      boolean ignoreNulls) {
    ImmutableList<ImmutableList<RexLiteral>> tuples = rel.tuples;
    if (tuples.size() <= 1) {
      return ImmutableSet.of(ImmutableBitSet.of());
    }
    // Identify the single-column keys - a subset of all composite keys
    List<Set<RexLiteral>> ranges = new ArrayList<>();
    int rowSize = tuples.get(0).size();
    for (int i = 0; i < rowSize; i++) {
      ranges.add(new HashSet<>());
    }
    for (ImmutableList<RexLiteral> tuple : tuples) {
      for (int i = 0; i < rowSize; i++) {
        ranges.get(i).add(tuple.get(i));
      }
    }

    ImmutableSet.Builder<ImmutableBitSet> keySetBuilder = ImmutableSet.builder();
    for (int i = 0; i < ranges.size(); i++) {
      final Set<RexLiteral> range = ranges.get(i);
      if (range.size() == tuples.size()) {
        keySetBuilder.add(ImmutableBitSet.of(i));
      }
    }
    return keySetBuilder.build();
  }

  // Catch-all rule when none of the others apply.
  public @Nullable Set<ImmutableBitSet> getUniqueKeys(RelNode rel, RelMetadataQuery mq,
      boolean ignoreNulls) {
    // no information available
    return null;
  }
}
