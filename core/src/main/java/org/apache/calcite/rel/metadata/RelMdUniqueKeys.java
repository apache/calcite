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
import org.apache.calcite.rel.RelNode;
import org.apache.calcite.rel.SingleRel;
import org.apache.calcite.rel.core.Aggregate;
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
import org.apache.calcite.rex.RexInputRef;
import org.apache.calcite.rex.RexNode;
import org.apache.calcite.rex.RexProgram;
import org.apache.calcite.util.ImmutableBitSet;
import org.apache.calcite.util.Util;

import com.google.common.collect.ImmutableMultimap;
import com.google.common.collect.ImmutableSet;
import com.google.common.collect.Maps;

import org.checkerframework.checker.nullness.qual.Nullable;

import java.util.HashSet;
import java.util.List;
import java.util.Map;
import java.util.Set;
import java.util.stream.Collectors;

import static java.util.Objects.requireNonNull;

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
    return mq.getUniqueKeys(rel.getInput(), ignoreNulls);
  }

  public @Nullable Set<ImmutableBitSet> getUniqueKeys(Sort rel, RelMetadataQuery mq,
      boolean ignoreNulls) {
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

    Map<Integer, ImmutableBitSet> mapInToOutPos =
        Maps.transformValues(inToOutPosBuilder.build().asMap(), ImmutableBitSet::of);

    ImmutableSet.Builder<ImmutableBitSet> resultBuilder = ImmutableSet.builder();
    // Now add to the projUniqueKeySet the child keys that are fully
    // projected.
    for (ImmutableBitSet colMask : childUniqueKeySet) {
      if (!inColumnsUsed.contains(colMask)) {
        // colMask contains a column that is not projected as RexInput => the key is not unique
        continue;
      }
      // colMask is mapped to output project, however, the column can be mapped more than once:
      // select id, id, id, unique2, unique2
      // the resulting unique keys would be {{0},{3}}, {{0},{4}}, {{0},{1},{4}}, ...

      Iterable<List<ImmutableBitSet>> product = Linq4j.product(
          Util.transform(colMask,
              in -> Util.filter(
                  requireNonNull(mapInToOutPos.get(in),
                      () -> "no entry for column " + in + " in mapInToOutPos: " + mapInToOutPos)
                      .powerSet(), bs -> !bs.isEmpty())));

      resultBuilder.addAll(Util.transform(product, ImmutableBitSet::union));
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
      retSet.addAll(leftSet);
    }

    // same as above except left and right are reversed
    if ((leftUnique != null)
        && leftUnique
        && (rightSet != null)
        && !rel.getJoinType().generatesNullsOnRight()) {
      retSet.addAll(rightSet);
    }

    return retSet;
  }

  public Set<ImmutableBitSet> getUniqueKeys(Aggregate rel, RelMetadataQuery mq,
      boolean ignoreNulls) {
    if (Aggregate.isSimple(rel)) {
      final ImmutableBitSet groupKeys = rel.getGroupSet();
      final Set<ImmutableBitSet> inputUniqueKeys = mq
          .getUniqueKeys(rel.getInput(), ignoreNulls);
      if (inputUniqueKeys == null) {
        return ImmutableSet.of(groupKeys);
      }

      // Try to find more precise unique keys.
      final Set<ImmutableBitSet> preciseUniqueKeys = inputUniqueKeys.stream()
          .filter(groupKeys::contains).collect(Collectors.toSet());
      return preciseUniqueKeys.isEmpty() ? ImmutableSet.of(groupKeys) : preciseUniqueKeys;
    } else if (ignoreNulls) {
      // group by keys form a unique key
      return ImmutableSet.of(rel.getGroupSet());
    } else {
      // If the aggregate has grouping sets, all group by keys might be null which means group by
      // keys do not form a unique key.
      return ImmutableSet.of();
    }
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

  // Catch-all rule when none of the others apply.
  public @Nullable Set<ImmutableBitSet> getUniqueKeys(RelNode rel, RelMetadataQuery mq,
      boolean ignoreNulls) {
    // no information available
    return null;
  }
}
