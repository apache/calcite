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
package org.eigenbase.rel.metadata;

import java.util.*;

import org.eigenbase.rel.*;
import org.eigenbase.rel.rules.*;
import org.eigenbase.rex.*;

import net.hydromatic.optiq.BuiltinMethod;
import net.hydromatic.optiq.util.BitSets;

/**
 * RelMdUniqueKeys supplies a default implementation of {@link
 * RelMetadataQuery#getUniqueKeys} for the standard logical algebra.
 */
public class RelMdUniqueKeys {
  public static final RelMetadataProvider SOURCE =
      ReflectiveRelMetadataProvider.reflectiveSource(
          BuiltinMethod.UNIQUE_KEYS.method, new RelMdUniqueKeys());

  //~ Constructors -----------------------------------------------------------

  private RelMdUniqueKeys() {}

  //~ Methods ----------------------------------------------------------------

  public Set<BitSet> getUniqueKeys(FilterRelBase rel, boolean ignoreNulls) {
    return RelMetadataQuery.getUniqueKeys(rel.getChild(), ignoreNulls);
  }

  public Set<BitSet> getUniqueKeys(SortRel rel, boolean ignoreNulls) {
    return RelMetadataQuery.getUniqueKeys(rel.getChild(), ignoreNulls);
  }

  public Set<BitSet> getUniqueKeys(CorrelatorRel rel, boolean ignoreNulls) {
    return RelMetadataQuery.getUniqueKeys(rel.getLeft(), ignoreNulls);
  }

  public Set<BitSet> getUniqueKeys(ProjectRelBase rel, boolean ignoreNulls) {
    // ProjectRel maps a set of rows to a different set;
    // Without knowledge of the mapping function(whether it
    // preserves uniqueness), it is only safe to derive uniqueness
    // info from the child of a project when the mapping is f(a) => a.
    //
    // Further more, the unique bitset coming from the child needs
    // to be mapped to match the output of the project.
    Map<Integer, Integer> mapInToOutPos = new HashMap<Integer, Integer>();

    List<RexNode> projExprs = rel.getProjects();

    Set<BitSet> projUniqueKeySet = new HashSet<BitSet>();

    // Build an input to output position map.
    for (int i = 0; i < projExprs.size(); i++) {
      RexNode projExpr = projExprs.get(i);
      if (projExpr instanceof RexInputRef) {
        mapInToOutPos.put(((RexInputRef) projExpr).getIndex(), i);
      }
    }

    if (mapInToOutPos.isEmpty()) {
      // if there's no RexInputRef in the projected expressions
      // return empty set.
      return projUniqueKeySet;
    }

    Set<BitSet> childUniqueKeySet =
        RelMetadataQuery.getUniqueKeys(rel.getChild(), ignoreNulls);

    if (childUniqueKeySet != null) {
      // Now add to the projUniqueKeySet the child keys that are fully
      // projected.
      for (BitSet colMask : childUniqueKeySet) {
        BitSet tmpMask = new BitSet();
        boolean completeKeyProjected = true;
        for (int bit : BitSets.toIter(colMask)) {
          if (mapInToOutPos.containsKey(bit)) {
            tmpMask.set(mapInToOutPos.get(bit));
          } else {
            // Skip the child unique key if part of it is not
            // projected.
            completeKeyProjected = false;
            break;
          }
        }
        if (completeKeyProjected) {
          projUniqueKeySet.add(tmpMask);
        }
      }
    }

    return projUniqueKeySet;
  }

  public Set<BitSet> getUniqueKeys(JoinRelBase rel, boolean ignoreNulls) {
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

    Set<BitSet> retSet = new HashSet<BitSet>();
    Set<BitSet> leftSet = RelMetadataQuery.getUniqueKeys(left, ignoreNulls);
    Set<BitSet> rightSet = null;

    Set<BitSet> tmpRightSet =
        RelMetadataQuery.getUniqueKeys(right, ignoreNulls);
    int nFieldsOnLeft = left.getRowType().getFieldCount();

    if (tmpRightSet != null) {
      rightSet = new HashSet<BitSet>();
      for (BitSet colMask : tmpRightSet) {
        BitSet tmpMask = new BitSet();
        for (int bit : BitSets.toIter(colMask)) {
          tmpMask.set(bit + nFieldsOnLeft);
        }
        rightSet.add(tmpMask);
      }

      if (leftSet != null) {
        for (BitSet colMaskRight : rightSet) {
          for (BitSet colMaskLeft : leftSet) {
            BitSet colMaskConcat = new BitSet();
            colMaskConcat.or(colMaskLeft);
            colMaskConcat.or(colMaskRight);
            retSet.add(colMaskConcat);
          }
        }
      }
    }

    // locate the columns that participate in equijoins
    final JoinInfo joinInfo = rel.analyzeCondition();

    // determine if either or both the LHS and RHS are unique on the
    // equijoin columns
    Boolean leftUnique =
        RelMetadataQuery.areColumnsUnique(left, joinInfo.leftSet(),
            ignoreNulls);
    Boolean rightUnique =
        RelMetadataQuery.areColumnsUnique(right, joinInfo.rightSet(),
            ignoreNulls);

    // if the right hand side is unique on its equijoin columns, then we can
    // add the unique keys from left if the left hand side is not null
    // generating
    if ((rightUnique != null)
        && rightUnique
        && (leftSet != null)
        && !(rel.getJoinType().generatesNullsOnLeft())) {
      retSet.addAll(leftSet);
    }

    // same as above except left and right are reversed
    if ((leftUnique != null)
        && leftUnique
        && (rightSet != null)
        && !(rel.getJoinType().generatesNullsOnRight())) {
      retSet.addAll(rightSet);
    }

    return retSet;
  }

  public Set<BitSet> getUniqueKeys(SemiJoinRel rel, boolean ignoreNulls) {
    // only return the unique keys from the LHS since a semijoin only
    // returns the LHS
    return RelMetadataQuery.getUniqueKeys(rel.getLeft(), ignoreNulls);
  }

  public Set<BitSet> getUniqueKeys(AggregateRelBase rel, boolean ignoreNulls) {
    Set<BitSet> retSet = new HashSet<BitSet>();

    // group by keys form a unique key
    if (rel.getGroupCount() > 0) {
      BitSet groupKey = new BitSet();
      for (int i = 0; i < rel.getGroupCount(); i++) {
        groupKey.set(i);
      }
      retSet.add(groupKey);
    }
    return retSet;
  }

  // Catch-all rule when none of the others apply.
  public Set<BitSet> getUniqueKeys(RelNode rel, boolean ignoreNulls) {
    // no information available
    return null;
  }
}

// End RelMdUniqueKeys.java
