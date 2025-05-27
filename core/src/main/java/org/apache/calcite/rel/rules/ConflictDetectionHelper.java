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

import org.apache.calcite.rel.core.JoinRelType;

import com.google.common.collect.ImmutableMap;

import java.util.HashMap;
import java.util.List;
import java.util.Map;

/**
 * Conflict detection algorithm based on CD-C. More details are in paper:
 * <a href="https://15721.courses.cs.cmu.edu/spring2019/papers/23-optimizer2/p493-moerkotte.pdf">
 *   On the correct and complete enumeration of the core search space</a>.
 */
public class ConflictDetectionHelper {

  private ConflictDetectionHelper() {
  }

  private static final ImmutableMap<JoinRelType, Integer> INDEX_OF_TABLE =
      ImmutableMap.<JoinRelType, Integer>builder()
          .put(JoinRelType.INNER, 0).put(JoinRelType.SEMI, 1).put(JoinRelType.ANTI, 2)
          .put(JoinRelType.LEFT, 3).put(JoinRelType.FULL, 4).build();

  // TODO: when special attribute is null rejecting, left/full join is
  //  associative/left_asscom/right_asscom. See table2/3 in paper
  private static final boolean[][] ASSOCIATIVE_TABLE = {
      //            inner-B  semi-B  anti-B  left-B  full-B
      /* inner-A */ {true,   true,   true,   true,   false},
      /* semi-A  */ {false,  false,  false,  false,  false},
      /* anti-A  */ {false,  false,  false,  false,  false},
      /* left-A  */ {false,  false,  false,  false,  false},
      /* full-A  */ {false,  false,  false,  false,  false}};

  private static final boolean[][] LEFT_ASSCOM_TABLE = {
      //            inner-B  semi-B  anti-B  left-B  full-B
      /* inner-A */ {true,   true,   true,   true,   false},
      /* semi-A  */ {true,   true,   true,   true,   false},
      /* anti-A  */ {true,   true,   true,   true,   false},
      /* left-A  */ {true,   true,   true,   true,   false},
      /* full-A  */ {false,  false,  false,  false,  false}};

  private static final boolean[][] RIGHT_ASSCOM_TABLE = {
      //            inner-B  semi-B  anti-B  left-B  full-B
      /* inner-A */ {true,   false,  false,  false,  false},
      /* semi-A  */ {false,  false,  false,  false,  false},
      /* anti-A  */ {false,  false,  false,  false,  false},
      /* left-A  */ {false,  false,  false,  false,  false},
      /* full-A  */ {false,  false,  false,  false,  false}};

  /**
   * Make conflict rules for join operator based on CD-C.
   *
   * @param leftSubEdges  left sub operators
   * @param rightSubEdges right sub operators
   * @param joinType      current join operator
   * @return  a map from table set1 to table set2 that if set1 and the tables in the current join
   * operator have an intersection, then set2 must be included in the current join operator
   */
  public static Map<Long, Long> makeConflictRules(
      List<HyperEdge> leftSubEdges,
      List<HyperEdge> rightSubEdges,
      JoinRelType joinType) {
    Map<Long, Long> conflictRules = new HashMap<>();
    for (HyperEdge leftSubEdge : leftSubEdges) {
      if (!isAssociative(leftSubEdge.getJoinType(), joinType)) {
        if (leftSubEdge.getLeftNodeUsedInPredicate() != 0) {
          conflictRules.merge(
              leftSubEdge.getInitialRightNodeBits(),
              leftSubEdge.getLeftNodeUsedInPredicate(),
              (oldValue, newValue) -> oldValue | newValue);
        } else {
          conflictRules.merge(
              leftSubEdge.getInitialRightNodeBits(),
              leftSubEdge.getInitialLeftNodeBits(),
              (oldValue, newValue) -> oldValue | newValue);
        }

      }
      if (!isLeftAsscom(leftSubEdge.getJoinType(), joinType)) {
        if (leftSubEdge.getRightNodeUsedInPredicate() != 0) {
          conflictRules.merge(
              leftSubEdge.getInitialLeftNodeBits(),
              leftSubEdge.getRightNodeUsedInPredicate(),
              (oldValue, newValue) -> oldValue | newValue);
        } else {
          conflictRules.merge(
              leftSubEdge.getInitialLeftNodeBits(),
              leftSubEdge.getInitialRightNodeBits(),
              (oldValue, newValue) -> oldValue | newValue);
        }
      }
    }

    for (HyperEdge rightSubEdge : rightSubEdges) {
      if (!isAssociative(joinType, rightSubEdge.getJoinType())) {
        if (rightSubEdge.getRightNodeUsedInPredicate() != 0) {
          conflictRules.merge(
              rightSubEdge.getInitialLeftNodeBits(),
              rightSubEdge.getRightNodeUsedInPredicate(),
              (oldValue, newValue) -> oldValue | newValue);
        } else {
          conflictRules.merge(
              rightSubEdge.getInitialLeftNodeBits(),
              rightSubEdge.getInitialRightNodeBits(),
              (oldValue, newValue) -> oldValue | newValue);
        }
      }
      if (!isRightAsscom(joinType, rightSubEdge.getJoinType())) {
        if (rightSubEdge.getLeftNodeUsedInPredicate() != 0) {
          conflictRules.merge(
              rightSubEdge.getInitialRightNodeBits(),
              rightSubEdge.getLeftNodeUsedInPredicate(),
              (oldValue, newValue) -> oldValue | newValue);
        } else {
          conflictRules.merge(
              rightSubEdge.getInitialRightNodeBits(),
              rightSubEdge.getInitialLeftNodeBits(),
              (oldValue, newValue) -> oldValue | newValue);
        }
      }
    }
    return conflictRules;
  }

  /**
   * For conflict rule <code>T1 -&gt; T2</code>, if T1 and tes have intersection, we can add T2
   * into tes and remove this rule. See section 5.5 in paper.
   *
   * @param tes                       tes
   * @param conflictRulesAfterAbsorb  conflict rules after absorbing
   * @param initialconflictRules      conflict rules before absorbing
   * @return  tes after absorbing conflict rules
   */
  public static long absorbConflictRulesIntoTES(
      long tes,
      Map<Long, Long> conflictRulesAfterAbsorb,
      Map<Long, Long> initialconflictRules) {
    for (Map.Entry<Long, Long> rule : initialconflictRules.entrySet()) {
      if (LongBitmap.isOverlap(tes, rule.getKey())) {
        tes |= rule.getValue();
        continue;
      }
      conflictRulesAfterAbsorb.put(rule.getKey(), rule.getValue());
    }
    return tes;
  }

  /**
   * Check whether the operator is applicable through the conflict rule.
   *
   * @param subGraph  table set of the current join operator
   * @param edges     hyper edges with conflict rules
   * @return  true if the operator is applicable
   */
  public static boolean applicable(long subGraph, List<HyperEdge> edges) {
    for (HyperEdge edge : edges) {
      for (Map.Entry<Long, Long> rule : edge.getConflictRules().entrySet()) {
        if (LongBitmap.isOverlap(subGraph, rule.getKey())
            && !LongBitmap.isSubSet(rule.getValue(), subGraph)) {
          return false;
        }
      }
    }
    return true;
  }

  public static boolean isCommutative(JoinRelType operator) {
    return operator == JoinRelType.INNER || operator == JoinRelType.FULL;
  }

  public static boolean isAssociative(JoinRelType operatorA, JoinRelType operatorB) {
    Integer indexA = INDEX_OF_TABLE.get(operatorA);
    Integer indexB = INDEX_OF_TABLE.get(operatorB);
    if (indexA == null || indexB == null) {
      return false;
    }
    return ASSOCIATIVE_TABLE[indexA][indexB];
  }

  public static boolean isLeftAsscom(JoinRelType operatorA, JoinRelType operatorB) {
    Integer indexA = INDEX_OF_TABLE.get(operatorA);
    Integer indexB = INDEX_OF_TABLE.get(operatorB);
    if (indexA == null || indexB == null) {
      return false;
    }
    return LEFT_ASSCOM_TABLE[indexA][indexB];
  }

  public static boolean isRightAsscom(JoinRelType operatorA, JoinRelType operatorB) {
    Integer indexA = INDEX_OF_TABLE.get(operatorA);
    Integer indexB = INDEX_OF_TABLE.get(operatorB);
    if (indexA == null || indexB == null) {
      return false;
    }
    return RIGHT_ASSCOM_TABLE[indexA][indexB];
  }

}
