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
 * <a href="https://dl.acm.org/doi/pdf/10.1145/2463676.2465314">
 *   On the correct and complete enumeration of the core search space</a>.
 */
public class ConflictDetectionHelper {

  private ConflictDetectionHelper() {
  }

  /**
   * Tables in the paper involve 7 types of join: cross product, inner join, semi join, anti join,
   * left join, full join, group join. Cross product and inner join have the same result in the
   * associative/left_asscom/right_asscom tables. The inner join with true condition is equivalent
   * to the cross product, and there is no group join in Calcite, so cross product and group join
   * do not appear in the following table.
   */
  private static final ImmutableMap<JoinRelType, Integer> INDEX_OF_TABLE =
      ImmutableMap.<JoinRelType, Integer>builder()
          .put(JoinRelType.INNER, 0).put(JoinRelType.SEMI, 1).put(JoinRelType.ANTI, 2)
          .put(JoinRelType.LEFT, 3).put(JoinRelType.FULL, 4).build();

  /**
   * For {@code e1 joinA e2 joinB e3}, whether joinA and joinB are associative is shown in the
   * following table. In particular, for left/full-A and left-B, we assume that the predicates of
   * left-B are not null-rejecting on e2; for full-A and full-B, we assume that the predicates of
   * full-A and full-B are not null-rejecting on e2. See table.2 in paper.
   */
  private static final boolean[][] ASSOCIATIVE_TABLE = {
      //            inner-B  semi-B  anti-B  left-B  full-B
      /* inner-A */ {true,   true,   true,   true,   false},
      /* semi-A  */ {false,  false,  false,  false,  false},
      /* anti-A  */ {false,  false,  false,  false,  false},
      /* left-A  */ {false,  false,  false,  false,  false},
      /* full-A  */ {false,  false,  false,  false,  false}};

  /**
   * For {@code e1 joinA e2 joinB e3}, whether joinA and joinB are left asscom is shown in the
   * following table. In particular, for left-A and full-B, we assume that the predicates of left-A
   * are not null-rejecting on e1; for full-A and left-B, we assume that the predicates of left-B
   * are not null-rejecting on e3; for full-A and full-B, we assume that the predicates of full-A
   * and full-B are not null-rejecting on e1. See table.3 in paper.
   */
  private static final boolean[][] LEFT_ASSCOM_TABLE = {
      //            inner-B  semi-B  anti-B  left-B  full-B
      /* inner-A */ {true,   true,   true,   true,   false},
      /* semi-A  */ {true,   true,   true,   true,   false},
      /* anti-A  */ {true,   true,   true,   true,   false},
      /* left-A  */ {true,   true,   true,   true,   false},
      /* full-A  */ {false,  false,  false,  false,  false}};

  /**
   * For {@code e1 joinA e2 joinB e3}, whether joinA and joinB are right asscom is shown in the
   * following table. In particular, for full-A and full-B, we assume that the predicates of full-A
   * and full-B are not null-rejecting on e3. See table.3 in paper.
   */
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
   * For conflict rule T1 → T2, if T1 and tes have intersection, we can add T2
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
