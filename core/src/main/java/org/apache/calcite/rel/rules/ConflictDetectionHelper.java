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

import java.util.ArrayList;
import java.util.List;

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
   * Make conflict rules for join operator based on CD-C. See Figure.11 in paper.
   *
   * @param leftSubEdges  left sub operators
   * @param rightSubEdges right sub operators
   * @param joinType      current join operator
   * @return  conflict rule list
   */
  public static List<ConflictRule> makeConflictRules(
      List<HyperEdge> leftSubEdges,
      List<HyperEdge> rightSubEdges,
      JoinRelType joinType) {
    List<ConflictRule> conflictRules = new ArrayList<>();
    //       o_b
    //      /   \
    //    ...   ...
    //    /
    //  o_a
    //
    // calculate the conflict rules for join_operator_b based on the all join operator
    // (join_operator_a) on the left side of join_operator_b
    for (HyperEdge leftSubEdge : leftSubEdges) {
      if (!isAssociative(leftSubEdge.getJoinType(), joinType)) {
        // if (o_a, o_b) does not satisfy the associative law
        if (leftSubEdge.getLeftNodeUsedInPredicate() != 0) {
          // if the predicate of o_a does not reference the table on its left side,
          // a less restrictive conflict rule will be added
          conflictRules.add(
              new ConflictRule(
                  leftSubEdge.getInitialRightNodeBits(),
                  leftSubEdge.getLeftNodeUsedInPredicate()));
        } else {
          conflictRules.add(
              new ConflictRule(
              leftSubEdge.getInitialRightNodeBits(),
              leftSubEdge.getInitialLeftNodeBits()));
        }
      }
      if (!isLeftAsscom(leftSubEdge.getJoinType(), joinType)) {
        // if (o_a, o_b) does not satisfy the left-asscom law
        if (leftSubEdge.getRightNodeUsedInPredicate() != 0) {
          // if the predicate of o_a does not reference the table on its right side,
          // a less restrictive conflict rule will be added
          conflictRules.add(
              new ConflictRule(
                  leftSubEdge.getInitialLeftNodeBits(),
                  leftSubEdge.getRightNodeUsedInPredicate()));
        } else {
          conflictRules.add(
              new ConflictRule(
                  leftSubEdge.getInitialLeftNodeBits(),
                  leftSubEdge.getInitialRightNodeBits()));
        }
      }
    }

    //       o_b
    //      /   \
    //    ...   ...
    //            \
    //            o_a
    //
    // calculate the conflict rules for join_operator_b based on the all join operator
    // (join_operator_a) on the right side of join_operator_b
    for (HyperEdge rightSubEdge : rightSubEdges) {
      if (!isAssociative(joinType, rightSubEdge.getJoinType())) {
        // if (o_b, o_a) does not satisfy the associative law
        if (rightSubEdge.getRightNodeUsedInPredicate() != 0) {
          // if the predicate of o_a does not reference the table on its right side,
          // a less restrictive conflict rule will be added
          conflictRules.add(
              new ConflictRule(
                  rightSubEdge.getInitialLeftNodeBits(),
                  rightSubEdge.getRightNodeUsedInPredicate()));
        } else {
          conflictRules.add(
              new ConflictRule(
                  rightSubEdge.getInitialLeftNodeBits(),
                  rightSubEdge.getInitialRightNodeBits()));
        }
      }
      if (!isRightAsscom(joinType, rightSubEdge.getJoinType())) {
        // if (o_b, o_a) does not satisfy the right-asscom law
        if (rightSubEdge.getLeftNodeUsedInPredicate() != 0) {
          // if the predicate of o_a does not reference the table on its left side,
          // a less restrictive conflict rule will be added
          conflictRules.add(
              new ConflictRule(
                  rightSubEdge.getInitialRightNodeBits(),
                  rightSubEdge.getLeftNodeUsedInPredicate()));
        } else {
          conflictRules.add(
              new ConflictRule(
                  rightSubEdge.getInitialRightNodeBits(),
                  rightSubEdge.getInitialLeftNodeBits()));
        }
      }
    }
    return conflictRules;
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
