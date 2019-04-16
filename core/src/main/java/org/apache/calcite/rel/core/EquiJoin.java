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
package org.apache.calcite.rel.core;

import org.apache.calcite.plan.RelOptCluster;
import org.apache.calcite.plan.RelTraitSet;
import org.apache.calcite.rel.RelNode;
import org.apache.calcite.rex.RexNode;
import org.apache.calcite.util.ImmutableIntList;

import java.math.BigDecimal;
import java.util.ArrayList;
import java.util.LinkedHashMap;
import java.util.List;
import java.util.Map;
import java.util.Objects;
import java.util.Set;

/**
 * Base class for any join whose condition is based on column equality.
 */
public abstract class EquiJoin extends Join {
  public final ImmutableIntList leftKeys;
  public final ImmutableIntList rightKeys;

  /** Creates an EquiJoin. */
  public EquiJoin(RelOptCluster cluster, RelTraitSet traits, RelNode left,
      RelNode right, RexNode condition, ImmutableIntList leftKeys,
      ImmutableIntList rightKeys, Set<CorrelationId> variablesSet,
      JoinRelType joinType) {
    super(cluster, traits, left, right, condition, variablesSet, joinType);
    this.leftKeys = Objects.requireNonNull(leftKeys);
    this.rightKeys = Objects.requireNonNull(rightKeys);
  }

  @Deprecated // to be removed before 2.0
  public EquiJoin(RelOptCluster cluster, RelTraitSet traits, RelNode left,
      RelNode right, RexNode condition, ImmutableIntList leftKeys,
      ImmutableIntList rightKeys, JoinRelType joinType,
      Set<String> variablesStopped) {
    this(cluster, traits, left, right, condition, leftKeys, rightKeys,
        CorrelationId.setOf(variablesStopped), joinType);
  }

  public ImmutableIntList getLeftKeys() {
    return leftKeys;
  }

  public ImmutableIntList getRightKeys() {
    return rightKeys;
  }

  @Override public JoinInfo analyzeCondition() {
    return JoinInfo.of(leftKeys, rightKeys);
  }


  private static final Map<Class, Integer> CANDIDATE_TYPE_2_LEVEL = new
      LinkedHashMap<>();

  static {
    CANDIDATE_TYPE_2_LEVEL.put(Byte.class, 1);
    CANDIDATE_TYPE_2_LEVEL.put(Short.class, 2);
    CANDIDATE_TYPE_2_LEVEL.put(Integer.class, 3);
    CANDIDATE_TYPE_2_LEVEL.put(Long.class, 4);
    CANDIDATE_TYPE_2_LEVEL.put(Float.class, 5);
    CANDIDATE_TYPE_2_LEVEL.put(Double.class, 6);
    CANDIDATE_TYPE_2_LEVEL.put(BigDecimal.class, 7);
    CANDIDATE_TYPE_2_LEVEL.put(String.class, 8);
  }

  private static boolean canCastType(Class from, Class to) {
    if (from == to) {
      return true;
    }
    if (from == String.class && (to == Double.class
        || to == BigDecimal.class)) {
      return true;
    }
    if (Number.class.isAssignableFrom(from) && to == String.class) {
      return true;
    }
    Integer f = CANDIDATE_TYPE_2_LEVEL.get(from);
    Integer t = CANDIDATE_TYPE_2_LEVEL.get(to);
    return f != null && t != null && f <= t;
  }

  public static List<Class> consistentKeyTypes(List<Class> leftKeysClassList,
      List<Class> rightKeysClassList) {
    assert leftKeysClassList.size() == rightKeysClassList.size();
    List<Class> targetFieldClasses = new ArrayList<>();
    int size = leftKeysClassList.size();
    for (int i = 0; i < size; i++) {
      Class leftKeyClass = leftKeysClassList.get(i);
      Class rightKeyClass = rightKeysClassList.get(i);
      if (leftKeyClass == rightKeyClass) {
        targetFieldClasses.add(leftKeyClass);
      } else {
        //try to convert leftKeyClass and rightKeyClass to same class
        // Allow implicit conversion from Byte -> Integer -> Long -> Float
        // ->DoubleDecimal -> String
        Class targetClass = null;
        for (Class to : CANDIDATE_TYPE_2_LEVEL.keySet()) {
          if (canCastType(leftKeyClass, to) && canCastType(rightKeyClass, to)) {
            targetClass = to;
            break;
          }
        }
        if (targetClass == null) {
          //can't get consistentKeyTypes,quick fail
          return null;
        } else {
          targetFieldClasses.add(targetClass);
        }
      }
    }
    return targetFieldClasses;
  }
}

// End EquiJoin.java
