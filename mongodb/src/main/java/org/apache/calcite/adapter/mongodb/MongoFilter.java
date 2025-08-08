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
package org.apache.calcite.adapter.mongodb;

import org.apache.calcite.plan.RelOptCluster;
import org.apache.calcite.plan.RelOptCost;
import org.apache.calcite.plan.RelOptPlanner;
import org.apache.calcite.plan.RelOptUtil;
import org.apache.calcite.plan.RelTraitSet;
import org.apache.calcite.rel.RelNode;
import org.apache.calcite.rel.core.Filter;
import org.apache.calcite.rel.metadata.RelMetadataQuery;
import org.apache.calcite.rel.type.RelDataType;
import org.apache.calcite.rex.RexBuilder;
import org.apache.calcite.rex.RexCall;
import org.apache.calcite.rex.RexInputRef;
import org.apache.calcite.rex.RexLiteral;
import org.apache.calcite.rex.RexNode;
import org.apache.calcite.rex.RexUtil;
import org.apache.calcite.sql.type.SqlTypeName;
import org.apache.calcite.util.JsonBuilder;
import org.apache.calcite.util.Pair;

import com.google.common.collect.HashMultimap;
import com.google.common.collect.Multimap;

import org.checkerframework.checker.nullness.qual.Nullable;

import java.util.ArrayList;
import java.util.Collection;
import java.util.LinkedHashMap;
import java.util.List;
import java.util.Map;

/**
 * Implementation of a {@link org.apache.calcite.rel.core.Filter}
 * relational expression in MongoDB.
 */
public class MongoFilter extends Filter implements MongoRel {
  public MongoFilter(
      RelOptCluster cluster,
      RelTraitSet traitSet,
      RelNode child,
      RexNode condition) {
    super(cluster, traitSet, child, condition);
    assert getConvention() == MongoRel.CONVENTION;
    assert getConvention() == child.getConvention();
  }

  @Override public @Nullable RelOptCost computeSelfCost(RelOptPlanner planner,
      RelMetadataQuery mq) {
    return super.computeSelfCost(planner, mq).multiplyBy(0.1);
  }

  @Override public MongoFilter copy(RelTraitSet traitSet, RelNode input,
      RexNode condition) {
    return new MongoFilter(getCluster(), traitSet, input, condition);
  }

  @Override public void implement(Implementor implementor) {
    implementor.visitChild(0, getInput());
    Translator translator =
        new Translator(implementor.rexBuilder,
            MongoRules.mongoFieldNames(getRowType()));
    String match = translator.translateMatch(condition);
    implementor.add(null, match);
  }

  /** Translates {@link RexNode} expressions into MongoDB expression strings. */
  static class Translator {
    final JsonBuilder builder = new JsonBuilder();
    private final RexBuilder rexBuilder;
    private final List<String> fieldNames;

    Translator(RexBuilder rexBuilder, List<String> fieldNames) {
      this.rexBuilder = rexBuilder;
      this.fieldNames = fieldNames;
    }

    private String translateMatch(RexNode condition) {
      Map<String, Object> map = builder.map();
      map.put("$match", translateOr(condition));
      return builder.toJsonString(map);
    }

    private Map<String, Object> translateOr(RexNode condition) {
      final RexNode condition2 =
          RexUtil.expandSearch(rexBuilder, null, condition);

      List<Map<String, Object>> list = new ArrayList<>();
      for (RexNode node : RelOptUtil.disjunctions(condition2)) {
        list.add(translateAnd(node));
      }
      switch (list.size()) {
      case 1:
        return list.get(0);
      default:
        Map<String, Object> map = builder.map();
        map.put("$or", list);
        return map;
      }
    }

    /** Translates a condition that may be an AND of other conditions. Gathers
     * together conditions that apply to the same field. */
    private Map<String, Object> translateAnd(RexNode node0) {
      final Multimap<String, Pair<String, RexLiteral>> multimap =
          HashMultimap.create();
      final Map<String, RexLiteral> eqMap =
          new LinkedHashMap<>();
      final List<Map<String, Object>> orMapList = new ArrayList<>();
      for (RexNode node : RelOptUtil.conjunctions(node0)) {
        translateMatch2(node, orMapList, multimap, eqMap);
      }
      Map<String, Object> map = builder.map();
      for (Map.Entry<String, RexLiteral> entry : eqMap.entrySet()) {
        multimap.removeAll(entry.getKey());
        map.put(entry.getKey(), literalValue(entry.getValue()));
      }
      for (Map.Entry<String, Collection<Pair<String, RexLiteral>>> entry
          : multimap.asMap().entrySet()) {
        Map<String, Object> map2 = builder.map();
        for (Pair<String, RexLiteral> s : entry.getValue()) {
          String op = s.left;
          if ("$ne".equals(op))  {
            if (map2.containsKey("$nin")) {
              map2.computeIfPresent("$nin", (k, v) -> {
                ((List<Object>) v).add(literalValue(s.right));
                return v;
              });
            } else if (map2.containsKey(op)) {
              // if two $ne conditions, translate to $nin op
              List<Object> ninList = builder.list();
              ninList.add(map2.remove(op));
              ninList.add(literalValue(s.right));
              map2.put("$nin", ninList);
            } else {
              // only one $ne condition
              map2.put(op, literalValue(s.right));
            }
          } else {
            addPredicate(map2, op, literalValue(s.right));
          }
        }
        map.put(entry.getKey(), map2);
      }
      if (!orMapList.isEmpty()) {
        Map<String, Object> andMap = builder.map();
        if (!map.isEmpty()) {
          orMapList.add(map);
        }
        andMap.put("$and", orMapList);
        return andMap;
      }
      return map;
    }

    private static void addPredicate(Map<String, Object> map, String op, Object v) {
      if (map.containsKey(op) && stronger(op, map.get(op), v)) {
        return;
      }
      map.put(op, v);
    }

    /** Returns whether {@code v0} is a stronger value for operator {@code key}
     * than {@code v1}.
     *
     * <p>For example, {@code stronger("$lt", 100, 200)} returns true, because
     * "&lt; 100" is a more powerful condition than "&lt; 200".
     */
    private static boolean stronger(String key, Object v0, Object v1) {
      if (key.equals("$lt") || key.equals("$lte")) {
        if (v0 instanceof Number && v1 instanceof Number) {
          return ((Number) v0).doubleValue() < ((Number) v1).doubleValue();
        }
        if (v0 instanceof String && v1 instanceof String) {
          return v0.toString().compareTo(v1.toString()) < 0;
        }
      }
      if (key.equals("$gt") || key.equals("$gte")) {
        return stronger("$lt", v1, v0);
      }
      return false;
    }

    private static Object literalValue(RexLiteral literal) {
      return literal.getValue2();
    }

    private Void translateMatch2(RexNode node, List<Map<String, Object>> orMapList,
        Multimap<String, Pair<String, RexLiteral>> multimap, Map<String, RexLiteral> eqMap) {
      switch (node.getKind()) {
      case EQUALS:
        return translateBinary(null, null, (RexCall) node, multimap, eqMap);
      case LESS_THAN:
        return translateBinary("$lt", "$gt", (RexCall) node, multimap, eqMap);
      case LESS_THAN_OR_EQUAL:
        return translateBinary("$lte", "$gte", (RexCall) node, multimap, eqMap);
      case NOT_EQUALS:
        return translateBinary("$ne", "$ne", (RexCall) node, multimap, eqMap);
      case GREATER_THAN:
        return translateBinary("$gt", "$lt", (RexCall) node, multimap, eqMap);
      case GREATER_THAN_OR_EQUAL:
        return translateBinary("$gte", "$lte", (RexCall) node, multimap, eqMap);
      case OR:
        return translateOrAddToList(node, orMapList);
      case IS_NOT_NULL:
        return translateUnary("$ne", (RexCall) node, multimap, eqMap);
      case IS_NULL:
        return translateUnary("$eq", (RexCall) node, multimap, eqMap);
      default:
        throw new AssertionError("cannot translate " + node);
      }
    }

    private Void translateOrAddToList(RexNode node, List<Map<String, Object>> orMapList) {
      Map<String, Object> or = translateOr(node);
      orMapList.add(or);
      return null;
    }

    /** Translates a call to a binary operator, reversing arguments if
     * necessary. */
    private Void translateBinary(String op, String rop, RexCall call,
        Multimap<String, Pair<String, RexLiteral>> multimap, Map<String, RexLiteral> eqMap) {
      final RexNode left = call.operands.get(0);
      final RexNode right = call.operands.get(1);
      boolean b = translateBinary2(op, left, right, multimap, eqMap);
      if (b) {
        return null;
      }
      b = translateBinary2(rop, right, left, multimap, eqMap);
      if (b) {
        return null;
      }
      throw new AssertionError("cannot translate op " + op + " call " + call);
    }

    /** Translates a call to a binary operator. Returns whether successful. */
    private boolean translateBinary2(String op, RexNode left, RexNode right,
        Multimap<String, Pair<String, RexLiteral>> multimap, Map<String, RexLiteral> eqMap) {
      switch (right.getKind()) {
      case LITERAL:
        break;
      default:
        return false;
      }
      final RexLiteral rightLiteral = (RexLiteral) right;
      switch (left.getKind()) {
      case INPUT_REF:
        final RexInputRef left1 = (RexInputRef) left;
        String name = fieldNames.get(left1.getIndex());
        translateOp2(op, name, rightLiteral, multimap, eqMap);
        return true;
      case CAST:
        return translateBinary2(op, ((RexCall) left).operands.get(0), right, multimap, eqMap);
      case ITEM:
        String itemName = MongoRules.isItem((RexCall) left);
        if (itemName != null) {
          translateOp2(op, itemName, rightLiteral, multimap, eqMap);
          return true;
        }
        // fall through
      default:
        return false;
      }
    }

    private void translateOp2(String op, String name, RexLiteral right,
        Multimap<String, Pair<String, RexLiteral>> multimap, Map<String, RexLiteral> eqMap) {
      if (op == null) {
        // E.g.: {deptno: 100}
        eqMap.put(name, right);
      } else {
        // E.g. {deptno: {$lt: 100}}
        // which may later be combined with other conditions:
        // E.g. {deptno: [$lt: 100, $gt: 50]}
        multimap.put(name, Pair.of(op, right));
      }
    }

    /** Translates is null/is not null to {$eq: null}/{$ne: null}. */
    private Void translateUnary(String op, RexCall call,
        Multimap<String, Pair<String, RexLiteral>> multimap, Map<String, RexLiteral> eqMap) {
      final RexNode left = call.operands.get(0);
      RelDataType nullType = rexBuilder.getTypeFactory().createSqlType(SqlTypeName.NULL);
      final RexNode right = rexBuilder.makeNullLiteral(nullType);
      translateBinary2(op, left, right, multimap, eqMap);
      return null;
    }
  }
}
