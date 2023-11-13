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
import org.apache.calcite.rex.RexBuilder;
import org.apache.calcite.rex.RexCall;
import org.apache.calcite.rex.RexInputRef;
import org.apache.calcite.rex.RexLiteral;
import org.apache.calcite.rex.RexNode;
import org.apache.calcite.rex.RexUtil;
import org.apache.calcite.sql.SqlKind;
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
    final Multimap<String, Pair<String, RexLiteral>> multimap =
        HashMultimap.create();
    final Map<String, RexLiteral> eqMap =
        new LinkedHashMap<>();
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

    private Object translateOr(RexNode condition) {
      final RexNode condition2 =
          RexUtil.expandSearch(rexBuilder, null, condition);

      List<Object> list = new ArrayList<>();
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
      eqMap.clear();
      multimap.clear();
      for (RexNode node : RelOptUtil.conjunctions(node0)) {
        translateMatch2(node);
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
          addPredicate(map2, s.left, literalValue(s.right));
        }
        map.put(entry.getKey(), map2);
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

//    private Void translateLike(RexCall node) {
//      // RexNode 객체에서 LIKE 패턴 추출
//      String likePattern = ((RexLiteral) node.operands.get(1)).getValueAs(String.class);
//
//      // SQL LIKE 패턴을 MongoDB regex 패턴으로 변환
//      String regexPattern = likePattern.replace("%", ".*").replace("_", ".");
//
//      // TODO: 이제 regexPattern를 사용하여 필요한 작업 수행
//      System.out.println("translateLike 함수");
//      return null;
//    }

//    private Void translateLike(RexCall call) {
//      System.out.println("translateLike 안");
//      // RexCall 객체에서 필요한 정보를 추출합니다.
//      final RexNode left = call.operands.get(0);
//      final RexNode right = call.operands.get(1);
//
//      System.out.println("피연산자 가져오기 성공");
//
//      // left가 INPUT_REF인지 확인합니다.
//      if (left.getKind() != SqlKind.INPUT_REF) {
//        System.out.println("INPUT_REF TYPE ");
//        return null;
//      }
//
//      // left를 RexInputRef 타입으로 캐스팅하고, 필드 이름을 가져옵니다.
//      final RexInputRef left1 = (RexInputRef) left;
//      String name = fieldNames.get(left1.getIndex());
//
//      // right가 LITERAL인지 확인하고, RexLiteral 타입으로 캐스팅합니다.
//      if (right.getKind() != SqlKind.LITERAL) {
//        return null;
//      }
//
//      System.out.println("타입 변경 성공");
//
//      RexLiteral rightLiteral = (RexLiteral) right;
//
//      // LIKE 패턴을 정규 표현식으로 변환합니다.
//      String likePattern = rightLiteral.toString();
//      String regexPatternString = likePattern.replace("%", ".*").replace("_", ".");
//      RexLiteral regexPattern = rexBuilder.makeLiteral(regexPatternString);
//
//      System.out.println("패턴 변환 성공");
//
//      // MongoDB의 regex 연산을 수행합니다.
//      translateOp2("$regex", name, regexPattern);
//
//      System.out.println("성공");
//
//      throw new AssertionError("cannot translate op call " + call);
//    }

    private Void translateLike(RexCall call) {
//      System.out.println("Inside translateLike Method");

      final RexNode left = call.operands.get(0);
      final RexNode right = call.operands.get(1);

//      System.out.println("Bring Operands - Success");
//      System.out.println("Left: " + left);
//      System.out.println("Right: " + right);

      boolean b = translateLike2(left ,right);
//      System.out.println("b: " + b);

      if (b) {
        return null;
      }

      throw new AssertionError("cannot translate op call" + call);
    }

    private boolean translateLike2(RexNode left, RexNode right) {
      switch (right.getKind()) {
      case LITERAL:
//        System.out.println("Right is LITERAL");
        break;
      default:
        return false;
      }
//      System.out.println("After Switch-Case");

      final RexLiteral rightLiteral = (RexLiteral) right;
//      System.out.println("RexLiteral");

      String likePattern = rightLiteral.getValue2().toString();
//      System.out.println("likePattern: " + likePattern);

      String regexPatternString = likePattern.replace("%", ".*").replace("_", ".");
      RexLiteral regexPattern = rexBuilder.makeLiteral(regexPatternString);
//      System.out.println("regexPatter: " + regexPattern);

//      System.out.println("Left Kind: " + left.getKind());
      switch (left.getKind()) {
      case INPUT_REF:
//        System.out.println("Left is INPUT_REF");
        final RexInputRef left1 = (RexInputRef) left;
        String name = fieldNames.get(left1.getIndex());
        translateOp2("$regex", name, regexPattern);
        return true;
      case CAST:
//        System.out.println("Left is CAST");
        return translateLike2(((RexCall) left).operands.get(0), right);
      case ITEM:
//        System.out.println("Left is ITEM");
        String itemName = MongoRules.isItem((RexCall) left);
//        System.out.println("itemName: " + itemName);
        if (itemName != null) {
//          System.out.println("itemName is not Null");
          translateOp2("$regex", itemName, regexPattern);
          return true;
        }
      default:
//        System.out.println("Left is default");
        return false;
      }
    }



    private Void translateMatch2(RexNode node) {
      switch (node.getKind()) {
      case EQUALS:
        return translateBinary(null, null, (RexCall) node);
      case LESS_THAN:
        return translateBinary("$lt", "$gt", (RexCall) node);
      case LESS_THAN_OR_EQUAL:
        return translateBinary("$lte", "$gte", (RexCall) node);
      case NOT_EQUALS:
        return translateBinary("$ne", "$ne", (RexCall) node);
      case GREATER_THAN:
        return translateBinary("$gt", "$lt", (RexCall) node);
      case GREATER_THAN_OR_EQUAL:
        return translateBinary("$gte", "$lte", (RexCall) node);
      case LIKE:
//        System.out.println("translateMatch2 안");
        return translateLike((RexCall) node);

      default:
        throw new AssertionError("cannot translate " + node);
      }
    }

    /** Translates a call to a binary operator, reversing arguments if
     * necessary. */
    private Void translateBinary(String op, String rop, RexCall call) {
      final RexNode left = call.operands.get(0);
      final RexNode right = call.operands.get(1);
      boolean b = translateBinary2(op, left, right);
      if (b) {
        return null;
      }
      b = translateBinary2(rop, right, left);
      if (b) {
        return null;
      }
      throw new AssertionError("cannot translate op " + op + " call " + call);
    }

    /** Translates a call to a binary operator. Returns whether successful. */
    private boolean translateBinary2(String op, RexNode left, RexNode right) {
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
        translateOp2(op, name, rightLiteral);
        return true;
      case CAST:
        return translateBinary2(op, ((RexCall) left).operands.get(0), right);
      case ITEM:
        String itemName = MongoRules.isItem((RexCall) left);
        if (itemName != null) {
          translateOp2(op, itemName, rightLiteral);
          return true;
        }
        // fall through
      default:
        return false;
      }
    }

    private void translateOp2(String op, String name, RexLiteral right) {
      if (op == null) {
        // E.g.: {deptno: 100}
        eqMap.put(name, right);
//        System.out.println("eqMap: " + eqMap.toString());
      } else {
        // E.g. {deptno: {$lt: 100}}
        // which may later be combined with other conditions:
        // E.g. {deptno: [$lt: 100, $gt: 50]}
        multimap.put(name, Pair.of(op, right));
//        System.out.println("multimap: " + multimap.toString());
      }
    }
  }
}
