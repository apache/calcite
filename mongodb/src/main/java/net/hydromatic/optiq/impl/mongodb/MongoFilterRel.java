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
package net.hydromatic.optiq.impl.mongodb;

import org.eigenbase.rel.*;
import org.eigenbase.relopt.*;
import org.eigenbase.rex.*;
import org.eigenbase.util.JsonBuilder;
import org.eigenbase.util.Pair;

import com.google.common.collect.HashMultimap;
import com.google.common.collect.Multimap;

import java.util.*;

/**
 * Implementation of a {@link org.eigenbase.rel.FilterRel} relational expression
 * in MongoDB.
 */
public class MongoFilterRel
    extends FilterRelBase
    implements MongoRel {
  public MongoFilterRel(
      RelOptCluster cluster,
      RelTraitSet traitSet,
      RelNode child,
      RexNode condition) {
    super(cluster, traitSet, child, condition);
    assert getConvention() == MongoRel.CONVENTION;
    assert getConvention() == child.getConvention();
  }

  @Override
  public RelOptCost computeSelfCost(RelOptPlanner planner) {
    return super.computeSelfCost(planner).multiplyBy(0.1);
  }

  public MongoFilterRel copy(RelTraitSet traitSet, RelNode input,
      RexNode condition) {
    return new MongoFilterRel(getCluster(), traitSet, input, condition);
  }

  public void implement(Implementor implementor) {
    implementor.visitChild(0, getChild());
    Translator translator =
        new Translator(MongoRules.mongoFieldNames(getRowType()));
    String match = translator.translateMatch(condition);
    implementor.add(null, match);
  }

  /** Translates {@link RexNode} expressions into MongoDB expression strings. */
  static class Translator {
    final JsonBuilder builder = new JsonBuilder();
    final Multimap<String, Pair<String, RexLiteral>> multimap =
        HashMultimap.create();
    final Map<String, RexLiteral> eqMap =
        new LinkedHashMap<String, RexLiteral>();
    private final List<String> fieldNames;

    Translator(List<String> fieldNames) {
      this.fieldNames = fieldNames;
    }

    private String translateMatch(RexNode condition) {
      Map<String, Object> map = builder.map();
      map.put("$match", translateOr(condition));
      return builder.toJsonString(map);
    }

    private Object translateOr(RexNode condition) {
      List<Object> list = new ArrayList<Object>();
      for (RexNode node : RelOptUtil.disjunctions(condition)) {
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
        map.put(entry.getKey(), literalToString(entry.getValue()));
      }
      for (Map.Entry<String, Collection<Pair<String, RexLiteral>>> entry
          : multimap.asMap().entrySet()) {
        Map<String, Object> map2 = builder.map();
        for (Pair<String, RexLiteral> s : entry.getValue()) {
          map2.put(s.left, literalToString(s.right));
        }
        map.put(entry.getKey(), map2);
      }
      return map;
    }

    private static Object literalToString(RexLiteral literal) {
      return literal.getValue2();
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
      case OTHER_FUNCTION:
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
      } else {
        // E.g. {deptno: {$lt: 100}}
        // which may later be combined with other conditions:
        // E.g. {deptno: [$lt: 100, $gt: 50]}
        multimap.put(name, Pair.of(op, right));
      }
    }
  }
}

// End MongoFilterRel.java
