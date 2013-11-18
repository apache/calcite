/*
// Licensed to Julian Hyde under one or more contributor license
// agreements. See the NOTICE file distributed with this work for
// additional information regarding copyright ownership.
//
// Julian Hyde licenses this file to you under the Apache License,
// Version 2.0 (the "License"); you may not use this file except in
// compliance with the License. You may obtain a copy of the License at:
//
// http://www.apache.org/licenses/LICENSE-2.0
//
// Unless required by applicable law or agreed to in writing, software
// distributed under the License is distributed on an "AS IS" BASIS,
// WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
// See the License for the specific language governing permissions and
// limitations under the License.
*/
package net.hydromatic.optiq.impl.mongodb;

import org.eigenbase.rel.*;
import org.eigenbase.relopt.*;
import org.eigenbase.rex.*;
import org.eigenbase.util.JsonBuilder;
import org.eigenbase.util.Pair;
import org.eigenbase.util.Util;

import com.google.common.collect.HashMultimap;
import com.google.common.collect.Multimap;

import java.util.*;

/**
 * Implementation of {@link org.eigenbase.rel.FilterRel} relational expression in
 * MongoDB.
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

  @Override
  public RelNode copy(RelTraitSet traitSet, List<RelNode> inputs) {
    return new MongoFilterRel(getCluster(), traitSet, sole(inputs), condition);
  }

  public void implement(Implementor implementor) {
    implementor.visitChild(0, getChild());
    Foo foo = new Foo(getRowType().getFieldNames());
    String match = foo.translateMatch(condition);
    implementor.add(null, match);
  }

  static class Foo {
    final JsonBuilder builder = new JsonBuilder();
    final Multimap<String, Pair<String, RexLiteral>> multimap =
        HashMultimap.create();
    final Map<String, RexLiteral> eqMap =
        new LinkedHashMap<String, RexLiteral>();
    private final List<String> fieldNames;

    Foo(List<String> fieldNames) {
      this.fieldNames = fieldNames;
    }

    private String translateMatch(RexNode condition) {
      Map<String, Object> map = builder.map();
      map.put("$match", translateOr(condition));
      return builder.toJsonString(map);
    }

    private Object translateOr(RexNode condition) {
      List list = new ArrayList();
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
        return translateOp(null, ((RexCall) node).getOperands());
      case LESS_THAN:
        return translateOp("$lt", ((RexCall) node).getOperands());
      case LESS_THAN_OR_EQUAL:
        return translateOp("$lte", ((RexCall) node).getOperands());
      case NOT_EQUALS:
        return translateOp("$ne", ((RexCall) node).getOperands());
      case GREATER_THAN:
        return translateOp("$gt", ((RexCall) node).getOperands());
      case GREATER_THAN_OR_EQUAL:
        return translateOp("$gte", ((RexCall) node).getOperands());
      default:
        throw new AssertionError("cannot translate " + node);
      }
    }

    private Void translateOp(String op, List<RexNode> operands) {
      RexNode left = operands.get(0);
      RexNode right = operands.get(1);
      if (left instanceof RexInputRef && right instanceof RexLiteral) {
        translateOp2(op, (RexInputRef) left, (RexLiteral) right);
      } else if (right instanceof RexInputRef && left instanceof RexLiteral) {
        translateOp2(op, (RexInputRef) right, (RexLiteral) left);
      } else {
        throw new AssertionError("cannot translate op " + op + " operands "
            + operands);
      }
      return null;
    }

    private void translateOp2(String op, RexInputRef left, RexLiteral right) {
      String name = fieldNames.get(left.getIndex());
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

  private static String list(String start, String end, List list) {
    return start + Util.commaList(list) + end;
  }
}

// End MongoFilterRel.java
