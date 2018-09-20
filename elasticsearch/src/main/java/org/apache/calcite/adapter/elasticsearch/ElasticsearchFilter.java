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
package org.apache.calcite.adapter.elasticsearch;

import org.apache.calcite.plan.RelOptCluster;
import org.apache.calcite.plan.RelOptCost;
import org.apache.calcite.plan.RelOptPlanner;
import org.apache.calcite.plan.RelOptUtil;
import org.apache.calcite.plan.RelTraitSet;
import org.apache.calcite.rel.RelNode;
import org.apache.calcite.rel.core.Filter;
import org.apache.calcite.rel.metadata.RelMetadataQuery;
import org.apache.calcite.rex.RexCall;
import org.apache.calcite.rex.RexInputRef;
import org.apache.calcite.rex.RexLiteral;
import org.apache.calcite.rex.RexNode;
import org.apache.calcite.util.JsonBuilder;
import org.apache.calcite.util.Pair;

import com.fasterxml.jackson.core.JsonGenerator;
import com.fasterxml.jackson.databind.ObjectMapper;
import com.google.common.collect.HashMultimap;
import com.google.common.collect.Multimap;

import java.io.IOException;
import java.io.StringWriter;
import java.io.UncheckedIOException;
import java.util.ArrayList;
import java.util.Collection;
import java.util.HashMap;
import java.util.LinkedHashMap;
import java.util.List;
import java.util.Map;
import java.util.Objects;

/**
 * Implementation of a {@link org.apache.calcite.rel.core.Filter}
 * relational expression in Elasticsearch.
 */
public class ElasticsearchFilter extends Filter implements ElasticsearchRel {
  ElasticsearchFilter(RelOptCluster cluster, RelTraitSet traitSet, RelNode child,
      RexNode condition) {
    super(cluster, traitSet, child, condition);
    assert getConvention() == ElasticsearchRel.CONVENTION;
    assert getConvention() == child.getConvention();
  }

  @Override public RelOptCost computeSelfCost(RelOptPlanner planner, RelMetadataQuery mq) {
    return super.computeSelfCost(planner, mq).multiplyBy(0.1);
  }

  @Override public Filter copy(RelTraitSet relTraitSet, RelNode input, RexNode condition) {
    return new ElasticsearchFilter(getCluster(), relTraitSet, input, condition);
  }

  @Override public void implement(Implementor implementor) {
    implementor.visitChild(0, getInput());
    ObjectMapper mapper = implementor.elasticsearchTable.mapper;
    PredicateAnalyzerTranslator translator = new PredicateAnalyzerTranslator(mapper);
    try {
      implementor.add(translator.translateMatch(condition));
    } catch (IOException e) {
      throw new UncheckedIOException(e);
    } catch (PredicateAnalyzer.ExpressionNotAnalyzableException e) {
      throw new RuntimeException(e);
    }
  }

  /**
   * New version of translator which uses visitor pattern
   * and allow to process more complex (boolean) predicates.
   */
  static class PredicateAnalyzerTranslator {
    private final ObjectMapper mapper;

    PredicateAnalyzerTranslator(final ObjectMapper mapper) {
      this.mapper = Objects.requireNonNull(mapper, "mapper");
    }

    String translateMatch(RexNode condition) throws IOException,
        PredicateAnalyzer.ExpressionNotAnalyzableException {

      StringWriter writer = new StringWriter();
      JsonGenerator generator = mapper.getFactory().createGenerator(writer);
      QueryBuilders.constantScoreQuery(PredicateAnalyzer.analyze(condition)).writeJson(generator);
      generator.flush();
      generator.close();
      return "{\"query\" : " + writer.toString() + "}";
    }
  }

  /**
   * Translates {@link RexNode} expressions into Elasticsearch expression strings.
   */
  static class Translator {
    final JsonBuilder builder = new JsonBuilder();
    final Multimap<String, Pair<String, RexLiteral>> multimap =
        HashMultimap.create();
    final Map<String, RexLiteral> eqMap = new LinkedHashMap<>();
    private final List<String> fieldNames;

    Translator(List<String> fieldNames) {
      this.fieldNames = fieldNames;
    }

    private String translateMatch(RexNode condition) {
      // filter node
      final Map<String, Object> filterMap = new LinkedHashMap<>();
      filterMap.put("filter", translateOr(condition));

      // constant_score node
      final Map<String, Object> map = builder.map();
      map.put("constant_score", filterMap);

      return "\"query\" : " + builder.toJsonString(map).replaceAll("\\s+", "");
    }

    private Object translateOr(RexNode condition) {
      final List<Object> list = new ArrayList<>();

      final List<RexNode> orNodes = RelOptUtil.disjunctions(condition);
      for (RexNode node : orNodes) {
        List<Map<String, Object>> andNodes = translateAnd(node);

        if (andNodes.size() > 0) {
          Map<String, Object> andClause = new HashMap<>();
          andClause.put("must", andNodes);

          // boolean filters
          LinkedHashMap<String, Object> filterEvaluator = new LinkedHashMap<>();
          filterEvaluator.put("bool", andClause);
          list.add(filterEvaluator);
        } else {
          list.add(andNodes.get(0));
        }
      }

      if (orNodes.size() > 1) {
        Map<String, Object> map = builder.map();
        map.put("should", list);

        // boolean filters
        LinkedHashMap<String, Object> filterEvaluator = new LinkedHashMap<>();
        filterEvaluator.put("bool", map);
        return filterEvaluator;
      } else {
        return list.get(0);
      }
    }

    private void addPredicate(Map<String, Object> map, String op, Object v) {
      if (map.containsKey(op) && stronger(op, map.get(op), v)) {
        return;
      }
      map.put(op, v);
    }

    /**
     * Translates a condition that may be an AND of other conditions. Gathers
     * together conditions that apply to the same field.
     *
     * @param node0 expression node
     * @return list of elastic search term filters
     */
    private List<Map<String, Object>> translateAnd(RexNode node0) {
      eqMap.clear();
      multimap.clear();
      for (RexNode node : RelOptUtil.conjunctions(node0)) {
        translateMatch2(node);
      }
      List<Map<String, Object>> filters = new ArrayList<>();
      for (Map.Entry<String, RexLiteral> entry : eqMap.entrySet()) {
        multimap.removeAll(entry.getKey());

        Map<String, Object> filter = new HashMap<>();
        filter.put(entry.getKey(), literalValue(entry.getValue()));

        Map<String, Object> map = new HashMap<>();
        map.put("term", filter);
        filters.add(map);
      }
      for (Map.Entry<String, Collection<Pair<String, RexLiteral>>> entry
          : multimap.asMap().entrySet()) {
        Map<String, Object> map2 = builder.map();

        Map<String, Object> map = new HashMap<>();
        for (Pair<String, RexLiteral> s : entry.getValue()) {
          if (!s.left.equals("not")) {
            addPredicate(map2, s.left, literalValue(s.right));

            Map<String, Object> filter = new HashMap<>();
            filter.put(entry.getKey(), map2);

            map.put("range", filter);
          } else {
            map2.put(entry.getKey(), literalValue(s.right));

            Map<String, Object> termMap = new HashMap<>();
            termMap.put("term", map2);

            map.put("not", termMap);
          }
        }
        filters.add(map);
      }
      return filters;
    }

    private boolean stronger(String key, Object v0, Object v1) {
      if (key.equals("lt") || key.equals("lte")) {
        if (v0 instanceof Number && v1 instanceof Number) {
          return ((Number) v0).doubleValue() < ((Number) v1).doubleValue();
        }
        if (v0 instanceof String && v1 instanceof String) {
          return v0.toString().compareTo(v1.toString()) < 0;
        }
      }
      if (key.equals("gt") || key.equals("gte")) {
        return stronger("lt", v1, v0);
      }
      return false;
    }

    private static Object literalValue(RexLiteral literal) {
      return literal.getValue2();
    }

    private Void translateMatch2(RexNode node) {
      switch (node.getKind()) {
      case EQUALS:
        return translateBinary(null, null, (RexCall) node);
      case LESS_THAN:
        return translateBinary("lt", "gt", (RexCall) node);
      case LESS_THAN_OR_EQUAL:
        return translateBinary("lte", "gte", (RexCall) node);
      case NOT_EQUALS:
        return translateBinary("not", "not", (RexCall) node);
      case GREATER_THAN:
        return translateBinary("gt", "lt", (RexCall) node);
      case GREATER_THAN_OR_EQUAL:
        return translateBinary("gte", "lte", (RexCall) node);
      default:
        throw new AssertionError("cannot translate " + node);
      }
    }

    /**
     * Translates a call to a binary operator, reversing arguments if
     * necessary.
     * @param op operation
     * @param rop opposite operation of {@code op}
     * @param call current relational call
     * @return result can be ignored
     */
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

    /**
     * Translates a call to a binary operator. Returns whether successful.
     * @param op operation
     * @param left left node of the expression
     * @param right right node of the expression
     * @return {@code true} if translation happened, {@code false} otherwise
     */
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
        String itemName = ElasticsearchRules.isItem((RexCall) left);
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
        eqMap.put(name, right);
      } else {
        multimap.put(name, Pair.of(op, right));
      }
    }
  }
}

// End ElasticsearchFilter.java
