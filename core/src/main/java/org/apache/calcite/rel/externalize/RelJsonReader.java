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
package org.apache.calcite.rel.externalize;

import org.apache.calcite.plan.Convention;
import org.apache.calcite.plan.RelOptCluster;
import org.apache.calcite.plan.RelOptSchema;
import org.apache.calcite.plan.RelOptTable;
import org.apache.calcite.plan.RelTraitSet;
import org.apache.calcite.rel.RelCollation;
import org.apache.calcite.rel.RelCollations;
import org.apache.calcite.rel.RelDistribution;
import org.apache.calcite.rel.RelInput;
import org.apache.calcite.rel.RelNode;
import org.apache.calcite.rel.core.AggregateCall;
import org.apache.calcite.rel.type.RelDataType;
import org.apache.calcite.rex.RexLiteral;
import org.apache.calcite.rex.RexNode;
import org.apache.calcite.schema.Schema;
import org.apache.calcite.sql.SqlAggFunction;
import org.apache.calcite.util.ImmutableBitSet;
import org.apache.calcite.util.Pair;
import org.apache.calcite.util.Util;

import com.fasterxml.jackson.core.type.TypeReference;
import com.fasterxml.jackson.databind.DeserializationFeature;
import com.fasterxml.jackson.databind.ObjectMapper;
import com.google.common.collect.ImmutableList;

import org.checkerframework.checker.nullness.qual.Nullable;

import java.io.IOException;
import java.lang.reflect.Constructor;
import java.lang.reflect.InvocationTargetException;
import java.util.AbstractList;
import java.util.ArrayList;
import java.util.LinkedHashMap;
import java.util.List;
import java.util.Locale;
import java.util.Map;

import static java.util.Objects.requireNonNull;

/**
 * Reads a JSON plan and converts it back to a tree of relational expressions.
 *
 * @see org.apache.calcite.rel.RelInput
 */
public class RelJsonReader {
  private static final TypeReference<LinkedHashMap<String, Object>> TYPE_REF =
      new TypeReference<LinkedHashMap<String, Object>>() {
      };

  private final RelOptCluster cluster;
  private final RelOptSchema relOptSchema;
  private final RelJson relJson = new RelJson(null);
  private final Map<String, RelNode> relMap = new LinkedHashMap<>();
  private @Nullable RelNode lastRel;

  public RelJsonReader(RelOptCluster cluster, RelOptSchema relOptSchema,
      Schema schema) {
    this.cluster = cluster;
    this.relOptSchema = relOptSchema;
    Util.discard(schema);
  }

  public RelNode read(String s) throws IOException {
    lastRel = null;
    final ObjectMapper mapper = new ObjectMapper();
    Map<String, Object> o = mapper
        .configure(DeserializationFeature.USE_BIG_DECIMAL_FOR_FLOATS, true)
        .readValue(s, TYPE_REF);
    @SuppressWarnings("unchecked")
    final List<Map<String, Object>> rels = (List) requireNonNull(o.get("rels"), "rels");
    readRels(rels);
    return requireNonNull(lastRel, "lastRel");
  }

  private void readRels(List<Map<String, Object>> jsonRels) {
    for (Map<String, Object> jsonRel : jsonRels) {
      readRel(jsonRel);
    }
  }

  private void readRel(final Map<String, Object> jsonRel) {
    String id = (String) requireNonNull(jsonRel.get("id"), "jsonRel.id");
    String type = (String) jsonRel.get("relOp");
    Constructor constructor = relJson.getConstructor(type);
    RelInput input = new RelInput() {
      @Override public RelOptCluster getCluster() {
        return cluster;
      }

      @Override public RelTraitSet getTraitSet() {
        return cluster.traitSetOf(Convention.NONE);
      }

      @Override public RelOptTable getTable(String table) {
        final List<String> list = requireNonNull(
            getStringList(table),
            () -> "getStringList for " + table);
        return requireNonNull(
            relOptSchema.getTableForMember(list),
            () -> "table " + table + " is not found in schema " + relOptSchema.toString());
      }

      @Override public RelNode getInput() {
        final List<RelNode> inputs = getInputs();
        assert inputs.size() == 1;
        return inputs.get(0);
      }

      @Override public List<RelNode> getInputs() {
        final List<String> jsonInputs = getStringList("inputs");
        if (jsonInputs == null) {
          return ImmutableList.of(requireNonNull(lastRel, "lastRel"));
        }
        final ImmutableList.Builder<RelNode> inputs = new ImmutableList.Builder<>();
        for (String jsonInput : jsonInputs) {
          inputs.add(lookupInput(jsonInput));
        }
        return inputs.build();
      }

      @Override public @Nullable RexNode getExpression(String tag) {
        return relJson.toRex(this, jsonRel.get(tag));
      }

      @Override public ImmutableBitSet getBitSet(String tag) {
        return ImmutableBitSet.of(requireNonNull(getIntegerList(tag), tag));
      }

      @Override public @Nullable List<ImmutableBitSet> getBitSetList(String tag) {
        List<List<Integer>> list = getIntegerListList(tag);
        if (list == null) {
          return null;
        }
        final ImmutableList.Builder<ImmutableBitSet> builder =
            ImmutableList.builder();
        for (List<Integer> integers : list) {
          builder.add(ImmutableBitSet.of(integers));
        }
        return builder.build();
      }

      @Override public @Nullable List<String> getStringList(String tag) {
        //noinspection unchecked
        return (List<String>) jsonRel.get(tag);
      }

      @Override public @Nullable List<Integer> getIntegerList(String tag) {
        //noinspection unchecked
        return (List<Integer>) jsonRel.get(tag);
      }

      @Override public @Nullable List<List<Integer>> getIntegerListList(String tag) {
        //noinspection unchecked
        return (List<List<Integer>>) jsonRel.get(tag);
      }

      @Override public List<AggregateCall> getAggregateCalls(String tag) {
        @SuppressWarnings("unchecked")
        final List<Map<String, Object>> jsonAggs = (List) getNonNull(tag);
        final List<AggregateCall> inputs = new ArrayList<>();
        for (Map<String, Object> jsonAggCall : jsonAggs) {
          inputs.add(toAggCall(jsonAggCall));
        }
        return inputs;
      }

      @Override public @Nullable Object get(String tag) {
        return jsonRel.get(tag);
      }

      private Object getNonNull(String tag) {
        return requireNonNull(get(tag), () -> "no entry for tag " + tag);
      }

      @Override public @Nullable String getString(String tag) {
        return (String) get(tag);
      }

      @Override public float getFloat(String tag) {
        return ((Number) getNonNull(tag)).floatValue();
      }

      @Override public boolean getBoolean(String tag, boolean default_) {
        final Boolean b = (Boolean) get(tag);
        return b != null ? b : default_;
      }

      @Override public <E extends Enum<E>> @Nullable E getEnum(String tag, Class<E> enumClass) {
        return Util.enumVal(enumClass,
            ((String) getNonNull(tag)).toUpperCase(Locale.ROOT));
      }

      @Override public @Nullable List<RexNode> getExpressionList(String tag) {
        @SuppressWarnings("unchecked")
        final List<Object> jsonNodes = (List) jsonRel.get(tag);
        if (jsonNodes == null) {
          return null;
        }
        final List<RexNode> nodes = new ArrayList<>();
        for (Object jsonNode : jsonNodes) {
          nodes.add(relJson.toRex(this, jsonNode));
        }
        return nodes;
      }

      @Override public RelDataType getRowType(String tag) {
        final Object o = getNonNull(tag);
        return relJson.toType(cluster.getTypeFactory(), o);
      }

      @Override public RelDataType getRowType(String expressionsTag, String fieldsTag) {
        final List<RexNode> expressionList = getExpressionList(expressionsTag);
        @SuppressWarnings("unchecked") final List<String> names =
            (List<String>) getNonNull(fieldsTag);
        return cluster.getTypeFactory().createStructType(
            new AbstractList<Map.Entry<String, RelDataType>>() {
              @Override public Map.Entry<String, RelDataType> get(int index) {
                return Pair.of(names.get(index),
                    requireNonNull(expressionList, "expressionList").get(index).getType());
              }

              @Override public int size() {
                return names.size();
              }
            });
      }

      @Override public RelCollation getCollation() {
        //noinspection unchecked
        return relJson.toCollation((List) getNonNull("collation"));
      }

      @Override public RelDistribution getDistribution() {
        //noinspection unchecked
        return relJson.toDistribution((Map<String, Object>) getNonNull("distribution"));
      }

      @Override public ImmutableList<ImmutableList<RexLiteral>> getTuples(String tag) {
        //noinspection unchecked
        final List<List> jsonTuples = (List) getNonNull(tag);
        final ImmutableList.Builder<ImmutableList<RexLiteral>> builder =
            ImmutableList.builder();
        for (List jsonTuple : jsonTuples) {
          builder.add(getTuple(jsonTuple));
        }
        return builder.build();
      }

      public ImmutableList<RexLiteral> getTuple(List jsonTuple) {
        final ImmutableList.Builder<RexLiteral> builder =
            ImmutableList.builder();
        for (Object jsonValue : jsonTuple) {
          builder.add((RexLiteral) relJson.toRex(this, jsonValue));
        }
        return builder.build();
      }
    };
    try {
      final RelNode rel = (RelNode) constructor.newInstance(input);
      relMap.put(id, rel);
      lastRel = rel;
    } catch (InstantiationException | IllegalAccessException e) {
      throw new RuntimeException(e);
    } catch (InvocationTargetException e) {
      final Throwable e2 = e.getCause();
      if (e2 instanceof RuntimeException) {
        throw (RuntimeException) e2;
      }
      throw new RuntimeException(e2);
    }
  }

  private AggregateCall toAggCall(Map<String, Object> jsonAggCall) {
    @SuppressWarnings("unchecked")
    final Map<String, Object> aggMap = (Map) requireNonNull(
        jsonAggCall.get("agg"),
        "agg key is not found");
    final SqlAggFunction aggregation = requireNonNull(
        relJson.toAggregation(aggMap),
        () -> "relJson.toAggregation output for " + aggMap);
    final Boolean distinct = (Boolean) jsonAggCall.get("distinct");
    @SuppressWarnings("unchecked")
    final List<Integer> operands = (List<Integer>) requireNonNull(
        jsonAggCall.get("operands"),
        "jsonAggCall.operands");
    final Integer filterOperand = (Integer) jsonAggCall.get("filter");
    final Object jsonAggType = requireNonNull(jsonAggCall.get("type"), "jsonAggCall.type");
    final RelDataType type =
        relJson.toType(cluster.getTypeFactory(), jsonAggType);
    final String name = (String) jsonAggCall.get("name");
    return AggregateCall.create(aggregation, distinct, false, false, operands,
        filterOperand == null ? -1 : filterOperand,
        RelCollations.EMPTY,
        type, name);
  }

  private RelNode lookupInput(String jsonInput) {
    RelNode node = relMap.get(jsonInput);
    if (node == null) {
      throw new RuntimeException("unknown id " + jsonInput
          + " for relational expression");
    }
    return node;
  }
}
