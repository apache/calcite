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
package org.eigenbase.rel;

import java.io.IOException;
import java.lang.reflect.Constructor;
import java.lang.reflect.InvocationTargetException;
import java.util.*;

import org.eigenbase.relopt.*;
import org.eigenbase.reltype.*;
import org.eigenbase.rex.*;
import org.eigenbase.util.Pair;
import org.eigenbase.util.Util;

import net.hydromatic.optiq.Schema;
import net.hydromatic.optiq.util.BitSets;

import com.fasterxml.jackson.core.JsonParser;
import com.fasterxml.jackson.core.type.TypeReference;
import com.fasterxml.jackson.databind.ObjectMapper;

import com.google.common.collect.ImmutableList;

/**
 * Reads a JSON plan and converts it back to a tree of relational expressions.
 *
 * @see org.eigenbase.rel.RelInput
 */
public class RelJsonReader {
  private static final TypeReference<LinkedHashMap<String, Object>> TYPE_REF =
      new TypeReference<LinkedHashMap<String, Object>>() {
      };

  private final RelOptCluster cluster;
  private final RelOptSchema relOptSchema;
  private final Schema schema;
  private final RelJson relJson = new RelJson(null);
  private final Map<String, RelNode> relMap =
      new LinkedHashMap<String, RelNode>();
  private RelNode lastRel;

  public RelJsonReader(RelOptCluster cluster, RelOptSchema relOptSchema,
      Schema schema) {
    this.cluster = cluster;
    this.relOptSchema = relOptSchema;
    this.schema = schema;
  }

  public RelNode read(String s) throws IOException {
    lastRel = null;
    final ObjectMapper mapper = new ObjectMapper();
    mapper.configure(JsonParser.Feature.ALLOW_UNQUOTED_FIELD_NAMES, true);
    mapper.configure(JsonParser.Feature.ALLOW_SINGLE_QUOTES, true);
    Map<String, Object> o = mapper.readValue(s, TYPE_REF);
    readRels((List<Map<String, Object>>) o.get("rels"));
    System.out.println(lastRel);
    return lastRel;
  }

  private void readRels(List<Map<String, Object>> jsonRels) {
    for (Map<String, Object> jsonRel : jsonRels) {
      readRel(jsonRel);
    }
  }

  private void readRel(final Map<String, Object> jsonRel) {
    String id = (String) jsonRel.get("id");
    String type = (String) jsonRel.get("relOp");
    Constructor constructor = relJson.getConstructor(type);
    RelInput input = new RelInput() {
      public RelOptCluster getCluster() {
        return cluster;
      }

      public RelTraitSet getTraitSet() {
        return cluster.traitSetOf(Convention.NONE);
      }

      public RelOptTable getTable(String table) {
        final List<String> list = (List<String>) jsonRel.get(table);
        return relOptSchema.getTableForMember(list);
      }

      public RelNode getInput() {
        final List<RelNode> inputs = getInputs();
        assert inputs.size() == 1;
        return inputs.get(0);
      }

      public List<RelNode> getInputs() {
        List<String> jsonInputs = (List<String>) jsonRel.get("inputs");
        if (jsonInputs == null) {
          return ImmutableList.of(lastRel);
        }
        final List<RelNode> inputs = new ArrayList<RelNode>();
        for (String jsonInput : jsonInputs) {
          inputs.add(lookupInput(jsonInput));
        }
        return inputs;
      }

      public RexNode getExpression(String tag) {
        return relJson.toRex(this, jsonRel.get(tag));
      }

      public BitSet getBitSet(String tag) {
        return BitSets.of(getIntegerList(tag));
      }

      public List<Integer> getIntegerList(String tag) {
        return (List<Integer>) jsonRel.get(tag);
      }

      public List<AggregateCall> getAggregateCalls(String tag) {
        List<Map<String, Object>> jsonAggs = (List) jsonRel.get(tag);
        final List<AggregateCall> inputs = new ArrayList<AggregateCall>();
        for (Map<String, Object> jsonAggCall : jsonAggs) {
          inputs.add(toAggCall(jsonAggCall));
        }
        return inputs;
      }

      public Object get(String tag) {
        return jsonRel.get(tag);
      }

      public String getString(String tag) {
        return (String) jsonRel.get(tag);
      }

      public float getFloat(String tag) {
        return ((Number) jsonRel.get(tag)).floatValue();
      }

      public boolean getBoolean(String tag) {
        return (Boolean) jsonRel.get(tag);
      }

      public <E extends Enum<E>> E getEnum(String tag, Class<E> enumClass) {
        return Util.enumVal(enumClass, getString(tag).toUpperCase());
      }

      public List<RexNode> getExpressionList(String tag) {
        final List<Object> jsonNodes = (List) jsonRel.get(tag);
        final List<RexNode> nodes = new ArrayList<RexNode>();
        for (Object jsonNode : jsonNodes) {
          nodes.add(relJson.toRex(this, jsonNode));
        }
        return nodes;
      }

      public RelDataType getRowType(String tag) {
        final Object o = jsonRel.get(tag);
        return relJson.toType(cluster.getTypeFactory(), o);
      }

      public RelDataType getRowType(String expressionsTag, String fieldsTag) {
        final List<RexNode> expressionList = getExpressionList(expressionsTag);
        @SuppressWarnings("unchecked") final List<String> names =
            (List<String>) get(fieldsTag);
        return cluster.getTypeFactory().createStructType(
            new AbstractList<Map.Entry<String, RelDataType>>() {
              @Override
              public Map.Entry<String, RelDataType> get(int index) {
                return Pair.of(names.get(index),
                    expressionList.get(index).getType());
              }

              @Override
              public int size() {
                return names.size();
              }
            });
      }

      public RelCollation getCollation() {
        return relJson.toCollation((List) get("collation"));
      }

      public List<List<RexLiteral>> getTuples(String tag) {
        List<List> jsonTuples = (List) get(tag);
        final List<List<RexLiteral>> list = new ArrayList<List<RexLiteral>>();
        for (List jsonTuple : jsonTuples) {
          list.add(getTuple(jsonTuple));
        }
        return list;
      }

      public List<RexLiteral> getTuple(List jsonTuple) {
        final List<RexLiteral> list = new ArrayList<RexLiteral>();
        for (Object jsonValue : jsonTuple) {
          list.add((RexLiteral) relJson.toRex(this, jsonValue));
        }
        return list;
      }
    };
    try {
      final RelNode rel = (RelNode) constructor.newInstance(input);
      relMap.put(id, rel);
      lastRel = rel;
    } catch (InstantiationException e) {
      throw new RuntimeException(e);
    } catch (IllegalAccessException e) {
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
    final String aggName = (String) jsonAggCall.get("agg");
    final Aggregation aggregation = relJson.toAggregation(aggName, jsonAggCall);
    final Boolean distinct = (Boolean) jsonAggCall.get("distinct");
    final List<Integer> operands = (List<Integer>) jsonAggCall.get("operands");
    final RelDataType type =
        relJson.toType(cluster.getTypeFactory(), jsonAggCall.get("type"));
    return new AggregateCall(aggregation, distinct, operands, type, null);
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

// End RelJsonReader.java
