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

import org.apache.calcite.rel.RelNode;
import org.apache.calcite.rel.RelWriter;
import org.apache.calcite.sql.SqlExplainLevel;
import org.apache.calcite.util.JsonBuilder;
import org.apache.calcite.util.Pair;

import com.google.common.collect.ImmutableList;

import java.util.ArrayList;
import java.util.IdentityHashMap;
import java.util.List;
import java.util.Map;

/**
 * Callback for a relational expression to dump itself as JSON.
 *
 * @see RelJsonReader
 */
public class RelJsonWriter implements RelWriter {
  //~ Instance fields ----------------------------------------------------------

  protected final JsonBuilder jsonBuilder;
  protected final RelJson relJson;
  private final Map<RelNode, String> relIdMap = new IdentityHashMap<>();
  protected final List<Object> relList;
  private final List<Pair<String, Object>> values = new ArrayList<>();
  private String previousId;

  //~ Constructors -------------------------------------------------------------

  public RelJsonWriter() {
    jsonBuilder = new JsonBuilder();
    relList = jsonBuilder.list();
    relJson = new RelJson(jsonBuilder);
  }

  //~ Methods ------------------------------------------------------------------

  protected void explain_(RelNode rel, List<Pair<String, Object>> values) {
    final Map<String, Object> map = jsonBuilder.map();

    map.put("id", null); // ensure that id is the first attribute
    map.put("relOp", relJson.classToTypeName(rel.getClass()));
    for (Pair<String, Object> value : values) {
      if (value.right instanceof RelNode) {
        continue;
      }
      put(map, value.left, value.right);
    }
    // omit 'inputs: ["3"]' if "3" is the preceding rel
    final List<Object> list = explainInputs(rel.getInputs());
    if (list.size() != 1 || !list.get(0).equals(previousId)) {
      map.put("inputs", list);
    }

    final String id = Integer.toString(relIdMap.size());
    relIdMap.put(rel, id);
    map.put("id", id);

    relList.add(map);
    previousId = id;
  }

  private void put(Map<String, Object> map, String name, Object value) {
    map.put(name, relJson.toJson(value));
  }

  private List<Object> explainInputs(List<RelNode> inputs) {
    final List<Object> list = jsonBuilder.list();
    for (RelNode input : inputs) {
      String id = relIdMap.get(input);
      if (id == null) {
        input.explain(this);
        id = previousId;
      }
      list.add(id);
    }
    return list;
  }

  public final void explain(RelNode rel, List<Pair<String, Object>> valueList) {
    explain_(rel, valueList);
  }

  public SqlExplainLevel getDetailLevel() {
    return SqlExplainLevel.ALL_ATTRIBUTES;
  }

  public RelWriter item(String term, Object value) {
    values.add(Pair.of(term, value));
    return this;
  }

  private List<Object> getList(List<Pair<String, Object>> values, String tag) {
    for (Pair<String, Object> value : values) {
      if (value.left.equals(tag)) {
        //noinspection unchecked
        return (List<Object>) value.right;
      }
    }
    final List<Object> list = new ArrayList<>();
    values.add(Pair.of(tag, (Object) list));
    return list;
  }

  public RelWriter done(RelNode node) {
    final List<Pair<String, Object>> valuesCopy =
        ImmutableList.copyOf(values);
    values.clear();
    explain_(node, valuesCopy);
    return this;
  }

  @Override public boolean nest() {
    return true;
  }

  /**
   * Returns a JSON string describing the relational expressions that were just
   * explained.
   */
  public String asString() {
    final Map<String, Object> map = jsonBuilder.map();
    map.put("rels", relList);
    return jsonBuilder.toJsonString(map);
  }
}

// End RelJsonWriter.java
