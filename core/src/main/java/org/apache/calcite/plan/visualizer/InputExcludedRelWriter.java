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
package org.apache.calcite.plan.visualizer;

import org.apache.calcite.rel.RelNode;
import org.apache.calcite.rel.RelWriter;
import org.apache.calcite.sql.SqlExplainLevel;
import org.apache.calcite.util.Pair;

import org.checkerframework.checker.nullness.qual.Nullable;

import java.util.LinkedHashMap;
import java.util.List;
import java.util.Map;

/**
 * An implement of RelWriter for explaining a single RelNode.
 * The result only contains the properties of the RelNode,
 * but does not explain the children.
 *
 * <pre>{@code
 * InputExcludedRelWriter relWriter = new InputExcludedRelWriter();
 * rel.explain(relWriter);
 * String digest = relWriter.toString();
 * }</pre>
 *
 */
class InputExcludedRelWriter implements RelWriter {

  private final Map<String, @Nullable Object> values = new LinkedHashMap<>();

  InputExcludedRelWriter() {
  }


  @Override public void explain(RelNode rel, List<Pair<String, @Nullable Object>> valueList) {
    valueList.forEach(pair -> values.put(pair.left, pair.right));
  }

  @Override public SqlExplainLevel getDetailLevel() {
    return SqlExplainLevel.EXPPLAN_ATTRIBUTES;
  }

  @Override public RelWriter input(String term, RelNode input) {
    // do nothing, ignore input
    return this;
  }

  @Override public RelWriter item(String term, @Nullable Object value) {
    this.values.put(term, value);
    return this;
  }

  @Override public RelWriter itemIf(String term, @Nullable Object value, boolean condition) {
    if (condition) {
      this.values.put(term, value);
    }
    return this;
  }

  @Override public RelWriter done(RelNode node) {
    return this;
  }

  @Override public boolean nest() {
    return false;
  }

  @Override public String toString() {
    return values.toString();
  }
}
