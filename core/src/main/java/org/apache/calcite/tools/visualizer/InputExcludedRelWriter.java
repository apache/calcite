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
package org.apache.calcite.tools.visualizer;

import org.apache.calcite.rel.RelNode;
import org.apache.calcite.rel.RelWriter;
import org.apache.calcite.sql.SqlExplainLevel;
import org.apache.calcite.util.Pair;

import org.checkerframework.checker.nullness.qual.Nullable;

import java.util.LinkedHashMap;
import java.util.List;
import java.util.Map;

/**
 * This RelWriter is indented to be used for getting a digest of a relNode,
 *  excluding the field of the relNode's inputs.
 * The result digest of the RelNode only contains its own properties.
 * <p>
 *
 * <pre>
 * InputExcludedRelWriter relWriter = new InputExcludedRelWriter();
 * rel.explain(relWriter);
 * String digest = relWriter.toString();
 * </pre>
 *
 */
public class InputExcludedRelWriter implements RelWriter {

  private final Map<String, Object> values = new LinkedHashMap<>();

  public InputExcludedRelWriter() {
  }


  @Override public void explain(RelNode rel, List<Pair<String, Object>> valueList) {
    valueList.forEach(pair -> this.values.put(pair.left, pair.right));
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
