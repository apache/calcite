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

import org.checkerframework.checker.nullness.qual.Nullable;

import java.util.Collections;
import java.util.LinkedHashMap;
import java.util.List;
import java.util.Map;
import java.util.Objects;

/**
 * Helper class to create the node update.
 */
class NodeUpdateHelper {

  private final String key;
  private final @Nullable RelNode rel;
  private final NodeUpdateInfo state;
  private @Nullable NodeUpdateInfo update = null;

  NodeUpdateHelper(String key, @Nullable RelNode rel) {
    this.key = key;
    this.rel = rel;
    this.state = new NodeUpdateInfo();
  }

  String getKey() {
    return key;
  }

  @Nullable RelNode getRel() {
    return this.rel;
  }

  void updateAttribute(final String attr, final Object newValue) {
    if (Objects.equals(newValue, state.get(attr))) {
      return;
    }

    state.put(attr, newValue);

    if (update == null) {
      update = new NodeUpdateInfo();
    }

    if (newValue instanceof List
        && ((List<?>) newValue).isEmpty()
        && !update.containsKey(attr)) {
      return;
    }

    update.put(attr, newValue);
  }

  boolean isEmptyUpdate() {
    return this.update == null || update.isEmpty();
  }

  /**
   * Gets an object representing all the changes since the last call to this method.
   *
   * @return an object or null if there are no changes.
   */
  @Nullable Object getAndResetUpdate() {
    if (isEmptyUpdate()) {
      return null;
    }
    NodeUpdateInfo update = this.update;
    this.update = null;
    return update;
  }

  Map<String, Object> getState() {
    return Collections.unmodifiableMap(this.state);
  }

  /**
   * Get the current value for the attribute.
   */
  @Nullable Object getValue(final String attr) {
    return this.state.get(attr);
  }

  /**
   * Type alias.
   */
  private static class NodeUpdateInfo extends LinkedHashMap<String, Object> {
  }
}
