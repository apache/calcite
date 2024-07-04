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

import com.google.common.collect.ImmutableList;
import com.google.common.collect.ImmutableMap;

import java.util.List;
import java.util.Map;

/**
 * A step in the visualizer represents one rule call of the planner.
 */
class StepInfo {
  private final String id;
  private final Map<String, Object> updates;
  private final List<String> matchedRels;

  StepInfo(final String id,
      final Map<String, Object> updates,
      final List<String> matchedRels) {
    this.id = id;
    this.updates = ImmutableMap.copyOf(updates);
    this.matchedRels = ImmutableList.copyOf(matchedRels);
  }

  public String getId() {
    return id;
  }

  public Map<String, Object> getUpdates() {
    return updates;
  }

  public List<String> getMatchedRels() {
    return matchedRels;
  }
}
