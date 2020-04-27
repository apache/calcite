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
package org.apache.calcite.plan.volcano;

import org.apache.calcite.rel.RelNode;

import com.google.common.collect.HashMultimap;
import com.google.common.collect.Multimap;

import java.util.HashMap;
import java.util.Iterator;
import java.util.LinkedList;
import java.util.List;
import java.util.Map;

/**
 * A rule queue that manage rule matches for cascade planner
 */
public class CascadeRuleQueue {

  private final CascadePlanner planner;
  Map<Integer, List<VolcanoRuleMatch>> matches = new HashMap<>();

  public CascadeRuleQueue(CascadePlanner planner) {
    this.planner = planner;
  }

  public void addMatch(VolcanoRuleMatch match) {
    RelNode rel = match.rel(0);
    List<VolcanoRuleMatch> queue = matches.
        computeIfAbsent(rel.getId(), id -> new LinkedList<>());
    addMatch(match, queue);
  }

  private void addMatch(VolcanoRuleMatch match, List<VolcanoRuleMatch> queue) {
    if (!planner.isSubstituteRule(match)) {
      queue.add(0, match);
    } else {
      queue.add(match);
    }
  }

  public VolcanoRuleMatch popMatch(RelNode node, boolean transform) {
    List<VolcanoRuleMatch> queue = matches.get(node.getId());
    if (queue == null) {
      return null;
    }
    Iterator<VolcanoRuleMatch> iterator = queue.iterator();
    while (iterator.hasNext()) {
      VolcanoRuleMatch next = iterator.next();
      if (transform && !planner.isTransformationRule(next)) {
        continue;
      }
      iterator.remove();
      if (!skipMatch(next)) {
        return next;
      }
    }
    return null;
  }

  private Multimap<Integer, VolcanoRuleMatch> skipped = HashMultimap.create();
  private boolean skipMatch(VolcanoRuleMatch match) {
    for (RelNode rel : match.rels) {
      if (planner.isPruned(rel)) {
        skipped.put(rel.getId(), match);
        return true;
      }
    }
    return false;
  }
}
