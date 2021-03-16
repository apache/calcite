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

import java.util.Map;
import java.util.Set;

/**
 * VisualizerRuleMatchInfo is a helper data class for Volcano Visualizer.
 * This class will be serialized to JSON and read by the Javascript file.
 */
public class VisualizerRuleMatchInfo {

  private Map<String, String> setLabels;
  private Map<String, String> setOriginalRel;
  private Map<String, Set<String>> nodesInSet;
  private Map<String, Set<String>> nodeInputs;
  private Set<String> matchedNodes;
  private Set<String> newNodes;
  private Set<String> importanceZeroNodes;

  public VisualizerRuleMatchInfo() {}

  public VisualizerRuleMatchInfo(
      Map<String, String> setLabels,
      Map<String, String> setOriginalRel,
      Map<String, Set<String>> nodesInSet,
      Map<String, Set<String>> nodeInputs, Set<String> matchedNodes,
      Set<String> newNodes,
      Set<String> importanceZeroNodes) {
    this.setLabels = setLabels;
    this.setOriginalRel = setOriginalRel;
    this.nodesInSet = nodesInSet;
    this.nodeInputs = nodeInputs;
    this.matchedNodes = matchedNodes;
    this.newNodes = newNodes;
    this.importanceZeroNodes = importanceZeroNodes;
  }

  public Map<String, String> getSetLabels() {
    return setLabels;
  }

  public void setSetLabels(Map<String, String> setLabels) {
    this.setLabels = setLabels;
  }

  public Map<String, Set<String>> getNodesInSet() {
    return nodesInSet;
  }

  public void setNodesInSet(Map<String, Set<String>> nodesInSet) {
    this.nodesInSet = nodesInSet;
  }

  public Map<String, Set<String>> getNodeInputs() {
    return nodeInputs;
  }

  public void setNodeInputs(Map<String, Set<String>> nodeInputs) {
    this.nodeInputs = nodeInputs;
  }

  public Set<String> getMatchedNodes() {
    return matchedNodes;
  }

  public void setMatchedNodes(Set<String> matchedNodes) {
    this.matchedNodes = matchedNodes;
  }

  public Set<String> getNewNodes() {
    return newNodes;
  }

  public void setNewNodes(Set<String> newNodes) {
    this.newNodes = newNodes;
  }

  public Map<String, String> getSetOriginalRel() {
    return setOriginalRel;
  }

  public void setSetOriginalRel(Map<String, String> setOriginalRel) {
    this.setOriginalRel = setOriginalRel;
  }

  public Set<String> getImportanceZeroNodes() {
    return importanceZeroNodes;
  }

  public void setImportanceZeroNodes(Set<String> importanceZeroNodes) {
    this.importanceZeroNodes = importanceZeroNodes;
  }
}
