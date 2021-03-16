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

import org.apache.calcite.plan.RelOptCost;
import org.apache.calcite.rel.RelNode;
import org.apache.calcite.tools.visualizer.InputExcludedRelWriter;
import org.apache.calcite.tools.visualizer.VisualizerNodeInfo;
import org.apache.calcite.tools.visualizer.VisualizerRuleMatchInfo;
import org.apache.calcite.tools.visualizer.VisualizerTvrInfo;

import com.fasterxml.jackson.core.JsonProcessingException;
import com.fasterxml.jackson.databind.ObjectMapper;
import com.google.common.base.Charsets;
import com.google.common.collect.HashMultimap;
import com.google.common.collect.SetMultimap;
import com.google.common.io.Resources;

import java.io.IOException;
import java.io.UncheckedIOException;
import java.nio.file.Files;
import java.nio.file.Path;
import java.nio.file.Paths;
import java.util.ArrayList;
import java.util.Collection;
import java.util.Deque;
import java.util.HashMap;
import java.util.HashSet;
import java.util.LinkedList;
import java.util.List;
import java.util.Map;
import java.util.Set;
import java.util.function.Consumer;
import java.util.stream.Collectors;

import static java.util.stream.Collectors.joining;

/**
 * This is tool to visualize the rule match process of the VolcanoPlanner.
 *
 * To use the visualizer, add a listener before the VolcanoPlanner optimization phase,
 * and writes the output to a file after the optimization ends.
 *
 * ```
 * // construct the visualizer and attach a listener to VolcanoPlanner
 * VolcanoRuleMatchVisualizerListener visualizerListener =
 *   new VolcanoRuleMatchVisualizerListener(volcanoPlanner);
 * volcanoPlanner.addListener(visualizerListener);
 *
 * volcanoPlanner.findBestExpr();
 *
 * // after the optimization, adds the final best plan and writes the output to files
 * visualizerListener.getVisualizer().addFinalPlan();
 * visualizerListener.getVisualizer().writeToFile(outputDirectory, null);
 * ```
 *
 */
public class VolcanoRuleMatchVisualizer {

  VolcanoPlanner volcanoPlanner;

  // a sequence of ruleMatch ID to represent the order of rule match
  List<String> ruleMatchSequence = new ArrayList<>();
  // map of ruleMatch ID and the info, including the state snapshot at the time of ruleMatch
  Map<String, VisualizerRuleMatchInfo> ruleInfoMap = new HashMap<>();
  Map<String, List<VisualizerTvrInfo>> ruleTvrInfoMap = new HashMap<>();
  // map of nodeID to the ruleID it's first added
  Map<String, String> nodeAddedInRule = new HashMap<>();

  // a map of relNode ID to the actual RelNode object
  // contains all the relNodes appear during the optimization
  // all RelNode are immutable in Calcite, therefore only new nodes will be added
  Map<String, RelNode> allNodes = new HashMap<>();

  public boolean discardIfNoChange = true;

  public VolcanoRuleMatchVisualizer(VolcanoPlanner volcanoPlanner) {
    this.volcanoPlanner = volcanoPlanner;
  }

  public void addRuleMatch(String ruleCallID, Collection<? extends RelNode> matchedRels,
      boolean discardIfNoChange) {

    // store the current state snapshot
    // nodes contained in the sets
    // and inputs of relNodes (and relSubsets)
    Map<String, String> setLabels = new HashMap<>();
    Map<String, String> setOriginalRel = new HashMap<>();
    Map<String, Set<String>> nodesInSet = new HashMap<>();
    Map<String, Set<String>> nodeInputs = new HashMap<>();

    // newNodes appeared after this ruleCall
    Set<String> newNodes = new HashSet<>();

    // populate current snapshot, and fill in the allNodes map
    volcanoPlanner.allSets.forEach(set -> {
      String setID = "set-" + set.id;
      String setLabel = getSetLabel(set);
      setLabels.put(setID, setLabel);
      setOriginalRel.put(setID, set.rel == null ? "" : String.valueOf(set.rel.getId()));

      nodesInSet.put(setID, nodesInSet.getOrDefault(setID, new HashSet<>()));

      Consumer<RelNode> addNode = rel -> {
        String nodeID = String.valueOf(rel.getId());
        nodesInSet.get(setID).add(nodeID);

        if (!allNodes.containsKey(nodeID)) {
          newNodes.add(nodeID);
          allNodes.put(nodeID, rel);
        }
      };

      Consumer<RelNode> addLink = rel -> {
        String nodeID = String.valueOf(rel.getId());
        nodeInputs.put(nodeID, new HashSet<>());
        if (rel instanceof RelSubset) {
          RelSubset relSubset = (RelSubset) rel;
          relSubset.getRelList().stream()
              .filter(input -> input.getTraitSet().equals(relSubset.getTraitSet()))
              .forEach(input -> nodeInputs.get(nodeID).add(String.valueOf(input.getId())));
          relSubset.set.subsets.stream()
              .filter(other -> !other.equals(relSubset))
              .filter(other -> other.getTraitSet().satisfies(relSubset.getTraitSet()))
              .forEach(other -> nodeInputs.get(nodeID).add(String.valueOf(other.getId())));
        } else {
          rel.getInputs().forEach(input -> nodeInputs.get(nodeID)
              .add(String.valueOf(input.getId())));
        }
      };

      set.rels.forEach(addNode);
      set.subsets.forEach(addNode);
      set.rels.forEach(addLink);
      set.subsets.forEach(addLink);
    });

    // add tvr info
    List<VisualizerTvrInfo> tvrs = volcanoPlanner.allSets.stream()
        .flatMap(set -> set.tvrLinks.values().stream()).distinct()
        .map(tvrMetaSet -> snapshotTvr(tvrMetaSet))
        .collect(Collectors.toList());

    boolean tvrChanged = ruleMatchSequence.isEmpty() || ruleTvrInfoMap
        .get(ruleMatchSequence.get(ruleMatchSequence.size() - 1)).equals(tvrs);

    if (newNodes.isEmpty() && !tvrChanged && this.discardIfNoChange && discardIfNoChange) {
      return;
    }

    // get the matched nodes of this rule
    Set<String> matchedNodeIDs = matchedRels.stream()
        .map(rel -> String.valueOf(rel.getId()))
        .collect(Collectors.toSet());

    // get importance 0 rels as of right now
    Set<String> importanceZeroNodes = new HashSet<>();
    volcanoPlanner.prunedNodes
        .forEach(rel -> importanceZeroNodes.add(Integer.toString(rel.getId())));

    VisualizerRuleMatchInfo ruleMatchInfo =
        new VisualizerRuleMatchInfo(setLabels, setOriginalRel, nodesInSet,
            nodeInputs, matchedNodeIDs, newNodes, importanceZeroNodes);

    ruleMatchSequence.add(ruleCallID);
    ruleInfoMap.put(ruleCallID, ruleMatchInfo);
    ruleTvrInfoMap.put(ruleCallID, tvrs);

    newNodes.forEach(newNode -> nodeAddedInRule.put(newNode, ruleCallID));
  }

  /**
   * Add a final plan to the variable
   */
  public void addFinalPlan() {
    assert !ruleMatchSequence.contains("FINAL");

    Set<RelNode> finalPlanNodes = new HashSet<>();
    Deque<RelSubset> subsetsToVisit = new LinkedList<>();
    subsetsToVisit.add((RelSubset) volcanoPlanner.getRoot());

    RelSubset subset;
    while ((subset = subsetsToVisit.poll()) != null) {
      // add subset itself to the highlight list
      finalPlanNodes.add(subset);
      // highlight its best node if it exists
      RelNode best = subset.getBest();
      if (best == null) {
        continue;
      }
      finalPlanNodes.add(best);
      // recursively visit the input relSubsets of the best node
      best.getInputs().stream().map(rel -> (RelSubset) rel).forEach(subsetsToVisit::add);
    }

    this.addRuleMatch("FINAL", new ArrayList<>(finalPlanNodes), false);
  }

  private String getSetLabel(RelSet set) {
    StringBuilder sb = new StringBuilder();
    sb.append("set-").append(set.id).append("    ");

    String tvrLinksStr = set.tvrLinks.asMap().entrySet().stream().map(
        e -> e.getKey() + "-[" + e.getValue().stream()
            .map(metaSet -> Integer.toString(metaSet.tvrId))
            .collect(joining(",")) + "]").collect(joining(", "));

    sb.append(tvrLinksStr);

    return sb.toString();
  }


  public VisualizerTvrInfo snapshotTvr(TvrMetaSet tvrMetaSet) {
    SetMultimap<String, String> tvrSets = HashMultimap.create();
    tvrMetaSet.tvrSets.forEach((tvrSemantics, set) -> {
      tvrSets.put(tvrSemantics.toString(), "set-" + set.id);
    });

    SetMultimap<String, String> tvrPropertyLinks = HashMultimap.create();
    tvrMetaSet.tvrPropertyLinks.forEach((tvrProperty, tvr) -> {
      tvrPropertyLinks.put(tvrProperty.toString(), tvr.toString());
    });

    return new VisualizerTvrInfo(tvrMetaSet.toString(), tvrSets.asMap(),
        tvrPropertyLinks.asMap());
  }

  private String getJsonStringResult() {
    try {
      Map<String, VisualizerNodeInfo> nodeInfoMap = new HashMap<>();
      for (String nodeID : allNodes.keySet()) {
        RelNode relNode = allNodes.get(nodeID);

        RelOptCost cost = volcanoPlanner.getCost(
            relNode, volcanoPlanner.getRoot().getCluster().getMetadataQuery());
        Double rowCount =
            relNode.getCluster().getMetadataQuery().getRowCount(relNode);

        VisualizerNodeInfo nodeInfo;
        if (relNode instanceof RelSubset) {
          RelSubset relSubset = (RelSubset) relNode;
          String nodeLabel = "subset#" + relSubset.getId() + "-set#" + relSubset.set.id + "-\n"
              + relSubset.getTraitSet().toString();
          String relIDs = relSubset.getRelList().stream()
              .map(i -> "#" + i.getId()).collect(joining(", "));
          String explanation = "rels: [" + relIDs + "]";
          nodeInfo =
              new VisualizerNodeInfo(nodeLabel, true, explanation, cost, rowCount);
        } else {
          InputExcludedRelWriter relWriter = new InputExcludedRelWriter();
          relNode.explain(relWriter);
          String inputIDs = relNode.getInputs().stream()
              .map(i -> "#" + i.getId()).collect(joining(", "));
          String explanation = relWriter.toString() + ", inputs: [" + inputIDs + "]";

          String nodeLabel = "#" + relNode.getId() + "-" + relNode.getRelTypeName();
          nodeInfo = new VisualizerNodeInfo(nodeLabel, false, explanation, cost,
              rowCount);
        }

        nodeInfoMap.put(nodeID, nodeInfo);
      }

      HashMap<String, Object> data = new HashMap<>();
      data.put("allNodes", nodeInfoMap);
      data.put("ruleMatchSequence", ruleMatchSequence);
      data.put("ruleMatchInfoMap", ruleInfoMap);
      data.put("ruleMatchTvrInfoMap", ruleTvrInfoMap);
      data.put("nodeAddedInRule", nodeAddedInRule);

      ObjectMapper objectMapper = new ObjectMapper();
      return objectMapper.writeValueAsString(data);
    } catch (JsonProcessingException e) {
      throw new RuntimeException(e);
    }
  }

  /**
   * Writes the HTML and JS files of the rule match visualization.
   *
   * The old files with the same name will be replaced.
   *
   * @param outputDirectory directory of the output files
   * @param suffix file name suffix, can be null
   */
  public void writeToFile(String outputDirectory, String suffix) {
    // default HTML template is under "resources"
    writeToFile("volcano-viz", outputDirectory, suffix);
  }

  public void writeToFile(String templateDirectory, String outputDirectory, String suffix) {
    try {
      if (suffix == null) {
        suffix = "";
      }

      String htmlTemplate = Resources.toString(
          Resources.getResource(
          Paths.get(templateDirectory).resolve("viz-template.html").toString()),
          Charsets.UTF_8);
      String tvrHtmlTemplate = Resources.toString(
          Resources.getResource(
          Paths.get(templateDirectory).resolve("viz-tvr-template.html").toString()),
          Charsets.UTF_8);

      String htmlFileName = "volcano-viz" + suffix + ".html";
      String tvrHtmlFileName = "volcano-viz-tvr" + suffix + ".html";
      String dataFileName = "volcano-viz-data" + suffix + ".js";

      String replaceString = "src=\"volcano-viz-data.js\"";
      int replaceIndex = htmlTemplate.indexOf(replaceString);
      String htmlContent = htmlTemplate.substring(0, replaceIndex)
          + "src=\"" + dataFileName + "\""
          + htmlTemplate.substring(replaceIndex + replaceString.length());

      int tvrReplaceIndex = tvrHtmlTemplate.indexOf(replaceString);
      String tvrHtmlContent = tvrHtmlTemplate.substring(0, tvrReplaceIndex)
          + "src=\"" + dataFileName + "\""
          + tvrHtmlTemplate.substring(tvrReplaceIndex + replaceString.length());

      String dataJsContent = "var data = " + getJsonStringResult() + ";\n";

      Path outputDirPath = Paths.get(outputDirectory);
      Path htmlOutput = outputDirPath.resolve(htmlFileName);
      Path tvrHtmlOutput = outputDirPath.resolve(tvrHtmlFileName);
      Path dataOutput = outputDirPath.resolve(dataFileName);

      if (! Files.exists(outputDirPath)) {
        Files.createDirectories(outputDirPath);
      }

      Files.deleteIfExists(htmlOutput);
      Files.deleteIfExists(tvrHtmlOutput);
      Files.deleteIfExists(dataOutput);
      Files.createFile(htmlOutput);
      Files.createFile(tvrHtmlOutput);
      Files.createFile(dataOutput);
      Files.write(htmlOutput, htmlContent.getBytes(Charsets.UTF_8));
      Files.write(tvrHtmlOutput, tvrHtmlContent.getBytes(Charsets.UTF_8));
      Files.write(dataOutput, dataJsContent.getBytes(Charsets.UTF_8));
    } catch (IOException e) {
      throw new UncheckedIOException(e);
    }
  }

}
