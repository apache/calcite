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

import org.apache.calcite.plan.RelOptCost;
import org.apache.calcite.plan.RelOptListener;
import org.apache.calcite.plan.RelOptPlanner;
import org.apache.calcite.plan.RelOptRuleCall;
import org.apache.calcite.plan.hep.HepRelVertex;
import org.apache.calcite.plan.volcano.RelSubset;
import org.apache.calcite.rel.RelNode;
import org.apache.calcite.rel.metadata.RelMetadataQuery;

import org.apache.commons.io.IOUtils;

import com.fasterxml.jackson.core.JsonProcessingException;
import com.fasterxml.jackson.core.util.DefaultPrettyPrinter;
import com.fasterxml.jackson.core.util.Separators;
import com.fasterxml.jackson.databind.ObjectMapper;

import org.checkerframework.checker.nullness.qual.Nullable;

import java.io.IOException;
import java.io.InputStream;
import java.io.UncheckedIOException;
import java.nio.file.Files;
import java.nio.file.Path;
import java.nio.file.Paths;
import java.nio.file.StandardOpenOption;
import java.text.DecimalFormat;
import java.text.MessageFormat;
import java.util.ArrayList;
import java.util.Arrays;
import java.util.HashSet;
import java.util.LinkedHashMap;
import java.util.List;
import java.util.Locale;
import java.util.Map;
import java.util.Set;

import static com.google.common.collect.ImmutableList.toImmutableList;

import static org.apache.calcite.util.Util.transform;

import static java.nio.charset.StandardCharsets.UTF_8;
import static java.util.Objects.requireNonNull;

/**
 * This is a tool to visualize the rule match process of a RelOptPlanner.
 *
 * <blockquote><pre>{@code
 * // create the visualizer
 * RuleMatchVisualizer viz =
 *     new RuleMatchVisualizer("/path/to/output/dir", "file-name-suffix");
 * viz.attachTo(planner)
 *
 * planner.findBestExpr();
 *
 * // extra step for HepPlanner: write the output to files
 * // a VolcanoPlanner will call it automatically
 * viz.writeToFile();
 * }</pre></blockquote>
 */
public class RuleMatchVisualizer implements RelOptListener {

  private static final String INITIAL = "INITIAL";
  private static final String FINAL = "FINAL";
  public static final String DEFAULT_SET = "default";

  // default HTML template can be edited at
  // core/src/main/resources/org/apache/calcite/plan/visualizer/viz-template.html
  private static final String TEMPLATE_DIRECTORY =
      "org/apache/calcite/plan/visualizer";

  private final @Nullable String outputDirectory;
  private final @Nullable String outputSuffix;

  private String latestRuleID = "";
  private int latestRuleTransformCount = 1;
  private boolean initialized = false;

  private @Nullable RelOptPlanner planner = null;

  private boolean includeTransitiveEdges = false;
  private boolean includeIntermediateCosts = false;

  private final List<StepInfo> steps = new ArrayList<>();
  private final Map<String, NodeUpdateHelper> allNodes = new LinkedHashMap<>();

  /**
   * Use this constructor to save the result on disk at the end of the planning
   * phase.
   *
   * <p>Note: when using HepPlanner, {@link #writeToFile()} needs to be called
   * manually.
   */
  public RuleMatchVisualizer(
      String outputDirectory,
      String outputSuffix) {
    this.outputDirectory = requireNonNull(outputDirectory, "outputDirectory");
    this.outputSuffix = requireNonNull(outputSuffix, "outputSuffix");
  }

  /**
   * Use this constructor when the result shall not be written to disk.
   */
  public RuleMatchVisualizer() {
    this.outputDirectory = null;
    this.outputSuffix = null;
  }

  /**
   * Attaches the visualizer to the planner.
   * Must be called before applying the rules.
   * Must be called exactly once.
   */
  public void attachTo(RelOptPlanner planner) {
    assert this.planner == null;
    planner.addListener(this);
    this.planner = planner;
  }

  /**
   * Output edges from a subset to the nodes of all subsets that satisfy it.
   */
  public void setIncludeTransitiveEdges(final boolean includeTransitiveEdges) {
    this.includeTransitiveEdges = includeTransitiveEdges;
  }

  /**
   * Output intermediate costs, including all cost updates.
   */
  public void setIncludeIntermediateCosts(final boolean includeIntermediateCosts) {
    this.includeIntermediateCosts = includeIntermediateCosts;
  }

  @Override public void ruleAttempted(RuleAttemptedEvent event) {
    // HepPlanner compatibility
    if (!initialized) {
      requireNonNull(planner, "planner");
      RelNode root = requireNonNull(planner.getRoot());
      initialized = true;
      updateInitialPlan(root);
    }
  }

  /**
   * Register initial plan.
   * (Workaround for HepPlanner)
   */
  private void updateInitialPlan(RelNode node) {
    if (node instanceof HepRelVertex) {
      updateInitialPlan(node.stripped());
      return;
    }
    this.registerRelNode(node);
    for (RelNode input : getInputs(node)) {
      updateInitialPlan(input);
    }
  }

  /**
   * Get the inputs for a node, unwrapping {@link HepRelVertex} nodes.
   * (Workaround for HepPlanner)
   */
  private static List<RelNode> getInputs(final RelNode node) {
    return transform(node.getInputs(), n ->
        n instanceof HepRelVertex ? n.stripped() : n);
  }

  @Override public void relChosen(RelChosenEvent event) {
    if (event.getRel() == null) {
      requireNonNull(planner, "planner");
      RelNode root = requireNonNull(planner.getRoot());
      updateFinalPlan(root);
      this.addStep(FINAL, null);
      this.writeToFile();
    }
  }

  /**
   * Mark nodes that are part of the final plan.
   */
  private void updateFinalPlan(RelNode node) {
    int size = this.steps.size();
    if (size > 0 && FINAL.equals(this.steps.get(size - 1).getId())) {
      return;
    }

    this.registerRelNode(node).updateAttribute("inFinalPlan", Boolean.TRUE);
    if (node instanceof RelSubset) {
      RelNode best = ((RelSubset) node).getBest();
      if (best == null) {
        return;
      }
      updateFinalPlan(best);
    } else {
      for (RelNode input : getInputs(node)) {
        updateFinalPlan(input);
      }
    }
  }

  @Override public void ruleProductionSucceeded(RuleProductionEvent event) {
    // method is called once before ruleMatch, and once after ruleMatch
    if (event.isBefore()) {
      // add the initialState
      if (latestRuleID.isEmpty()) {
        this.addStep(INITIAL, null);
        this.latestRuleID = INITIAL;
      }
      return;
    }

    // we add the state after the rule is applied
    RelOptRuleCall ruleCall = event.getRuleCall();
    String ruleID = Integer.toString(ruleCall.id);
    String displayRuleName = ruleCall.id + "-" + ruleCall.getRule();

    // a rule might call transform to multiple times, handle it by modifying the rule name
    if (ruleID.equals(this.latestRuleID)) {
      latestRuleTransformCount++;
      displayRuleName += "-" + latestRuleTransformCount;
    } else {
      latestRuleTransformCount = 1;
    }
    this.latestRuleID = ruleID;

    this.addStep(displayRuleName, ruleCall);
  }

  @Override public void relDiscarded(RelDiscardedEvent event) {
  }

  @Override public void relEquivalenceFound(RelEquivalenceEvent event) {
    final RelNode rel = requireNonNull(event.getRel());
    Object eqClass = event.getEquivalenceClass();
    if (eqClass instanceof String) {
      String eqClassStr = (String) eqClass;
      eqClassStr = eqClassStr.replace("equivalence class ", "");
      String setId = "set-" + eqClassStr;
      registerSet(setId);
      registerRelNode(rel).updateAttribute("set", setId);
    }
    // register node
    this.registerRelNode(rel);
  }

  /**
   * Add a set.
   */
  private void registerSet(final String setID) {
    this.allNodes.computeIfAbsent(setID, k -> {
      NodeUpdateHelper h = new NodeUpdateHelper(setID, null);
      h.updateAttribute("label", DEFAULT_SET.equals(setID) ? "" : setID);
      h.updateAttribute("kind", "set");
      return h;
    });
  }

  /**
   * Add a RelNode to track its changes.
   */
  private NodeUpdateHelper registerRelNode(final RelNode rel) {
    return this.allNodes.computeIfAbsent(key(rel), k -> {
      NodeUpdateHelper h = new NodeUpdateHelper(key(rel), rel);
      // attributes that need to be set only once
      h.updateAttribute("label", getNodeLabel(rel));
      h.updateAttribute("explanation", getNodeExplanation(rel));
      h.updateAttribute("set", DEFAULT_SET);

      if (rel instanceof RelSubset) {
        h.updateAttribute("kind", "subset");
      }
      return h;
    });
  }

  /**
   * Check and store the changes of the rel node.
   */
  private void updateNodeInfo(final RelNode rel, final boolean isLastStep) {
    NodeUpdateHelper helper = registerRelNode(rel);
    if (this.includeIntermediateCosts || isLastStep) {
      final RelOptPlanner planner = requireNonNull(this.planner);
      RelMetadataQuery mq = rel.getCluster().getMetadataQuery();
      RelOptCost cost = planner.getCost(rel, mq);
      Double rowCount = mq.getRowCount(rel);
      helper.updateAttribute("cost", formatCost(rowCount, cost));
    }

    List<String> inputs = new ArrayList<>();
    if (rel instanceof RelSubset) {
      RelSubset relSubset = (RelSubset) rel;
      relSubset.getRels().forEach(input -> inputs.add(key(input)));
      Set<String> transitive = new HashSet<>();
      relSubset.getSubsetsSatisfyingThis()
          .filter(other -> !other.equals(relSubset))
          .forEach(input -> {
            inputs.add(key(input));
            if (!includeTransitiveEdges) {
              input.getRels().forEach(r -> transitive.add(key(r)));
            }
          });
      inputs.removeAll(transitive);
    } else {
      getInputs(rel).forEach(input -> inputs.add(key(input)));
    }

    helper.updateAttribute("inputs", inputs);
  }

  /**
   * Add the updates since the last step to {@link #steps}.
   */
  private void addStep(String stepID, @Nullable RelOptRuleCall ruleCall) {
    Map<String, Object> nextNodeUpdates = new LinkedHashMap<>();

    // HepPlanner compatibility
    boolean usesDefaultSet = this.allNodes.values()
        .stream()
        .anyMatch(h -> DEFAULT_SET.equals(h.getValue("set")));
    if (usesDefaultSet) {
      this.registerSet(DEFAULT_SET);
    }

    for (NodeUpdateHelper h : allNodes.values()) {
      RelNode rel = h.getRel();
      if (rel != null) {
        updateNodeInfo(rel, FINAL.equals(stepID));
      }
      if (h.isEmptyUpdate()) {
        continue;
      }
      Object update = h.getAndResetUpdate();
      if (update != null) {
        nextNodeUpdates.put(h.getKey(), update);
      }
    }

    List<String> matchedRels =
        Arrays.stream(ruleCall == null ? new RelNode[0] : ruleCall.rels)
            .map(RuleMatchVisualizer::key)
            .collect(toImmutableList());
    this.steps.add(new StepInfo(stepID, nextNodeUpdates, matchedRels));
  }

  public String getJsonStringResult() {
    try {
      LinkedHashMap<String, Object> data = new LinkedHashMap<>();
      data.put("steps", steps);
      ObjectMapper objectMapper = new ObjectMapper();
      Separators withoutSpacesSeparators =
          Separators.createDefaultInstance().
          withObjectFieldValueSpacing(Separators.Spacing.NONE);
      DefaultPrettyPrinter printer =
          new DefaultPrettyPrinter().withSeparators(withoutSpacesSeparators);
      return objectMapper.writer(printer).writeValueAsString(data);
    } catch (JsonProcessingException e) {
      throw new RuntimeException(e);
    }
  }

  /**
   * Writes the HTML and JS files of the rule match visualization.
   *
   * <p>The old files with the same name will be replaced.
   */
  public void writeToFile() {
    if (outputDirectory == null || outputSuffix == null) {
      return;
    }

    try {
      final String templatePath =
          Paths.get(TEMPLATE_DIRECTORY).resolve("viz-template.html").toString();
      final ClassLoader cl = requireNonNull(getClass().getClassLoader());
      final InputStream resourceAsStream =
          requireNonNull(cl.getResourceAsStream(templatePath));
      String htmlTemplate = IOUtils.toString(resourceAsStream, UTF_8);

      String htmlFileName = "planner-viz" + outputSuffix + ".html";
      String dataFileName = "planner-viz-data" + outputSuffix + ".js";

      String replaceString = "src=\"planner-viz-data.js\"";
      int replaceIndex = htmlTemplate.indexOf(replaceString);
      String htmlContent = htmlTemplate.substring(0, replaceIndex)
          + "src=\"" + dataFileName + "\""
          + htmlTemplate.substring(replaceIndex + replaceString.length());

      String dataJsContent = "var data = " + getJsonStringResult() + ";\n";

      Path outputDirPath = Paths.get(outputDirectory);
      Path htmlOutput = outputDirPath.resolve(htmlFileName);
      Path dataOutput = outputDirPath.resolve(dataFileName);

      if (!Files.exists(outputDirPath)) {
        Files.createDirectories(outputDirPath);
      }

      Files.write(htmlOutput, htmlContent.getBytes(UTF_8), StandardOpenOption.CREATE,
          StandardOpenOption.TRUNCATE_EXISTING);
      Files.write(dataOutput, dataJsContent.getBytes(UTF_8), StandardOpenOption.CREATE,
          StandardOpenOption.TRUNCATE_EXISTING);
    } catch (IOException e) {
      throw new UncheckedIOException(e);
    }
  }

  //--------------------------------------------------------------------------------
  // methods related to string representation
  //--------------------------------------------------------------------------------

  private static String key(final RelNode rel) {
    return "" + rel.getId();
  }

  private static String getNodeLabel(final RelNode relNode) {
    if (relNode instanceof RelSubset) {
      final RelSubset relSubset = (RelSubset) relNode;
      String setId = getSetId(relSubset);
      return "subset#" + relSubset.getId() + "-set" + setId + "-\n"
          + relSubset.getTraitSet();
    }

    return "#" + relNode.getId() + "-" + relNode.getRelTypeName();
  }

  private static String getSetId(final RelSubset relSubset) {
    String explanation = getNodeExplanation(relSubset);
    int start = explanation.indexOf("RelSubset") + "RelSubset".length();
    if (start < 0) {
      return "";
    }
    int end = explanation.indexOf(".", start);
    if (end < 0) {
      return "";
    }
    return explanation.substring(start, end);
  }

  private static String getNodeExplanation(final RelNode relNode) {
    InputExcludedRelWriter relWriter = new InputExcludedRelWriter();
    relNode.explain(relWriter);
    return relWriter.toString();
  }

  private static String formatCost(Double rowCount, @Nullable RelOptCost cost) {
    if (cost == null) {
      return "null";
    }
    String originalStr = cost.toString();
    if (originalStr.contains("inf") || originalStr.contains("huge")
        || originalStr.contains("tiny")) {
      return originalStr;
    }
    return new MessageFormat("\nrowCount: {0}\nrows: {1}\ncpu:  {2}\nio:   {3}",
        Locale.ROOT).format(new String[]{
            formatCostScientific(rowCount),
            formatCostScientific(cost.getRows()),
            formatCostScientific(cost.getCpu()),
            formatCostScientific(cost.getIo())
        });
  }

  private static String formatCostScientific(double costNumber) {
    long costRounded = Math.round(costNumber);
    DecimalFormat formatter = (DecimalFormat) DecimalFormat.getInstance(Locale.ROOT);
    formatter.applyPattern("#.#############################################E0");
    return formatter.format(costRounded);
  }

}
