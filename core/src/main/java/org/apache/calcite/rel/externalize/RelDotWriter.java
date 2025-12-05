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
import org.apache.calcite.rel.metadata.RelMetadataQuery;
import org.apache.calcite.sql.SqlExplainLevel;
import org.apache.calcite.util.Pair;
import org.apache.calcite.util.Util;

import com.google.common.collect.HashMultimap;
import com.google.common.collect.Multimap;

import org.checkerframework.checker.nullness.qual.Nullable;
import org.immutables.value.Value;

import java.io.PrintWriter;
import java.util.ArrayList;
import java.util.HashMap;
import java.util.LinkedHashMap;
import java.util.List;
import java.util.Map;
import java.util.function.Predicate;

/**
 * Utility to dump a rel node plan in dot format.
 */
@Value.Enclosing
public class RelDotWriter extends RelWriterImpl {

  //~ Instance fields --------------------------------------------------------

  /**
   * Adjacent list of the plan graph.
   */
  private final Map<RelNode, List<RelNode>> outArcTable = new LinkedHashMap<>();

  private final Map<RelNode, String> nodeLabels = new HashMap<>();

  private final Multimap<RelNode, String> nodeStyles = HashMultimap.create();

  private final WriteOption option;

  //~ Constructors -----------------------------------------------------------

  public RelDotWriter(
      PrintWriter pw, SqlExplainLevel detailLevel,
      boolean withIdPrefix) {
    this(pw, detailLevel, withIdPrefix, WriteOption.DEFAULT);
  }

  public RelDotWriter(
      PrintWriter pw, SqlExplainLevel detailLevel,
      boolean withIdPrefix, WriteOption option) {
    super(pw, detailLevel, withIdPrefix);
    this.option = option;
  }

  //~ Methods ----------------------------------------------------------------

  @Override protected void explain_(RelNode rel,
      List<Pair<String, @Nullable Object>> values) {
    // get inputs
    List<RelNode> inputs = getInputs(rel);
    outArcTable.put(rel, inputs);

    // generate node label
    String label = getRelNodeLabel(rel, values);
    nodeLabels.put(rel, label);

    if (highlightNode(rel)) {
      nodeStyles.put(rel, "bold");
    }

    explainInputs(inputs);
  }

  protected String getRelNodeLabel(
      RelNode rel,
      List<Pair<String, @Nullable Object>> values) {
    List<String> labels = new ArrayList<>();
    StringBuilder sb = new StringBuilder();

    final RelMetadataQuery mq = rel.getCluster().getMetadataQuery();
    if (withIdPrefix) {
      sb.append(rel.getId()).append(":");
    }
    sb.append(rel.getRelTypeName());
    labels.add(sb.toString());
    sb.setLength(0);

    if (detailLevel != SqlExplainLevel.NO_ATTRIBUTES) {
      for (Pair<String, @Nullable Object> value : values) {
        if (value.right instanceof RelNode) {
          continue;
        }
        sb.append(value.left)
            .append(" = ")
            .append(value.right);
        labels.add(sb.toString());
        sb.setLength(0);
      }
    }

    switch (detailLevel) {
    case ALL_ATTRIBUTES:
      sb.append("rowcount = ")
          .append(mq.getRowCount(rel))
          .append(" cumulative cost = ")
          .append(mq.getCumulativeCost(rel))
          .append(" ");
      break;
    default:
      break;
    }
    switch (detailLevel) {
    case NON_COST_ATTRIBUTES:
    case ALL_ATTRIBUTES:
      if (!withIdPrefix) {
        // If we didn't print the rel id at the start of the line, print
        // it at the end.
        sb.append("id = ").append(rel.getId());
      }
      break;
    default:
      break;
    }
    labels.add(sb.toString().trim());
    sb.setLength(0);

    // format labels separately and then concat them
    int leftSpace = option.maxNodeLabelLength();
    List<String> newlabels = new ArrayList<>();
    for (int i = 0; i < labels.size(); i++) {
      if (option.maxNodeLabelLength() != -1 && leftSpace <= 0) {
        if (i < labels.size() - 1) {
          // this is not the last label, but we have to stop here
          newlabels.add("...");
        }
        break;
      }
      String formatted = formatNodeLabel(labels.get(i), option.maxNodeLabelLength());
      newlabels.add(formatted);
      leftSpace -= formatted.length();
    }

    return "\"" + String.join("\\n", newlabels) + "\"";
  }

  private static List<RelNode> getInputs(RelNode parent) {
    return Util.transform(parent.getInputs(), RelNode::stripped);
  }

  private void explainInputs(List<? extends @Nullable RelNode> inputs) {
    for (RelNode input : inputs) {
      if (input == null || nodeLabels.containsKey(input)) {
        continue;
      }
      input.explain(this);
    }
  }

  @Override public RelWriter done(RelNode node) {
    int numOfVisitedNodes = nodeLabels.size();
    super.done(node);
    if (numOfVisitedNodes == 0) {
      // When we enter this method call, no node
      // has been visited. So the current node must be the root of the plan.
      // Now we are exiting the method, all nodes in the plan
      // have been visited, so it is time to dump the plan.

      pw.println("digraph {");

      // print nodes with styles
      for (RelNode rel : nodeStyles.keySet()) {
        String style = String.join(",", nodeStyles.get(rel));
        pw.println(nodeLabels.get(rel) + " [style=\"" + style + "\"]");
      }

      // ordinary arcs
      for (Map.Entry<RelNode, List<RelNode>> entry : outArcTable.entrySet()) {
        RelNode src = entry.getKey();
        String srcDesc = nodeLabels.get(src);
        for (int i = 0; i < entry.getValue().size(); i++) {
          RelNode dst = entry.getValue().get(i);

          // label is the ordinal of the arc
          // arc direction from child to parent, to reflect the direction of data flow
          pw.println(nodeLabels.get(dst) + " -> " + srcDesc + " [label=\"" + i + "\"]");
        }
      }
      pw.println("}");
      pw.flush();
    }
    return this;
  }

  /**
   * Format the label into multiple lines according to the options.
   *
   * @param label the original label.
   * @param limit the maximal length of the formatted label.
   *              -1 means no limit.
   * @return the formatted label.
   */
  private String formatNodeLabel(String label, int limit) {
    label = label.trim();

    // escape quotes in the label.
    label = label.replace("\"", "\\\"");

    boolean trimmed = false;
    if (limit != -1 && label.length() > limit) {
      label = label.substring(0, limit);
      trimmed = true;
    }

    if (option.maxNodeLabelPerLine() == -1) {
      // no need to split into multiple lines.
      return label + (trimmed ? "..." : "");
    }

    List<String> descParts = new ArrayList<>();
    for (int i = 0; i < label.length(); i += option.maxNodeLabelPerLine()) {
      int endIdx = Math.min(i + option.maxNodeLabelPerLine(), label.length());
      descParts.add(label.substring(i, endIdx));
    }

    return String.join("\\n", descParts) + (trimmed ? "..." : "");
  }

  boolean highlightNode(RelNode node) {
    Predicate<RelNode> predicate = option.nodePredicate();
    return predicate != null && predicate.test(node);
  }

  /**
   * Options for displaying the rel node plan in dot format.
   */
  @Value.Immutable
  public interface WriteOption {

    /** Default configuration. */
    WriteOption DEFAULT = ImmutableRelDotWriter.WriteOption.of();

    /**
     * The max length of node labels.
     * If the label is too long, the visual display would be messy.
     * -1 means no limit to the label length.
     */
    @Value.Default default int maxNodeLabelLength() {
      return 100;
    }

    /**
     * The max length of node label in a line.
     * -1 means no limitation.
     */
    @Value.Default default int maxNodeLabelPerLine() {
      return 20;
    }

    /**
     * Predicate for nodes that need to be highlighted.
     */
    @Nullable Predicate<RelNode> nodePredicate();
  }
}
