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

import org.apache.calcite.plan.hep.HepRelVertex;
import org.apache.calcite.plan.volcano.RelSubset;
import org.apache.calcite.rel.RelNode;
import org.apache.calcite.rel.RelWriter;
import org.apache.calcite.rel.metadata.RelMetadataQuery;
import org.apache.calcite.sql.SqlExplainLevel;
import org.apache.calcite.util.ImmutableBeans;
import org.apache.calcite.util.Pair;
import org.apache.calcite.util.Util;

import com.google.common.collect.HashMultimap;
import com.google.common.collect.Multimap;

import java.io.PrintWriter;
import java.util.ArrayList;
import java.util.HashMap;
import java.util.LinkedHashMap;
import java.util.List;
import java.util.Map;
import java.util.function.Predicate;
import java.util.stream.Collectors;

/**
 * Utility to dump a rel node plan in dot format.
 */
public class RelDotWriter extends RelWriterImpl {

  //~ Instance fields --------------------------------------------------------

  /**
   * Adjacent list of the plan graph.
   */
  private final Map<RelNode, List<RelNode>> outArcTable = new LinkedHashMap<>();

  private Map<RelNode, String> nodeLabels = new HashMap<>();

  private Multimap<RelNode, String> nodeStyles = HashMultimap.create();

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
                          List<Pair<String, Object>> values) {
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
      List<Pair<String, Object>> values) {
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
      for (Pair<String, Object> value : values) {
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

  private List<RelNode> getInputs(RelNode parent) {
    return parent.getInputs().stream().map(child -> {
      if (child instanceof HepRelVertex) {
        return ((HepRelVertex) child).getCurrentRel();
      } else if (child instanceof RelSubset) {
        RelSubset subset = (RelSubset) child;
        return Util.first(subset.getBest(), subset.getOriginal());
      } else {
        return child;
      }
    }).collect(Collectors.toList());
  }

  private void explainInputs(List<RelNode> inputs) {
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
   * @param label the original label.
   * @param limit the maximal length of the formatted label.
   *              -1 means no limit.
   * @return the formatted label.
   */
  private String formatNodeLabel(String label, int limit) {
    label = label.trim();

    // escape quotes in the label.
    label.replace("\"", "\\\"");

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
    for (int idx = 0; idx < label.length(); idx += option.maxNodeLabelPerLine()) {
      int endIdx = idx + option.maxNodeLabelPerLine() > label.length() ? label.length()
          : idx + option.maxNodeLabelPerLine();
      descParts.add(label.substring(idx, endIdx));
    }

    return String.join("\\n", descParts) + (trimmed ? "..." : "");
  }

  boolean highlightNode(RelNode node) {
    return option.nodePredicate() != null && option.nodePredicate().test(node);
  }

  /**
   * Options for displaying the rel node plan in dot format.
   */
  public interface WriteOption {

    /** Default configuration. */
    WriteOption DEFAULT = ImmutableBeans.create(WriteOption.class);

    /**
     * The max length of node labels.
     * If the label is too long, the visual display would be messy.
     * -1 means no limit to the label length.
     */
    @ImmutableBeans.Property
    @ImmutableBeans.IntDefault(100)
    int maxNodeLabelLength();

    /**
     * The max length of node label in a line.
     * -1 means no limitation.
     */
    @ImmutableBeans.Property
    @ImmutableBeans.IntDefault(20)
    int maxNodeLabelPerLine();

    /**
     * Predicate for nodes that need to be highlighted.
     */
    @ImmutableBeans.Property
    Predicate<RelNode> nodePredicate();
  }
}
