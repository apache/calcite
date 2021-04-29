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

import org.apache.calcite.avatica.util.Spaces;
import org.apache.calcite.rel.RelNode;
import org.apache.calcite.rel.RelVisitor;
import org.apache.calcite.rel.metadata.RelMetadataQuery;
import org.apache.calcite.util.PartiallyOrderedSet;
import org.apache.calcite.util.Util;

import com.google.common.collect.Ordering;

import org.apiguardian.api.API;
import org.checkerframework.checker.nullness.qual.Nullable;

import java.io.PrintWriter;
import java.io.StringWriter;
import java.util.ArrayList;
import java.util.Arrays;
import java.util.Comparator;
import java.util.HashSet;
import java.util.Iterator;
import java.util.List;
import java.util.Map;
import java.util.Set;

import static java.util.Objects.requireNonNull;

/**
 * Utility class to dump state of <code>VolcanoPlanner</code>.
 */
@API(since = "1.23", status = API.Status.INTERNAL)
class Dumpers {

  private Dumpers() {}

  /**
   * Returns a multi-line string describing the provenance of a tree of
   * relational expressions. For each node in the tree, prints the rule that
   * created the node, if any. Recursively describes the provenance of the
   * relational expressions that are the arguments to that rule.
   *
   * <p>Thus, every relational expression and rule invocation that affected
   * the final outcome is described in the provenance. This can be useful
   * when finding the root cause of "mistakes" in a query plan.</p>
   *
   * @param provenanceMap The provenance map
   * @param root Root relational expression in a tree
   * @return Multi-line string describing the rules that created the tree
   */
  static String provenance(
      Map<RelNode, VolcanoPlanner.Provenance> provenanceMap, RelNode root) {
    final StringWriter sw = new StringWriter();
    final PrintWriter pw = new PrintWriter(sw);
    final List<RelNode> nodes = new ArrayList<>();
    new RelVisitor() {
      @Override public void visit(RelNode node, int ordinal, @Nullable RelNode parent) {
        nodes.add(node);
        super.visit(node, ordinal, parent);
      }
      // CHECKSTYLE: IGNORE 1
    }.go(root);
    final Set<RelNode> visited = new HashSet<>();
    for (RelNode node : nodes) {
      provenanceRecurse(provenanceMap, pw, node, 0, visited);
    }
    pw.flush();
    return sw.toString();
  }

  private static void provenanceRecurse(
      Map<RelNode, VolcanoPlanner.Provenance> provenanceMap,
      PrintWriter pw, RelNode node, int i, Set<RelNode> visited) {
    Spaces.append(pw, i * 2);
    if (!visited.add(node)) {
      pw.println("rel#" + node.getId() + " (see above)");
      return;
    }
    pw.println(node);
    final VolcanoPlanner.Provenance o = provenanceMap.get(node);
    Spaces.append(pw, i * 2 + 2);
    if (o == VolcanoPlanner.Provenance.EMPTY) {
      pw.println("no parent");
    } else if (o instanceof VolcanoPlanner.DirectProvenance) {
      RelNode rel = ((VolcanoPlanner.DirectProvenance) o).source;
      pw.println("direct");
      provenanceRecurse(provenanceMap, pw, rel, i + 2, visited);
    } else if (o instanceof VolcanoPlanner.RuleProvenance) {
      VolcanoPlanner.RuleProvenance rule = (VolcanoPlanner.RuleProvenance) o;
      pw.println("call#" + rule.callId + " rule [" + rule.rule + "]");
      for (RelNode rel : rule.rels) {
        provenanceRecurse(provenanceMap, pw, rel, i + 2, visited);
      }
    } else if (o == null && node instanceof RelSubset) {
      // A few operands recognize subsets, not individual rels.
      // The first rel in the subset is deemed to have created it.
      final RelSubset subset = (RelSubset) node;
      pw.println("subset " + subset);
      provenanceRecurse(provenanceMap, pw,
          subset.getRelList().get(0), i + 2, visited);
    } else {
      throw new AssertionError("bad type " + o);
    }
  }

  static void dumpSets(VolcanoPlanner planner, PrintWriter pw) {
    Ordering<RelSet> ordering = Ordering.from(Comparator.comparingInt(o -> o.id));
    for (RelSet set : ordering.immutableSortedCopy(planner.allSets)) {
      pw.println("Set#" + set.id
          + ", type: " + set.subsets.get(0).getRowType());
      int j = -1;
      for (RelSubset subset : set.subsets) {
        ++j;
        pw.println(
            "\t" + subset + ", best="
                + ((subset.best == null) ? "null"
                : ("rel#" + subset.best.getId())));
        assert subset.set == set;
        for (int k = 0; k < j; k++) {
          assert !set.subsets.get(k).getTraitSet().equals(
              subset.getTraitSet());
        }
        for (RelNode rel : subset.getRels()) {
          // "\t\trel#34:JavaProject(rel#32:JavaFilter(...), ...)"
          pw.print("\t\t" + rel);
          for (RelNode input : rel.getInputs()) {
            RelSubset inputSubset =
                planner.getSubset(
                    input,
                    input.getTraitSet());
            if (inputSubset == null) {
              pw.append("no subset found for input ").print(input.getId());
              continue;
            }
            RelSet inputSet = inputSubset.set;
            if (input instanceof RelSubset) {
              final Iterator<RelNode> rels =
                  inputSubset.getRels().iterator();
              if (rels.hasNext()) {
                input = rels.next();
                assert input.getTraitSet().satisfies(inputSubset.getTraitSet());
                assert inputSet.rels.contains(input);
                assert inputSet.subsets.contains(inputSubset);
              }
            }
          }
          if (planner.prunedNodes.contains(rel)) {
            pw.print(", pruned");
          }
          RelMetadataQuery mq = rel.getCluster().getMetadataQuery();
          pw.print(", rowcount=" + mq.getRowCount(rel));
          pw.println(", cumulative cost=" + planner.getCost(rel, mq));
        }
      }
    }
  }

  static void dumpGraphviz(VolcanoPlanner planner, PrintWriter pw) {
    Ordering<RelSet> ordering = Ordering.from(Comparator.comparingInt(o -> o.id));
    Set<RelNode> activeRels = new HashSet<>();
    for (VolcanoRuleCall volcanoRuleCall : planner.ruleCallStack) {
      activeRels.addAll(Arrays.asList(volcanoRuleCall.rels));
    }
    pw.println("digraph G {");
    pw.println("\troot [style=filled,label=\"Root\"];");
    PartiallyOrderedSet<RelSubset> subsetPoset = new PartiallyOrderedSet<>(
        (e1, e2) -> e1.getTraitSet().satisfies(e2.getTraitSet()));
    Set<RelSubset> nonEmptySubsets = new HashSet<>();
    for (RelSet set : ordering.immutableSortedCopy(planner.allSets)) {
      pw.print("\tsubgraph cluster");
      pw.print(set.id);
      pw.println("{");
      pw.print("\t\tlabel=");
      Util.printJavaString(pw, "Set " + set.id + " "
          + set.subsets.get(0).getRowType(), false);
      pw.print(";\n");
      for (RelNode rel : set.rels) {
        pw.print("\t\trel");
        pw.print(rel.getId());
        pw.print(" [label=");
        RelMetadataQuery mq = rel.getCluster().getMetadataQuery();

        // Note: rel traitset could be different from its subset.traitset
        // It can happen due to RelTraitset#simplify
        // If the traits are different, we want to keep them on a graph
        RelSubset relSubset = planner.getSubset(rel);
        if (relSubset == null) {
          pw.append("no subset found for rel");
          continue;
        }
        String traits = "." + relSubset.getTraitSet().toString();
        String title = rel.toString().replace(traits, "");
        if (title.endsWith(")")) {
          int openParen = title.indexOf('(');
          if (openParen != -1) {
            // Title is like rel#12:LogicalJoin(left=RelSubset#4,right=RelSubset#3,
            // condition==($2, $0),joinType=inner)
            // so we remove the parenthesis, and wrap parameters to the second line
            // This avoids "too wide" Graphiz boxes, and makes the graph easier to follow
            title = title.substring(0, openParen) + '\n'
                + title.substring(openParen + 1, title.length() - 1);
          }
        }
        Util.printJavaString(pw,
            title
                + "\nrows=" + mq.getRowCount(rel) + ", cost="
                + planner.getCost(rel, mq), false);
        if (!(rel instanceof AbstractConverter)) {
          nonEmptySubsets.add(relSubset);
        }
        if (relSubset.best == rel) {
          pw.print(",color=blue");
        }
        if (activeRels.contains(rel)) {
          pw.print(",style=dashed");
        }
        pw.print(",shape=box");
        pw.println("]");
      }

      subsetPoset.clear();
      for (RelSubset subset : set.subsets) {
        subsetPoset.add(subset);
        pw.print("\t\tsubset");
        pw.print(subset.getId());
        pw.print(" [label=");
        Util.printJavaString(pw, subset.toString(), false);
        boolean empty = !nonEmptySubsets.contains(subset);
        if (empty) {
          // We don't want to iterate over rels when we know the set is not empty
          for (RelNode rel : subset.getRels()) {
            if (!(rel instanceof AbstractConverter)) {
              empty = false;
              break;
            }
          }
          if (empty) {
            pw.print(",color=red");
          }
        }
        if (activeRels.contains(subset)) {
          pw.print(",style=dashed");
        }
        pw.print("]\n");
      }

      for (RelSubset subset : subsetPoset) {
        List<RelSubset> children = subsetPoset.getChildren(subset);
        if (children == null) {
          continue;
        }
        for (RelSubset parent : children) {
          pw.print("\t\tsubset");
          pw.print(subset.getId());
          pw.print(" -> subset");
          pw.print(parent.getId());
          pw.print(";");
        }
      }

      pw.print("\t}\n");
    }
    // Note: it is important that all the links are declared AFTER declaration of the nodes
    // Otherwise Graphviz creates nodes implicitly, and puts them into a wrong cluster
    pw.print("\troot -> subset");
    pw.print(requireNonNull(planner.root, "planner.root").getId());
    pw.println(";");
    for (RelSet set : ordering.immutableSortedCopy(planner.allSets)) {
      for (RelNode rel : set.rels) {
        RelSubset relSubset = planner.getSubset(rel);
        if (relSubset == null) {
          pw.append("no subset found for rel ").print(rel.getId());
          continue;
        }
        pw.print("\tsubset");
        pw.print(relSubset.getId());
        pw.print(" -> rel");
        pw.print(rel.getId());
        if (relSubset.best == rel) {
          pw.print("[color=blue]");
        }
        pw.print(";");
        List<RelNode> inputs = rel.getInputs();
        for (int i = 0; i < inputs.size(); i++) {
          RelNode input = inputs.get(i);
          pw.print(" rel");
          pw.print(rel.getId());
          pw.print(" -> ");
          pw.print(input instanceof RelSubset ? "subset" : "rel");
          pw.print(input.getId());
          if (relSubset.best == rel || inputs.size() > 1) {
            char sep = '[';
            if (relSubset.best == rel) {
              pw.print(sep);
              pw.print("color=blue");
              sep = ',';
            }
            if (inputs.size() > 1) {
              pw.print(sep);
              pw.print("label=\"");
              pw.print(i);
              pw.print("\"");
              // sep = ',';
            }
            pw.print(']');
          }
          pw.print(";");
        }
        pw.println();
      }
    }

    // Draw lines for current rules
    for (VolcanoRuleCall ruleCall : planner.ruleCallStack) {
      pw.print("rule");
      pw.print(ruleCall.id);
      pw.print(" [style=dashed,label=");
      Util.printJavaString(pw, ruleCall.rule.toString(), false);
      pw.print("]");

      RelNode[] rels = ruleCall.rels;
      for (int i = 0; i < rels.length; i++) {
        RelNode rel = rels[i];
        pw.print(" rule");
        pw.print(ruleCall.id);
        pw.print(" -> ");
        pw.print(rel instanceof RelSubset ? "subset" : "rel");
        pw.print(rel.getId());
        pw.print(" [style=dashed");
        if (rels.length > 1) {
          pw.print(",label=\"");
          pw.print(i);
          pw.print("\"");
        }
        pw.print("]");
        pw.print(";");
      }
      pw.println();
    }

    pw.print("}");
  }
}
