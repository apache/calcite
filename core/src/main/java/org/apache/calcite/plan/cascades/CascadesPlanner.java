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
package org.apache.calcite.plan.cascades;

import org.apache.calcite.config.CalciteSystemProperty;
import org.apache.calcite.plan.AbstractRelOptPlanner;
import org.apache.calcite.plan.Context;
import org.apache.calcite.plan.Convention;
import org.apache.calcite.plan.ConventionTraitDef;
import org.apache.calcite.plan.RelOptCost;
import org.apache.calcite.plan.RelOptCostFactory;
import org.apache.calcite.plan.RelOptRule;
import org.apache.calcite.plan.RelOptUtil;
import org.apache.calcite.plan.RelTrait;
import org.apache.calcite.plan.RelTraitDef;
import org.apache.calcite.plan.RelTraitSet;
import org.apache.calcite.plan.volcano.AbstractConverter;
import org.apache.calcite.rel.RelNode;
import org.apache.calcite.rel.convert.Converter;
import org.apache.calcite.rel.externalize.RelWriterImpl;
import org.apache.calcite.rel.logical.LogicalExchange;
import org.apache.calcite.rel.logical.LogicalSort;
import org.apache.calcite.rel.metadata.RelMetadataQuery;
import org.apache.calcite.rel.type.RelDataType;
import org.apache.calcite.util.Litmus;
import org.apache.calcite.util.Pair;
import org.apache.calcite.util.PartiallyOrderedSet;
import org.apache.calcite.util.Util;

import com.google.common.collect.Ordering;

import java.io.PrintWriter;
import java.io.StringWriter;
import java.util.ArrayDeque;
import java.util.ArrayList;
import java.util.Comparator;
import java.util.Deque;
import java.util.HashMap;
import java.util.HashSet;
import java.util.IdentityHashMap;
import java.util.LinkedHashSet;
import java.util.List;
import java.util.Map;
import java.util.Set;

import static org.apache.calcite.plan.cascades.CascadesUtils.isLogical;
import static org.apache.calcite.sql.SqlExplainLevel.ALL_ATTRIBUTES;

/**
 * Top-down optimizer based on the ideas of Cascades Framework.
 * See the great explanation in a thesis
 * <a href="https://15721.courses.cs.cmu.edu/spring2019/papers/22-optimizer1/xu-columbia-thesis1998.pdf">
 * "Efficiency in the Columbia database query optimizer"</a>.
 */
public class CascadesPlanner extends AbstractRelOptPlanner {

  private static final boolean ASSERTIONS_ENABLED = assertionsAreEnabled();

  private final Deque<CascadesTask> tasksStack = new ArrayDeque<>();

  private final Set<RelOptRule> logicalRules = new LinkedHashSet<>();

  private final Set<RelOptRule> physicalRules = new LinkedHashSet<>();

  private final List<RelGroup> allGroups = new ArrayList<>();

  private final Map<Pair<String, List<RelDataType>>, RelNode> digestToRel =
      new HashMap<>();

  private final IdentityHashMap<RelNode, RelSubGroup> relToSubGroup =
      new IdentityHashMap<>();

  private RelSubGroup root;

  private RelNode originalRoot;

  private RelTraitSet requiredTraits;

  private int nextGroupId = 0;

  private final Set<Pair<RelNode, RelOptRule>> firedRules = new HashSet<>();

  /**
   * Holds the currently registered RelTraitDefs.
   */
  private final List<RelTraitDef> traitDefs = new ArrayList<>();

  private RelTraitSet emptySet;

  private final  Map<RelTraitDef, Enforcer> enforcers = new HashMap<>();

  public CascadesPlanner(RelOptCostFactory costFactory,
      Context context) {
    super(costFactory == null ? CascadesCostImpl.FACTORY : costFactory,
         context);
  }

  @Override public RelNode findBestExp() {
    if (LOGGER.isTraceEnabled()) {
      LOGGER.trace("Initial state:");
      printPlannerStateToLog();
    }
    CascadesTask initialTask =
        new CascadesTask.InitialTask(this, root.getGroup(), requiredTraits);
    submitTask(initialTask);
    int cnt = 0;
    while (!tasksStack.isEmpty()) {
      CascadesTask task = tasksStack.pop();

      if (LOGGER.isDebugEnabled()) {
        LOGGER.debug("Perform task #" + (++cnt) + ": " + task);
      }
      if (LOGGER.isTraceEnabled()) {
        LOGGER.trace("Current stack-trace: " + task.fullToString());
      }

      task.perform();

      if (task.depth > allGroups.size() * 10) {
        throw new AssertionError("Cycle in task calls: " + task.fullToString());
      }

      if (LOGGER.isTraceEnabled()) {
        StringBuilder sb = new StringBuilder("Tasks call stack: \n");
        for (CascadesTask t0 : tasksStack) {
          sb.append("--").append(t0).append("\n");
        }
        LOGGER.trace(sb.toString());
      }
    }

    RelNode bestExp = root.buildCheapestPlan(this);

    if (LOGGER.isTraceEnabled()) {
      LOGGER.trace("Best rel: " +  RelOptUtil.toString(bestExp, ALL_ATTRIBUTES));
    }

    return bestExp;
  }

  @Override public RelSubGroup register(RelNode rel, RelNode equivRel) {
    if (ASSERTIONS_ENABLED) {
      assert rel != null;
      assert !isRegistered(rel);
      checkSameType(rel, equivRel);
    }

    RelGroup group = null;
    if (equivRel != null) {
      group = getGroup(equivRel);
    }

    return registerInGroup(rel, group);
  }

  private RelSubGroup registerInGroup(RelNode rel, RelGroup group) {
    if (ASSERTIONS_ENABLED) {
      checkRelState(rel);
    }

    if (rel instanceof RelSubGroup) {
      return registerSubGroup((RelSubGroup) rel, group);
    }

    // Do not register Sort and other converters because all trait
    // conversions will be added during optimization
    if (rel instanceof LogicalSort) {
      rel = ((LogicalSort) rel).getInput();
    } else if (rel instanceof LogicalExchange) {
      rel = ((LogicalExchange) rel).getInput();
    }

    // Register inputs and compute digest.
    rel = rel.onRegister(this);

    // Deduplication: If it is equivalent to an existing expression, return the group that
    // the equivalent expression belongs to.
    Pair<String, List<RelDataType>> key = key(rel);
    RelNode equivExp = digestToRel.get(key);
    if (equivExp != null) {
      if (equivExp == rel) {
        return getSubGroup(equivExp);
      } else {
        checkSameType(rel, equivExp);
        RelGroup equivGroup = getGroup(equivExp);
        if (equivGroup != null) {
          return registerSubGroup(getSubGroup(equivExp), equivGroup);
        }
      }
    }

    if (group == null) {
      group = new RelGroup(nextGroupId++, rel.getRowType());
      allGroups.add(group);
    }

    registerClass(rel);

    RelSubGroup subGroup = addRelToGroup(rel, group);

    digestToRel.put(key, rel);

    if (LOGGER.isTraceEnabled() && root != null) {
      LOGGER.trace("New relNode registered: " + rel + " in " + subGroup);
      printPlannerStateToLog();
    }
    return subGroup;
  }

  private RelSubGroup addRelToGroup(RelNode rel, RelGroup group) {
    RelSubGroup subGroup = group.add(rel);
    relToSubGroup.put(rel, subGroup);
    return subGroup;
  }

  private RelSubGroup registerSubGroup(RelSubGroup rel, RelGroup group) {
    // TODO merge groups
    return rel;
  }

  RelGroup getGroup(RelNode rel) {
    assert rel != null;
    RelSubGroup subGroup = getSubGroup(rel);
    return subGroup == null ? null : subGroup.getGroup();
  }

  private RelSubGroup getSubGroup(RelNode rel) {
    assert rel != null;
    return rel instanceof RelSubGroup ? (RelSubGroup) rel : relToSubGroup.get(rel);
  }

  private void checkSameType(RelNode rel, RelNode equivRel) {
    assert equivRel == null
        || RelOptUtil.equal("rel rowtype",
        rel.getRowType(),
        "equivRel rowtype",
        equivRel.getRowType(),
        Litmus.THROW);
  }

  private void checkRelState(RelNode rel) {
    assert rel.getCluster().getPlanner() == this;
    final RelTraitSet traits = rel.getTraitSet();
    final Convention convention = traits.getTrait(ConventionTraitDef.INSTANCE);
    assert convention != null;
    if (!convention.getInterface().isInstance(rel)
        && !(rel instanceof Converter)) {
      throw new AssertionError("Relational expression " + rel
          + " has calling-convention " + convention
          + " but does not implement the required interface '"
          + convention.getInterface() + "' of that convention");
    }
    if (traits.size() != traitDefs.size()) {
      throw new AssertionError("Relational expression " + rel
          + " does not have the correct number of traits: " + traits.size()
          + " != " + traitDefs.size());
    }
  }

  /**
   * Returns whether assertions are enabled in this class.
   */
  private static boolean assertionsAreEnabled() {
    boolean assertionsEnabled = false;
    //noinspection AssertWithSideEffects
    assert assertionsEnabled = true;
    return assertionsEnabled;
  }

  @Override public RelSubGroup ensureRegistered(RelNode rel, RelNode equivRel) {
    RelSubGroup result;
    final RelSubGroup subGroup = getSubGroup(rel);
    if (subGroup != null) {
      if (equivRel != null) {
        final RelSubGroup equivSubset = getSubGroup(equivRel);
//        if (subGroup.set != equivSubset.set) { TODO merge??
//          merge(equivSubset.set, subGroup.set);
//        }
      }
      result = subGroup;
    } else {
      result = register(rel, equivRel);
    }

    // Checking if tree is valid considerably slows down planning
    // Only doing it if logger level is debug or finer
//    if (LOGGER.isDebugEnabled()) { TODO validity check
//      assert isValid(Litmus.THROW);
//    }

    return result;
  }

  @Override public boolean isRegistered(RelNode rel) {
    return relToSubGroup.get(rel) != null;
  }

  @Override public void setRoot(RelNode rel) {
    this.root = ensureRegistered(rel, null);

    if (this.originalRoot == null) {
      this.originalRoot = rel;
    }

    requiredTraits = rel.getTraitSet();
  }

  @Override public RelNode getRoot() {
    return root;
  }

  @Override public List<RelOptRule> getRules() {
    List<RelOptRule> ruleList = new ArrayList<>(physicalRules.size() + logicalRules.size());
    ruleList.addAll(physicalRules);
    ruleList.addAll(logicalRules);
    return ruleList;
  }

  @Override public boolean addRule(RelOptRule rule) {
    if (rule instanceof ImplementationRule) {
      return physicalRules.add(rule);
    } else {
      return logicalRules.add(rule);
    }
  }

  public <T extends RelTrait> void addEnforcer(Enforcer<T> enforcer) {
    enforcers.put(enforcer.traitDef(), enforcer);
  }


  public <T extends RelTrait> RelSubGroup enforce(RelNode rel, T toTrait) {
    RelTraitDef<T> traitDef = toTrait.getTraitDef();
    Enforcer<T> enforcer = enforcers.get(traitDef);
    T fromTrait = rel.getTraitSet().getTrait(traitDef);

    if (fromTrait.satisfies(toTrait)) {
      return getSubGroup(rel);
    }
    // TODO composite traits handling
    if (!traitDef.canConvert(this, fromTrait, toTrait, rel)) {
      return null;
    }

    RelNode enforcedRel = enforcer.enforce(rel, toTrait);

    if (enforcedRel == null) {
      return null;
    }

    return register(enforcedRel, rel);
  }

  @Override public boolean removeRule(RelOptRule rule) {
    throw new UnsupportedOperationException("TODO"); // TODO: CODE: implement.
  }

  public Set<RelOptRule> logicalRules() {
    return logicalRules;
  }

  public Set<RelOptRule> physicalRules() {
    return physicalRules;
  }

  @Override public RelNode changeTraits(RelNode rel, RelTraitSet toTraits) {
    assert !rel.getTraitSet().equals(toTraits);
    assert toTraits.allSimple();

    RelSubGroup subGroup = ensureRegistered(rel, null);
    if (subGroup.getTraitSet().equals(toTraits)) {
      return subGroup;
    }

    // TODO Several groups for each instance of multi trait?
    return subGroup.getGroup().getOrCreateSubGroup(rel.getCluster(), toTraits);
  }

  @Override public boolean addRelTraitDef(RelTraitDef relTraitDef) {
    emptySet = null;
    return !traitDefs.contains(relTraitDef) && traitDefs.add(relTraitDef);
  }

  @Override public RelTraitSet emptyTraitSet() {
    if (emptySet != null) {
      return emptySet;
    }

    RelTraitSet traitSet = super.emptyTraitSet();
    for (RelTraitDef traitDef : traitDefs) {
      traitSet = traitSet.plus(traitDef.getDefault());
    }
    emptySet = traitSet;
    return traitSet;
  }

  @Override public List<RelTraitDef> getRelTraitDefs() {
    return traitDefs;
  }

  void submitTask(CascadesTask task) {
    tasksStack.push(task);
  }

  public RelOptCost getCost(RelNode rel, RelMetadataQuery mq) {
    assert rel != null : "pre-condition: rel != null";
    if (rel instanceof RelSubGroup) {
      RelNode cheapest = ((RelSubGroup) rel).cheapestSoFar();
      RelOptCost cost;
      cost = cheapest == null
          ? costFactory.makeInfiniteCost()
          : ((RelSubGroup) rel).cheapestCostSoFar();
      return cost;
    }

    if (isLogical(rel)) {
      return costFactory.makeInfiniteCost();
    }

    RelOptCost cost = mq.getNonCumulativeCost(rel);
    if (!costFactory.makeZeroCost().isLt(cost)) {
      // cost must be positive, so nudge it
      cost = costFactory.makeTinyCost();
    }
    for (RelNode input : rel.getInputs()) {
      cost = cost.plus(getCost(input, mq));
    }
    return cost;
  }

  boolean isFired(RelOptRule rule, RelNode rel) {
    return firedRules.contains(new Pair<>(rel, rule));
  }

  void addFired(RelOptRule rule, RelNode rel) {
    boolean added = firedRules.add(new Pair<>(rel, rule));
    assert added;
  }

  private void printPlannerStateToLog() {
    StringWriter sw = new StringWriter();
    PrintWriter pw = new PrintWriter(sw);
    dump(pw);
    LOGGER.trace(sw.toString());
    LOGGER.trace("Root=" + root);
  }

  /**
   * Dumps the internal state of this VolcanoPlanner to a writer.
   *
   * @param pw Print writer
   */
  public void dump(PrintWriter pw) {
    pw.println("Root: " + root);
    pw.println("Original rel:");

    if (originalRoot != null) {
      originalRoot.explain(
          new RelWriterImpl(pw, ALL_ATTRIBUTES, false));
    }

    try {
//      if (CalciteSystemProperty.DUMP_SETS.value()) { TODO
//        pw.println();
//        pw.println("Groups:");
//        dumpSets(pw);
//      }
      if (CalciteSystemProperty.DUMP_GRAPHVIZ.value()) {
        pw.println();
        pw.println("Graphviz:");
        dumpGraphviz(pw);
      }
    } catch (Exception | AssertionError e) {
      pw.println("Error when dumping plan state: \n"
          + e);
    }
  }

  private void dumpGraphviz(PrintWriter pw) {
    Ordering<RelGroup> ordering = Ordering.from(Comparator.comparingInt(o -> o.getId()));
    Set<RelNode> activeRels = new HashSet<>();
    pw.println("digraph G {");
    pw.println("\troot [style=filled,label=\"Root\"];");
    PartiallyOrderedSet<RelSubGroup> subGroupPoSet = new PartiallyOrderedSet<>(
        (e1, e2) -> e1.getTraitSet().satisfies(e2.getTraitSet()));
    Set<RelSubGroup> nonEmptySubGroups = new HashSet<>();
    for (RelGroup group : ordering.immutableSortedCopy(allGroups)) {
      pw.print("\tsubgraph cluster");
      pw.print(group.getId());
      pw.println("{");
      pw.print("\t\tlabel=");
      Util.printJavaString(pw, "Group " + group.getId() + " " + group.rowType(), false);
      pw.print(";\n");

      // TODO print logical and physical nodes in different manner
      for (RelNode rel : group.getRels()) {
        pw.print("\t\trel");
        pw.print(rel.getId());
        pw.print(" [label=");
        RelMetadataQuery mq = rel.getCluster().getMetadataQuery();

        // Note: rel traitset could be different from its subset.traitset
        // It can happen due to RelTraitset#simplify
        // If the traits are different, we want to keep them on a graph
        String traits = "." + getSubGroup(rel).getTraitSet().toString();
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
                + "\nrows=" + mq.getRowCount(rel) + ", cost=" + getCost(rel, mq), false);
        RelSubGroup subGroup = getSubGroup(rel);
        if (!(rel instanceof AbstractConverter)) {
          nonEmptySubGroups.add(subGroup);
        }
        if (subGroup.cheapestSoFar() == rel) {
          pw.print(",color=blue");
        }
        if (activeRels.contains(rel)) {
          pw.print(",style=dashed");
        }
        pw.print(",shape=box");
        pw.println("]");
      }

      subGroupPoSet.clear();
      for (RelSubGroup subGroup : group.allSubGroups()) {
        subGroupPoSet.add(subGroup);
        pw.print("\t\tsubGroup");
        pw.print(subGroup.getId());
        pw.print(" [label=");
        Util.printJavaString(pw, subGroup.toString(), false);
        boolean empty = !nonEmptySubGroups.contains(subGroup);
        if (empty) {
          // We don't want to iterate over rels when we know the group is not empty
          for (RelNode rel : subGroup.getRels()) {
            if (!(rel instanceof AbstractConverter)) {
              empty = false;
              break;
            }
          }
          if (empty) {
            pw.print(",color=red");
          }
        }
        if (activeRels.contains(subGroup)) {
          pw.print(",style=dashed");
        }
        pw.print("]\n");
      }

      for (RelSubGroup subGroup : subGroupPoSet) {
        for (RelSubGroup parent : subGroupPoSet.getChildren(subGroup)) {
          pw.print("\t\tsubGroup");
          pw.print(subGroup.getId());
          pw.print(" -> subGroup");
          pw.print(parent.getId());
          pw.print(";");
        }
      }

      pw.print("\t}\n");
    }
    // Note: it is important that all the links are declared AFTER declaration of the nodes
    // Otherwise Graphviz creates nodes implicitly, and puts them into a wrong cluster
    pw.print("\troot -> subGroup");
    pw.print(root.getId());
    pw.println(";");
    for (RelGroup group : ordering.immutableSortedCopy(allGroups)) {
      for (RelNode rel : group.getRels()) {
        RelSubGroup subGroup = getSubGroup(rel);
        pw.print("\tsubGroup");
        pw.print(subGroup.getId());
        pw.print(" -> rel");
        pw.print(rel.getId());
        if (subGroup.cheapestSoFar() == rel) {
          pw.print("[color=blue]");
        }
        pw.print(";");
        List<RelNode> inputs = rel.getInputs();
        for (int i = 0; i < inputs.size(); i++) {
          RelNode input = inputs.get(i);
          pw.print(" rel");
          pw.print(rel.getId());
          pw.print(" -> ");
          pw.print(input instanceof RelSubGroup ? "subGroup" : "rel");
          pw.print(input.getId());
          if (subGroup.cheapestSoFar() == rel || inputs.size() > 1) {
            char sep = '[';
            if (subGroup.cheapestSoFar() == rel) {
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

//    // Draw lines for current rules
//    for (VolcanoRuleCall ruleCall : ruleCallStack) {
//      pw.print("rule");
//      pw.print(ruleCall.id);
//      pw.print(" [style=dashed,label=");
//      Util.printJavaString(pw, ruleCall.rule.toString(), false);
//      pw.print("]");
//
//      RelNode[] rels = ruleCall.rels;
//      for (int i = 0; i < rels.length; i++) {
//        RelNode rel = rels[i];
//        pw.print(" rule");
//        pw.print(ruleCall.id);
//        pw.print(" -> ");
//        pw.print(rel instanceof RelSubset ? "subset" : "rel");
//        pw.print(rel.getId());
//        pw.print(" [style=dashed");
//        if (rels.length > 1) {
//          pw.print(",label=\"");
//          pw.print(i);
//          pw.print("\"");
//        }
//        pw.print("]");
//        pw.print(";");
//      }
//      pw.println();
//    }

    pw.print("}");
  }
}
