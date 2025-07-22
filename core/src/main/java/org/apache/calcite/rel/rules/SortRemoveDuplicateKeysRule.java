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
package org.apache.calcite.rel.rules;

import org.apache.calcite.plan.RelOptRuleCall;
import org.apache.calcite.plan.RelRule;
import org.apache.calcite.rel.RelCollations;
import org.apache.calcite.rel.RelFieldCollation;
import org.apache.calcite.rel.core.Project;
import org.apache.calcite.rel.core.Sort;
import org.apache.calcite.rel.type.RelDataTypeField;
import org.apache.calcite.rex.RexBuilder;
import org.apache.calcite.rex.RexNode;
import org.apache.calcite.tools.RelBuilder;

import org.immutables.value.Value;

import java.util.ArrayList;
import java.util.HashMap;
import java.util.LinkedHashMap;
import java.util.List;
import java.util.Map;
import java.util.stream.Collectors;

/**
 * Planner rule that remove duplicate sort keys.
 *
 * <p>The original SQL:
 * <pre>{@code
 * SELECT d1 FROM (
 *   SELECT deptno AS d1, deptno AS d2 FROM dept
 * ) AS tmp ORDER BY d1, d2
 * }</pre>
 *
 * <p>The original logical plan:
 * <pre>
 * LogicalProject(D1=[$0])
 *   LogicalSort(sort0=[$0], sort1=[$1], dir0=[ASC], dir1=[ASC])
 *     LogicalProject(D1=[$0], D2=[$0])
 *       LogicalTableScan(table=[[CATALOG, SALES, DEPT]])
 * </pre>
 *
 * <p>After optimization:
 * <pre>
 * LogicalProject(D1=[$0])
 *   LogicalProject(DEPTNO=[$0], DEPTNO0=[$0])
 *     LogicalSort(sort0=[$0], dir0=[ASC])
 *       LogicalProject(DEPTNO=[$0])
 *         LogicalTableScan(table=[[CATALOG, SALES, DEPT]])
 * </pre>
 */
@Value.Enclosing
public class SortRemoveDuplicateKeysRule
    extends RelRule<SortRemoveDuplicateKeysRule.Config>
    implements TransformationRule {

  /** Creates a SortRemoveDuplicateKeysRule. */
  protected SortRemoveDuplicateKeysRule(Config config) {
    super(config);
  }

  @Override public void onMatch(RelOptRuleCall call) {
    Sort sort = call.rel(0);
    Project project = call.rel(1);
    RelBuilder relBuilder = call.builder();

    Map<RexNode, List<Integer>> exprToIndices = new LinkedHashMap<>();
    List<RexNode> projects = project.getProjects();
    for (int i = 0; i < projects.size(); i++) {
      exprToIndices.computeIfAbsent(projects.get(i), k -> new ArrayList<>()).add(i);
    }

    List<RexNode> distinctProjects = new ArrayList<>(exprToIndices.keySet());
    if (distinctProjects.equals(projects)) {
      // No duplicate keys found, no need to transform.
      return;
    }

    Map<Integer, Integer> oldToNewIdx = new HashMap<>();
    int newIdx = 0;
    for (List<Integer> idxList : exprToIndices.values()) {
      for (int oldIdx : idxList) {
        oldToNewIdx.put(oldIdx, newIdx);
      }
      newIdx++;
    }

    List<RelFieldCollation> newCollations =
        sort.getCollation().getFieldCollations().stream()
            .map(fc -> fc.withFieldIndex(oldToNewIdx.get(fc.getFieldIndex())))
            .collect(Collectors.toList());

    List<RexNode> newProjects = new ArrayList<>();
    RexBuilder rexBuilder = sort.getCluster().getRexBuilder();
    for (int i = 0; i < sort.getRowType().getFieldCount(); i++) {
      RelDataTypeField field = sort.getRowType().getFieldList().get(i);
      newProjects.add(rexBuilder.makeInputRef(field.getType(), oldToNewIdx.get(i)));
    }

    relBuilder.push(project.getInput())
        .project(distinctProjects)
        .sort(RelCollations.of(newCollations))
        .project(newProjects);

    call.transformTo(relBuilder.build());
  }

  /** Rule configuration. */
  @Value.Immutable
  public interface Config extends RelRule.Config {
    Config DEFAULT = ImmutableSortRemoveDuplicateKeysRule.Config.of()
        .withOperandSupplier(b0 ->
            b0.operand(Sort.class).oneInput(b1 ->
                b1.operand(Project.class).anyInputs()));

    @Override default SortRemoveDuplicateKeysRule toRule() {
      return new SortRemoveDuplicateKeysRule(this);
    }
  }
}
