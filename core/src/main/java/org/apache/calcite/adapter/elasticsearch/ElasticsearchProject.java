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
package org.apache.calcite.adapter.elasticsearch;

import org.apache.calcite.adapter.java.JavaTypeFactory;
import org.apache.calcite.plan.RelOptCluster;
import org.apache.calcite.plan.RelOptCost;
import org.apache.calcite.plan.RelOptPlanner;
import org.apache.calcite.plan.RelTraitSet;
import org.apache.calcite.rel.RelNode;
import org.apache.calcite.rel.core.Project;
import org.apache.calcite.rel.metadata.RelMetadataQuery;
import org.apache.calcite.rel.type.RelDataType;
import org.apache.calcite.rex.RexNode;
import org.apache.calcite.util.Pair;
import org.apache.calcite.util.Util;

import java.util.ArrayList;
import java.util.List;

/**
 * Implementation of {@link org.apache.calcite.rel.core.Project}
 * relational expression in Elasticsearch.
 */
public class ElasticsearchProject extends Project implements ElasticsearchRel {
  public ElasticsearchProject(RelOptCluster cluster, RelTraitSet traitSet, RelNode input,
      List<? extends RexNode> projects, RelDataType rowType) {
    super(cluster, traitSet, input, projects, rowType);
    assert getConvention() == ElasticsearchRel.CONVENTION;
    assert getConvention() == input.getConvention();
  }

  @Override public Project copy(RelTraitSet relTraitSet, RelNode input, List<RexNode> projects,
      RelDataType relDataType) {
    return new ElasticsearchProject(getCluster(), traitSet, input, projects, relDataType);
  }

  @Override public RelOptCost computeSelfCost(RelOptPlanner planner, RelMetadataQuery mq) {
    return super.computeSelfCost(planner, mq).multiplyBy(0.1);
  }

  @Override public void implement(Implementor implementor) {
    implementor.visitChild(0, getInput());

    final List<String> inFields =
        ElasticsearchRules.elasticsearchFieldNames(getInput().getRowType());
    final ElasticsearchRules.RexToElasticsearchTranslator translator =
        new ElasticsearchRules.RexToElasticsearchTranslator(
            (JavaTypeFactory) getCluster().getTypeFactory(), inFields);

    final List<String> findItems = new ArrayList<>();
    final List<String> scriptFieldItems = new ArrayList<>();
    for (Pair<RexNode, String> pair: getNamedProjects()) {
      final String name = pair.right;
      final String expr = pair.left.accept(translator);

      if (expr.equals("\"" + name + "\"")) {
        findItems.add(ElasticsearchRules.quote(name));
      } else if (expr.matches("\"literal\":.+")) {
        scriptFieldItems.add(ElasticsearchRules.quote(name)
            + ":{\"script\": "
            + expr.split(":")[1] + "}");
      } else {
        scriptFieldItems.add(ElasticsearchRules.quote(name)
            + ":{\"script\":\"params._source."
            + expr.replaceAll("\"", "") + "\"}");
      }
    }
    final String findString = Util.toString(findItems, "", ", ", "");
    final String scriptFieldString = "\"script_fields\": {"
        + Util.toString(scriptFieldItems, "", ", ", "") + "}";
    final String fieldString = "\"_source\" : [" + findString + "]"
        + ", " + scriptFieldString;

    for (String opfield : implementor.list) {
      if (opfield.startsWith("\"_source\"")) {
        implementor.list.remove(opfield);
      }
    }
    implementor.add(fieldString);
  }
}

// End ElasticsearchProject.java
