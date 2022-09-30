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

import com.google.common.collect.ImmutableList;
import com.google.common.collect.ImmutableSet;

import org.checkerframework.checker.nullness.qual.Nullable;

import java.util.ArrayList;
import java.util.List;
import java.util.stream.Collectors;

/**
 * Implementation of {@link org.apache.calcite.rel.core.Project}
 * relational expression in Elasticsearch.
 */
public class ElasticsearchProject extends Project implements ElasticsearchRel {
  ElasticsearchProject(RelOptCluster cluster, RelTraitSet traitSet, RelNode input,
      List<? extends RexNode> projects, RelDataType rowType) {
    super(cluster, traitSet, ImmutableList.of(), input, projects, rowType, ImmutableSet.of());
    assert getConvention() == ElasticsearchRel.CONVENTION;
    assert getConvention() == input.getConvention();
  }

  @Override public Project copy(RelTraitSet relTraitSet, RelNode input, List<RexNode> projects,
      RelDataType relDataType) {
    return new ElasticsearchProject(getCluster(), traitSet, input, projects, relDataType);
  }

  @Override public @Nullable RelOptCost computeSelfCost(RelOptPlanner planner,
      RelMetadataQuery mq) {
    return super.computeSelfCost(planner, mq).multiplyBy(0.1);
  }

  @Override public void implement(Implementor implementor) {
    implementor.visitChild(0, getInput());

    final List<String> inFields =
            ElasticsearchRules.elasticsearchFieldNames(getInput().getRowType());
    final ElasticsearchRules.RexToElasticsearchTranslator translator =
            new ElasticsearchRules.RexToElasticsearchTranslator(
                    (JavaTypeFactory) getCluster().getTypeFactory(), inFields);

    final List<String> fields = new ArrayList<>();
    final List<String> scriptFields = new ArrayList<>();
    // registers wherever "select *" is present
    boolean hasSelectStar = false;
    for (Pair<RexNode, String> pair: getNamedProjects()) {
      final String name = pair.right;
      final String expr = pair.left.accept(translator);

      // "select *" present?
      hasSelectStar |= ElasticsearchConstants.isSelectAll(name);

      if (ElasticsearchRules.isItem(pair.left)) {
        implementor.addExpressionItemMapping(name, expr);
        fields.add(expr);
      } else if (expr.equals(name)) {
        fields.add(name);
      } else if (expr.matches("\"literal\":.+")) {
        scriptFields.add(ElasticsearchRules.quote(name)
                + ":{\"script\": "
                + expr.split(":")[1] + "}");
      } else {
        scriptFields.add(ElasticsearchRules.quote(name)
                + ":{\"script\":"
                // _source (ES2) vs params._source (ES5)
                + "\"" + implementor.elasticsearchTable.scriptedFieldPrefix() + "."
                + expr.replace("\"", "") + "\"}");
      }
    }

    if (hasSelectStar) {
      // means select * from elastic
      // this does not yet cover select *, _MAP['foo'], _MAP['bar'][0] from elastic
      return;
    }

    final StringBuilder query = new StringBuilder();
    if (scriptFields.isEmpty()) {
      List<String> newList = fields.stream()
          // _id field is available implicitly
          .filter(f -> !ElasticsearchConstants.ID.equals(f))
          .map(ElasticsearchRules::quote)
          .collect(Collectors.toList());

      final String findString = String.join(", ", newList);
      query.append("\"_source\" : [").append(findString).append("]");
    } else {
      // if scripted fields are present, ES ignores _source attribute
      for (String field: fields) {
        scriptFields.add(ElasticsearchRules.quote(field) + ":{\"script\": "
                // _source (ES2) vs params._source (ES5)
                + "\"" + implementor.elasticsearchTable.scriptedFieldPrefix() + "."
                + field + "\"}");
      }
      query.append("\"script_fields\": {" + String.join(", ", scriptFields) + "}");
    }

    implementor.list.removeIf(l -> l.startsWith("\"_source\""));
    implementor.add("{" + query.toString() + "}");
  }
}
