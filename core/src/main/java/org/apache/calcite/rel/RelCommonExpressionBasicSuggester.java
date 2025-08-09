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
package org.apache.calcite.rel;

import org.apache.calcite.plan.CommonRelExpressionRegistry;
import org.apache.calcite.plan.Context;
import org.apache.calcite.plan.Contexts;
import org.apache.calcite.plan.hep.HepPlanner;
import org.apache.calcite.plan.hep.HepProgram;
import org.apache.calcite.plan.hep.HepProgramBuilder;
import org.apache.calcite.rel.rules.CommonRelSubExprRegisterRule;

import org.apiguardian.api.API;
import org.checkerframework.checker.nullness.qual.Nullable;

import java.util.Collection;
import java.util.stream.Collectors;

/**
 * Suggester for common relational expressions that appear as is (identical trees)
 * more than once in the query plan.
 */
@API(since = "1.41.0", status = API.Status.EXPERIMENTAL)
public class RelCommonExpressionBasicSuggester implements RelCommonExpressionSuggester {

  @Override public Collection<RelNode> suggest(RelNode input, @Nullable Context context) {
    CommonRelExpressionRegistry localRegistry = new CommonRelExpressionRegistry();
    HepProgram ruleProgram = new HepProgramBuilder()
        .addRuleInstance(CommonRelSubExprRegisterRule.Config.JOIN.toRule())
        .addRuleInstance(CommonRelSubExprRegisterRule.Config.AGGREGATE.toRule())
        .addRuleInstance(CommonRelSubExprRegisterRule.Config.FILTER.toRule())
        .addRuleInstance(CommonRelSubExprRegisterRule.Config.PROJECT.toRule())
        .build();
    HepPlanner planner = new HepPlanner(ruleProgram, Contexts.of(localRegistry));
    planner.setRoot(input);
    planner.findBestExp();
    return localRegistry.entries().collect(Collectors.toList());
  }

}
