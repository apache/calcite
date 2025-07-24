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
package org.apache.calcite.adapter.salesforce;

import org.apache.calcite.adapter.enumerable.EnumerableConvention;
import org.apache.calcite.plan.Convention;
import org.apache.calcite.plan.RelOptRule;
import org.apache.calcite.plan.RelOptRuleCall;
import org.apache.calcite.plan.RelTraitSet;
import org.apache.calcite.rel.RelNode;
import org.apache.calcite.rel.convert.ConverterRule;
import org.apache.calcite.rel.core.Filter;
import org.apache.calcite.rel.core.Project;
import org.apache.calcite.rel.core.Sort;
import org.apache.calcite.rel.logical.LogicalFilter;
import org.apache.calcite.rel.logical.LogicalProject;

import com.google.common.collect.ImmutableList;

import java.util.List;

/**
 * Rules and relational operators for {@link SalesforceRel#CONVENTION SALESFORCE}
 * calling convention.
 */
public class SalesforceRules {
  
  private SalesforceRules() {}
  
  /**
   * Rule to convert a {@link SalesforceTableScan} to enumerable convention.
   */
  public static final RelOptRule TO_ENUMERABLE = 
      SalesforceToEnumerableConverterRule.INSTANCE;
  
  /**
   * Rule to push a {@link Filter} into a {@link SalesforceTableScan}.
   */
  public static final RelOptRule FILTER = 
      SalesforceFilterRule.INSTANCE;
  
  /**
   * Rule to push a {@link Project} into a {@link SalesforceTableScan}.
   */
  public static final RelOptRule PROJECT = 
      SalesforceProjectRule.INSTANCE;
  
  /**
   * Rule to push a {@link Sort} into Salesforce.
   */
  public static final RelOptRule SORT = 
      SalesforceSortRule.INSTANCE;
  
  /**
   * List of all Salesforce rules.
   */
  public static final List<RelOptRule> RULES = ImmutableList.of(
      FILTER,
      PROJECT,
      SORT
  );
  
  /**
   * Abstract base class for Salesforce converter rules.
   */
  abstract static class SalesforceConverterRule extends ConverterRule {
    protected SalesforceConverterRule(Config config) {
      super(config);
    }
  }
  
  /**
   * Rule to convert a {@link SalesforceRel} to {@link EnumerableConvention}.
   */
  private static class SalesforceToEnumerableConverterRule extends SalesforceConverterRule {
    
    static final SalesforceToEnumerableConverterRule INSTANCE = Config.INSTANCE
        .withConversion(RelNode.class, SalesforceRel.CONVENTION,
            EnumerableConvention.INSTANCE, "SalesforceToEnumerableConverterRule")
        .withRuleFactory(SalesforceToEnumerableConverterRule::new)
        .toRule(SalesforceToEnumerableConverterRule.class);
    
    protected SalesforceToEnumerableConverterRule(Config config) {
      super(config);
    }
    
    @Override
    public RelNode convert(RelNode rel) {
      return new SalesforceToEnumerableConverter(rel.getCluster(),
          rel.getTraitSet().replace(EnumerableConvention.INSTANCE), rel);
    }
  }
  
  /**
   * Rule to push down filter to Salesforce.
   */
  private static class SalesforceFilterRule extends SalesforceConverterRule {
    
    static final SalesforceFilterRule INSTANCE = Config.INSTANCE
        .withConversion(LogicalFilter.class, Convention.NONE,
            SalesforceRel.CONVENTION, "SalesforceFilterRule")
        .withRuleFactory(SalesforceFilterRule::new)
        .toRule(SalesforceFilterRule.class);
    
    protected SalesforceFilterRule(Config config) {
      super(config);
    }
    
    @Override
    public RelNode convert(RelNode rel) {
      final LogicalFilter filter = (LogicalFilter) rel;
      final RelTraitSet traitSet = filter.getTraitSet().replace(SalesforceRel.CONVENTION);
      return new SalesforceFilter(
          filter.getCluster(),
          traitSet,
          convert(filter.getInput(), SalesforceRel.CONVENTION),
          filter.getCondition());
    }
  }
  
  /**
   * Rule to push down projection to Salesforce.
   */
  private static class SalesforceProjectRule extends SalesforceConverterRule {
    
    static final SalesforceProjectRule INSTANCE = Config.INSTANCE
        .withConversion(LogicalProject.class, Convention.NONE,
            SalesforceRel.CONVENTION, "SalesforceProjectRule")
        .withRuleFactory(SalesforceProjectRule::new)
        .toRule(SalesforceProjectRule.class);
    
    protected SalesforceProjectRule(Config config) {
      super(config);
    }
    
    @Override
    public RelNode convert(RelNode rel) {
      final LogicalProject project = (LogicalProject) rel;
      final RelTraitSet traitSet = project.getTraitSet().replace(SalesforceRel.CONVENTION);
      return new SalesforceProject(
          project.getCluster(),
          traitSet,
          convert(project.getInput(), SalesforceRel.CONVENTION),
          project.getProjects(),
          project.getRowType());
    }
  }
  
  /**
   * Rule to push down sort to Salesforce.
   */
  private static class SalesforceSortRule extends SalesforceConverterRule {
    
    static final SalesforceSortRule INSTANCE = Config.INSTANCE
        .withConversion(Sort.class, Convention.NONE,
            SalesforceRel.CONVENTION, "SalesforceSortRule")
        .withRuleFactory(SalesforceSortRule::new)
        .toRule(SalesforceSortRule.class);
    
    protected SalesforceSortRule(Config config) {
      super(config);
    }
    
    @Override
    public RelNode convert(RelNode rel) {
      final Sort sort = (Sort) rel;
      final RelTraitSet traitSet = sort.getTraitSet().replace(SalesforceRel.CONVENTION);
      return new SalesforceSort(
          sort.getCluster(),
          traitSet,
          convert(sort.getInput(), SalesforceRel.CONVENTION),
          sort.getCollation(),
          sort.offset,
          sort.fetch);
    }
  }
}