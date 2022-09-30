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
package org.apache.calcite.adapter.innodb;

import org.apache.calcite.adapter.enumerable.EnumerableConvention;
import org.apache.calcite.plan.Convention;
import org.apache.calcite.plan.RelOptCluster;
import org.apache.calcite.plan.RelOptRule;
import org.apache.calcite.plan.RelOptRuleCall;
import org.apache.calcite.plan.RelRule;
import org.apache.calcite.plan.RelTraitSet;
import org.apache.calcite.rel.RelCollation;
import org.apache.calcite.rel.RelCollations;
import org.apache.calcite.rel.RelFieldCollation;
import org.apache.calcite.rel.RelNode;
import org.apache.calcite.rel.convert.ConverterRule;
import org.apache.calcite.rel.core.Sort;
import org.apache.calcite.rel.logical.LogicalFilter;
import org.apache.calcite.rel.logical.LogicalProject;
import org.apache.calcite.rel.type.RelDataType;
import org.apache.calcite.rex.RexInputRef;
import org.apache.calcite.rex.RexNode;
import org.apache.calcite.rex.RexUtil;
import org.apache.calcite.rex.RexVisitorImpl;
import org.apache.calcite.sql.validate.SqlValidatorUtil;

import com.alibaba.innodb.java.reader.schema.TableDef;
import com.google.common.collect.ImmutableList;

import org.immutables.value.Value;

import java.util.List;

/**
 * Rules and relational operators for {@link InnodbRel#CONVENTION}
 * calling convention.
 */
public class InnodbRules {
  private InnodbRules() {
  }

  /** Rule to convert a relational expression from
   * {@link InnodbRel#CONVENTION} to {@link EnumerableConvention}. */
  public static final InnodbToEnumerableConverterRule TO_ENUMERABLE =
      InnodbToEnumerableConverterRule.DEFAULT_CONFIG
          .toRule(InnodbToEnumerableConverterRule.class);

  /** Rule to convert a {@link org.apache.calcite.rel.logical.LogicalProject}
   * to a {@link InnodbProject}. */
  public static final InnodbProjectRule PROJECT =
      InnodbProjectRule.DEFAULT_CONFIG.toRule(InnodbProjectRule.class);

  /** Rule to convert a {@link org.apache.calcite.rel.logical.LogicalFilter} to
   * a {@link InnodbFilter}. */
  public static final InnodbFilterRule FILTER =
      InnodbFilterRule.InnodbFilterRuleConfig.DEFAULT.toRule();

  /** Rule to convert a {@link org.apache.calcite.rel.core.Sort} with a
   * {@link org.apache.calcite.rel.core.Filter} to a
   * {@link InnodbSort}. */
  public static final InnodbSortFilterRule SORT_FILTER =
      InnodbSortFilterRule.InnodbSortFilterRuleConfig.DEFAULT.toRule();

  /** Rule to convert a {@link org.apache.calcite.rel.core.Sort} to a
   * {@link InnodbSort} based on InnoDB table clustering index. */
  public static final InnodbSortTableScanRule SORT_SCAN =
      InnodbSortTableScanRule.InnodbSortTableScanRuleConfig.DEFAULT.toRule();

  public static final List<RelOptRule> RULES =
      ImmutableList.of(PROJECT,
          FILTER,
          SORT_FILTER,
          SORT_SCAN);

  static List<String> innodbFieldNames(final RelDataType rowType) {
    return SqlValidatorUtil.uniquify(rowType.getFieldNames(),
        SqlValidatorUtil.EXPR_SUGGESTER, true);
  }

  /** Translator from {@link RexNode} to strings in InnoDB's expression
   * language. */
  static class RexToInnodbTranslator extends RexVisitorImpl<String> {
    private final List<String> inFields;

    protected RexToInnodbTranslator(List<String> inFields) {
      super(true);
      this.inFields = inFields;
    }

    @Override public String visitInputRef(RexInputRef inputRef) {
      return inFields.get(inputRef.getIndex());
    }
  }

  /**
   * Base class for planner rules that convert a relational expression to
   * Innodb calling convention.
   */
  abstract static class InnodbConverterRule extends ConverterRule {
    InnodbConverterRule(Config config) {
      super(config);
    }
  }

  /**
   * Rule to convert a {@link org.apache.calcite.rel.logical.LogicalProject}
   * to a {@link InnodbProject}.
   *
   * @see #PROJECT
   */
  public static class InnodbProjectRule extends InnodbConverterRule {
    /** Default configuration. */
    private static final Config DEFAULT_CONFIG = Config.INSTANCE
        .withConversion(LogicalProject.class, Convention.NONE,
            InnodbRel.CONVENTION, "InnodbProjectRule")
        .withRuleFactory(InnodbProjectRule::new);

    protected InnodbProjectRule(Config config) {
      super(config);
    }

    @Override public boolean matches(RelOptRuleCall call) {
      LogicalProject project = call.rel(0);
      for (RexNode e : project.getProjects()) {
        if (!(e instanceof RexInputRef)) {
          return false;
        }
      }
      return project.getVariablesSet().isEmpty();
    }

    @Override public RelNode convert(RelNode rel) {
      final LogicalProject project = (LogicalProject) rel;
      final RelTraitSet traitSet = project.getTraitSet().replace(out);
      return new InnodbProject(project.getCluster(), traitSet,
          convert(project.getInput(), out), project.getProjects(),
          project.getRowType());
    }
  }

  /**
   * Rule to convert a {@link org.apache.calcite.rel.logical.LogicalFilter} to a
   * {@link InnodbFilter}.
   *
   * @see #FILTER
   */
  public static class InnodbFilterRule extends RelRule<InnodbFilterRule.InnodbFilterRuleConfig> {
    /** Creates a InnodbFilterRule. */
    protected InnodbFilterRule(InnodbFilterRuleConfig config) {
      super(config);
    }

    @Override public void onMatch(RelOptRuleCall call) {
      LogicalFilter filter = call.rel(0);
      InnodbTableScan scan = call.rel(1);
      if (filter.getTraitSet().contains(Convention.NONE)) {
        final RelNode converted = convert(filter, scan);
        if (converted != null) {
          call.transformTo(converted);
        }
      }
    }

    RelNode convert(LogicalFilter filter, InnodbTableScan scan) {
      final RelTraitSet traitSet = filter.getTraitSet().replace(InnodbRel.CONVENTION);

      final TableDef tableDef = scan.innodbTable.getTableDef();
      final RelOptCluster cluster = filter.getCluster();
      final InnodbFilterTranslator translator =
          new InnodbFilterTranslator(cluster.getRexBuilder(),
              filter.getRowType(), tableDef, scan.getForceIndexName());
      final IndexCondition indexCondition =
          translator.translateMatch(filter.getCondition());

      InnodbFilter innodbFilter =
          InnodbFilter.create(cluster, traitSet,
              convert(filter.getInput(), InnodbRel.CONVENTION),
              filter.getCondition(), indexCondition, tableDef,
              scan.getForceIndexName());

      // if some conditions can be pushed down, we left the remainder conditions
      // in the original filter and create a subsidiary filter
      if (innodbFilter.indexCondition.canPushDown()) {
        return LogicalFilter.create(innodbFilter,
            RexUtil.composeConjunction(cluster.getRexBuilder(),
                indexCondition.getRemainderConditions()));
      }
      return filter;
    }

    /** Rule configuration. */
    @Value.Immutable(singleton = false)
    public interface InnodbFilterRuleConfig extends RelRule.Config {
      InnodbFilterRuleConfig DEFAULT = ImmutableInnodbFilterRuleConfig.builder()
          .withOperandSupplier(b0 ->
              b0.operand(LogicalFilter.class)
                  .oneInput(b1 -> b1.operand(InnodbTableScan.class)
                      .noInputs()))
          .build();

      @Override default InnodbFilterRule toRule() {
        return new InnodbFilterRule(this);
      }
    }
  }

  /**
   * Rule to convert a {@link org.apache.calcite.rel.core.Sort} to a
   * {@link InnodbSort}.
   *
   * @param <C> The rule configuration type.
   */
  private static class AbstractInnodbSortRule<C extends RelRule.Config>
      extends RelRule<C> {

    AbstractInnodbSortRule(C config) {
      super(config);
    }

    RelNode convert(Sort sort) {
      final RelTraitSet traitSet =
          sort.getTraitSet().replace(InnodbRel.CONVENTION)
              .replace(sort.getCollation());
      return new InnodbSort(sort.getCluster(), traitSet,
          convert(sort.getInput(), traitSet.replace(RelCollations.EMPTY)),
          sort.getCollation());
    }

    /**
     * Check if it is possible to exploit sorting for a given collation.
     *
     * @return true if it is possible to achieve this sort in Innodb data source
     */
    protected boolean collationsCompatible(RelCollation sortCollation,
        RelCollation implicitCollation) {
      List<RelFieldCollation> sortFieldCollations = sortCollation.getFieldCollations();
      List<RelFieldCollation> implicitFieldCollations = implicitCollation.getFieldCollations();

      if (sortFieldCollations.size() > implicitFieldCollations.size()) {
        return false;
      }
      if (sortFieldCollations.size() == 0) {
        return true;
      }

      // check if we need to reverse the order of the implicit collation
      boolean reversed = sortFieldCollations.get(0).getDirection().reverse().lax()
          == implicitFieldCollations.get(0).getDirection();

      for (int i = 0; i < sortFieldCollations.size(); i++) {
        RelFieldCollation sorted = sortFieldCollations.get(i);
        RelFieldCollation implied = implicitFieldCollations.get(i);

        // check that the fields being sorted match
        if (sorted.getFieldIndex() != implied.getFieldIndex()) {
          return false;
        }

        // either all fields must be sorted in the same direction
        // or the opposite direction based on whether we decided
        // if the sort direction should be reversed above
        RelFieldCollation.Direction sortDirection = sorted.getDirection();
        RelFieldCollation.Direction implicitDirection = implied.getDirection();
        if ((!reversed && sortDirection != implicitDirection)
            || (reversed && sortDirection.reverse().lax() != implicitDirection)) {
          return false;
        }
      }

      return true;
    }

    @Override public void onMatch(RelOptRuleCall call) {
      final Sort sort = call.rel(0);
      final RelNode converted = convert(sort);
      if (converted != null) {
        call.transformTo(converted);
      }
    }
  }

  /**
   * Rule to convert a {@link org.apache.calcite.rel.core.Sort} to a
   * {@link InnodbSort}.
   *
   * @see #SORT_FILTER
   */
  public static class InnodbSortFilterRule
      extends AbstractInnodbSortRule<InnodbSortFilterRule.InnodbSortFilterRuleConfig> {
    /** Creates a InnodbSortFilterRule. */
    protected InnodbSortFilterRule(InnodbSortFilterRuleConfig config) {
      super(config);
    }

    @Override public boolean matches(RelOptRuleCall call) {
      final Sort sort = call.rel(0);
      final InnodbFilter filter = call.rel(2);
      return collationsCompatible(sort.getCollation(), filter.getImplicitCollation());
    }

    /** Rule configuration. */
    @Value.Immutable(singleton = false)
    public interface InnodbSortFilterRuleConfig extends RelRule.Config {
      InnodbSortFilterRuleConfig DEFAULT = ImmutableInnodbSortFilterRuleConfig.builder()
          .withOperandSupplier(b0 ->
              b0.operand(Sort.class)
                  .predicate(sort -> true)
                  .oneInput(b1 ->
                      b1.operand(InnodbToEnumerableConverter.class)
                          .oneInput(b2 ->
                              b2.operand(InnodbFilter.class)
                                  .predicate(innodbFilter -> true)
                                  .anyInputs())))
          .build();

      @Override default InnodbSortFilterRule toRule() {
        return new InnodbSortFilterRule(this);
      }
    }
  }

  /**
   * Rule to convert a {@link org.apache.calcite.rel.core.Sort} to a
   * {@link InnodbSort} based on InnoDB table clustering index.
   *
   * @see #SORT_SCAN
   */
  public static class InnodbSortTableScanRule
      extends AbstractInnodbSortRule<InnodbSortTableScanRule.InnodbSortTableScanRuleConfig> {
    /** Creates a InnodbSortTableScanRule. */
    protected InnodbSortTableScanRule(InnodbSortTableScanRuleConfig config) {
      super(config);
    }

    @Override public boolean matches(RelOptRuleCall call) {
      final Sort sort = call.rel(0);
      final InnodbTableScan tableScan = call.rel(2);
      return collationsCompatible(sort.getCollation(), tableScan.getImplicitCollation());
    }

    /** Rule configuration. */
    @Value.Immutable(singleton = false)
    public interface InnodbSortTableScanRuleConfig extends RelRule.Config {
      InnodbSortTableScanRuleConfig DEFAULT = ImmutableInnodbSortTableScanRuleConfig.builder()
          .withOperandSupplier(b0 ->
              b0.operand(Sort.class)
                  .predicate(sort -> true)
                  .oneInput(b1 ->
                      b1.operand(InnodbToEnumerableConverter.class)
                          .oneInput(b2 ->
                              b2.operand(InnodbTableScan.class)
                                  .predicate(tableScan -> true)
                                  .anyInputs())))
          .build();

      @Override default InnodbSortTableScanRule toRule() {
        return new InnodbSortTableScanRule(this);
      }
    }
  }
}
