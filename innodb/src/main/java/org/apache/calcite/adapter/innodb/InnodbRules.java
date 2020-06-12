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
import org.apache.calcite.plan.RelOptRule;
import org.apache.calcite.plan.RelOptRuleCall;
import org.apache.calcite.plan.RelOptRuleOperand;
import org.apache.calcite.plan.RelTraitSet;
import org.apache.calcite.rel.RelCollation;
import org.apache.calcite.rel.RelCollations;
import org.apache.calcite.rel.RelFieldCollation;
import org.apache.calcite.rel.RelNode;
import org.apache.calcite.rel.convert.ConverterRule;
import org.apache.calcite.rel.core.RelFactories;
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

import java.util.List;
import java.util.function.Predicate;

/**
 * Rules and relational operators for {@link InnodbRel#CONVENTION}
 * calling convention.
 */
public class InnodbRules {
  private InnodbRules() {
  }

  /** Rule to convert a relational expression from
   * {@link InnodbRel#CONVENTION} to {@link EnumerableConvention}. */
  public static final ConverterRule TO_ENUMERABLE =
      new InnodbToEnumerableConverterRule(RelFactories.LOGICAL_BUILDER);

  /** Rule to convert a {@link org.apache.calcite.rel.logical.LogicalProject}
   * to a {@link InnodbProject}. */
  public static final InnodbProjectRule PROJECT = new InnodbProjectRule();

  /** Rule to convert a {@link org.apache.calcite.rel.logical.LogicalFilter} to
   * a {@link InnodbFilter}. */
  public static final InnodbFilterRule FILTER = new InnodbFilterRule();

  /** Rule to convert a {@link org.apache.calcite.rel.core.Sort} to a
   * {@link InnodbSort}. */
  public static final InnodbSortFilterRule SORT_FILTER =
      new InnodbSortFilterRule();

  /** Rule to convert a {@link org.apache.calcite.rel.core.Sort} to a
   * {@link InnodbSort} based on InnoDB table clustering index. */
  public static final InnodbSortTableScanRule SORT_SCAN =
      new InnodbSortTableScanRule();

  public static final RelOptRule[] RULES = {
      PROJECT,
      FILTER,
      SORT_FILTER,
      SORT_SCAN
  };

  static List<String> innodbFieldNames(final RelDataType rowType) {
    return SqlValidatorUtil.uniquify(rowType.getFieldNames(),
        SqlValidatorUtil.EXPR_SUGGESTER, true);
  }

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
    protected final Convention out;

    InnodbConverterRule(Class<? extends RelNode> clazz, String description) {
      this(clazz, r -> true, description);
    }

    <R extends RelNode> InnodbConverterRule(Class<R> clazz,
        Predicate<? super R> predicate,
        String description) {
      super(clazz, predicate, Convention.NONE,
          InnodbRel.CONVENTION, RelFactories.LOGICAL_BUILDER, description);
      this.out = InnodbRel.CONVENTION;
    }
  }

  /**
   * Rule to convert a {@link org.apache.calcite.rel.logical.LogicalProject}
   * to a {@link InnodbProject}.
   *
   * @see #PROJECT
   */
  private static class InnodbProjectRule extends InnodbConverterRule {

    private InnodbProjectRule() {
      super(LogicalProject.class, "InnodbProjectRule");
    }

    @Override public boolean matches(RelOptRuleCall call) {
      LogicalProject project = call.rel(0);
      for (RexNode e : project.getProjects()) {
        if (!(e instanceof RexInputRef)) {
          return false;
        }
      }

      return true;
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
  private static class InnodbFilterRule extends RelOptRule {

    private InnodbFilterRule() {
      super(operand(LogicalFilter.class, operand(InnodbTableScan.class, none())),
          "InnodbFilterRule");
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

      TableDef tableDef = scan.innodbTable.getTableDef();
      InnodbFilter innodbFilter = new InnodbFilter(
          filter.getCluster(),
          traitSet,
          convert(filter.getInput(), InnodbRel.CONVENTION),
          filter.getCondition(),
          tableDef,
          scan.getForceIndexName());

      // if some conditions can be pushed down, we left the reminder conditions
      // in the original filter and create a subsidiary filter
      if (innodbFilter.canPushDownCondition()) {
        return LogicalFilter.create(innodbFilter,
            RexUtil.composeConjunction(filter.getCluster().getRexBuilder(),
                innodbFilter.getPushDownCondition().getRemainderConditions()));
      }
      return filter;
    }
  }

  /**
   * Rule to convert a {@link org.apache.calcite.rel.core.Sort} to a
   * {@link InnodbSort}.
   */
  private static class AbstractInnodbSortRule extends RelOptRule {

    AbstractInnodbSortRule(RelOptRuleOperand operand, String description) {
      super(operand, description);
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
  private static class InnodbSortFilterRule extends AbstractInnodbSortRule {

    private InnodbSortFilterRule() {
      super(
          operandJ(Sort.class, null, sort -> true,
              operand(InnodbToEnumerableConverter.class,
                  operandJ(InnodbFilter.class, null, innodbFilter -> true,
                      any()))),
          "InnodbSortFilterRule");
    }

    @Override public boolean matches(RelOptRuleCall call) {
      final Sort sort = call.rel(0);
      final InnodbFilter filter = call.rel(2);
      return collationsCompatible(sort.getCollation(), filter.getImplicitCollation());
    }
  }

  /**
   * Rule to convert a {@link org.apache.calcite.rel.core.Sort} to a
   * {@link InnodbSort} based on InnoDB table clustering index.
   *
   * @see #SORT_SCAN
   */
  private static class InnodbSortTableScanRule extends AbstractInnodbSortRule {

    private InnodbSortTableScanRule() {
      super(
          operandJ(Sort.class, null, sort -> true,
              operand(InnodbToEnumerableConverter.class,
                  operandJ(InnodbTableScan.class, null, tableScan -> true,
                      any()))),
          "InnodbSortTableScanRule");
    }

    @Override public boolean matches(RelOptRuleCall call) {
      final Sort sort = call.rel(0);
      final InnodbTableScan tableScan = call.rel(2);
      return collationsCompatible(sort.getCollation(), tableScan.getImplicitCollation());
    }
  }
}
