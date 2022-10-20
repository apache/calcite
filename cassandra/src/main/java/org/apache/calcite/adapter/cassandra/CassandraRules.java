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
package org.apache.calcite.adapter.cassandra;

import org.apache.calcite.adapter.enumerable.EnumerableConvention;
import org.apache.calcite.adapter.enumerable.EnumerableLimit;
import org.apache.calcite.plan.Convention;
import org.apache.calcite.plan.RelOptRule;
import org.apache.calcite.plan.RelOptRuleCall;
import org.apache.calcite.plan.RelOptUtil;
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
import org.apache.calcite.rex.RexCall;
import org.apache.calcite.rex.RexInputRef;
import org.apache.calcite.rex.RexNode;
import org.apache.calcite.rex.RexVisitorImpl;
import org.apache.calcite.sql.SqlKind;
import org.apache.calcite.sql.validate.SqlValidatorUtil;

import org.checkerframework.checker.nullness.qual.Nullable;
import org.immutables.value.Value;

import java.util.HashSet;
import java.util.List;
import java.util.Set;

/**
 * Rules and relational operators for
 * {@link CassandraRel#CONVENTION}
 * calling convention.
 */
public class CassandraRules {

  private CassandraRules() {}

  public static final CassandraFilterRule FILTER =
      CassandraFilterRule.Config.DEFAULT.toRule();
  public static final CassandraProjectRule PROJECT =
      CassandraProjectRule.DEFAULT_CONFIG.toRule(CassandraProjectRule.class);
  public static final CassandraSortRule SORT =
      CassandraSortRule.Config.DEFAULT.toRule();
  public static final CassandraLimitRule LIMIT =
      CassandraLimitRule.Config.DEFAULT.toRule();

  /** Rule to convert a relational expression from
   * {@link CassandraRel#CONVENTION} to {@link EnumerableConvention}. */
  public static final CassandraToEnumerableConverterRule TO_ENUMERABLE =
      CassandraToEnumerableConverterRule.DEFAULT_CONFIG
          .toRule(CassandraToEnumerableConverterRule.class);

  @SuppressWarnings("MutablePublicArray")
  protected static final RelOptRule[] RULES = {
      FILTER,
      PROJECT,
      SORT,
      LIMIT
  };

  static List<String> cassandraFieldNames(final RelDataType rowType) {
    return SqlValidatorUtil.uniquify(rowType.getFieldNames(),
        SqlValidatorUtil.EXPR_SUGGESTER, true);
  }

  /** Translator from {@link RexNode} to strings in Cassandra's expression
   * language. */
  static class RexToCassandraTranslator extends RexVisitorImpl<String> {
    private final List<String> inFields;

    protected RexToCassandraTranslator(
        List<String> inFields) {
      super(true);
      this.inFields = inFields;
    }

    @Override public String visitInputRef(RexInputRef inputRef) {
      return inFields.get(inputRef.getIndex());
    }
  }

  /** Base class for planner rules that convert a relational expression to
   * Cassandra calling convention. */
  abstract static class CassandraConverterRule extends ConverterRule {
    CassandraConverterRule(Config config) {
      super(config);
    }
  }

  /**
   * Rule to convert a {@link org.apache.calcite.rel.logical.LogicalFilter} to a
   * {@link CassandraFilter}.
   *
   * @see #FILTER
   */
  public static class CassandraFilterRule
      extends RelRule<CassandraFilterRule.CassandraFilterRuleConfig> {
    /** Creates a CassandraFilterRule. */
    protected CassandraFilterRule(CassandraFilterRuleConfig config) {
      super(config);
    }

    @Override public boolean matches(RelOptRuleCall call) {
      // Get the condition from the filter operation
      LogicalFilter filter = call.rel(0);
      RexNode condition = filter.getCondition();

      // Get field names from the scan operation
      CassandraTableScan scan = call.rel(1);

      List<String> partitionKeys = scan.cassandraTable.getPartitionKeys();
      List<String> clusteringKeys = scan.cassandraTable.getClusteringKeys();
      Set<String> partitionKeysSet = new HashSet<>(scan.cassandraTable.getPartitionKeys());

      List<String> fieldNames = CassandraRules.cassandraFieldNames(filter.getInput().getRowType());

      List<RexNode> disjunctions = RelOptUtil.disjunctions(condition);
      if (disjunctions.size() != 1) {
        return false;
      } else {
        // Check that all conjunctions are primary key equalities
        condition = disjunctions.get(0);
        for (RexNode predicate : RelOptUtil.conjunctions(condition)) {
          if (!isEqualityOnKey(predicate, fieldNames, partitionKeysSet, clusteringKeys)) {
            return false;
          }
        }
      }

      // Either all the partition keys must be specified or none
      return partitionKeysSet.size() == partitionKeys.size() || partitionKeysSet.isEmpty();
    }

    /** Check if the node is a supported predicate (primary key equality).
     *
     * @param node Condition node to check
     * @param fieldNames Names of all columns in the table
     * @param partitionKeys Names of primary key columns
     * @param clusteringKeys Names of primary key columns
     * @return True if the node represents an equality predicate on a primary key
     */
    private static boolean isEqualityOnKey(RexNode node, List<String> fieldNames,
        Set<String> partitionKeys, List<String> clusteringKeys) {
      if (node.getKind() != SqlKind.EQUALS) {
        return false;
      }

      RexCall call = (RexCall) node;
      final RexNode left = call.operands.get(0);
      final RexNode right = call.operands.get(1);
      String key = compareFieldWithLiteral(left, right, fieldNames);
      if (key == null) {
        key = compareFieldWithLiteral(left, right, fieldNames);
      }
      if (key != null) {
        return partitionKeys.remove(key) || clusteringKeys.contains(key);
      } else {
        return false;
      }
    }

    /** Check if an equality operation is comparing a primary key column with a literal.
     *
     * @param left Left operand of the equality
     * @param right Right operand of the equality
     * @param fieldNames Names of all columns in the table
     * @return The field being compared or null if there is no key equality
     */
    private static @Nullable String compareFieldWithLiteral(
        RexNode left, RexNode right, List<String> fieldNames) {
      // FIXME Ignore casts for new and assume they aren't really necessary
      if (left.isA(SqlKind.CAST)) {
        left = ((RexCall) left).getOperands().get(0);
      }

      if (left.isA(SqlKind.INPUT_REF) && right.isA(SqlKind.LITERAL)) {
        RexInputRef left1 = (RexInputRef) left;
        return fieldNames.get(left1.getIndex());
      } else {
        return null;
      }
    }

    @Override public void onMatch(RelOptRuleCall call) {
      LogicalFilter filter = call.rel(0);
      CassandraTableScan scan = call.rel(1);
      if (filter.getTraitSet().contains(Convention.NONE)) {
        final RelNode converted = convert(filter, scan);
        if (converted != null) {
          call.transformTo(converted);
        }
      }
    }

    @Nullable RelNode convert(LogicalFilter filter, CassandraTableScan scan) {
      final RelTraitSet traitSet = filter.getTraitSet().replace(CassandraRel.CONVENTION);
      final List<String> partitionKeys = scan.cassandraTable.getPartitionKeys();
      final List<String> clusteringKeys = scan.cassandraTable.getClusteringKeys();

      return new CassandraFilter(
          filter.getCluster(),
          traitSet,
          convert(filter.getInput(), CassandraRel.CONVENTION),
          filter.getCondition(),
          partitionKeys,
          clusteringKeys,
          scan.cassandraTable.getClusteringOrder());
    }

    /** Deprecated in favor of {@link CassandraFilterRuleConfig}. **/
    @Deprecated
    public interface Config extends CassandraFilterRuleConfig { }

    /** Rule configuration. */
    @Value.Immutable
    public interface CassandraFilterRuleConfig extends RelRule.Config {
      CassandraFilterRuleConfig DEFAULT = ImmutableCassandraFilterRuleConfig.builder()
          .withOperandSupplier(b0 ->
              b0.operand(LogicalFilter.class)
                  .oneInput(b1 -> b1.operand(CassandraTableScan.class)
                      .noInputs()))
          .build();

      @Override default CassandraFilterRule toRule() {
        return new CassandraFilterRule(this);
      }
    }

  }

  /**
   * Rule to convert a {@link org.apache.calcite.rel.logical.LogicalProject}
   * to a {@link CassandraProject}.
   *
   * @see #PROJECT
   */
  public static class CassandraProjectRule extends CassandraConverterRule {
    /** Default configuration. */
    private static final Config DEFAULT_CONFIG = Config.INSTANCE
        .withConversion(LogicalProject.class, Convention.NONE,
            CassandraRel.CONVENTION, "CassandraProjectRule")
        .withRuleFactory(CassandraProjectRule::new);

    protected CassandraProjectRule(Config config) {
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
      return new CassandraProject(project.getCluster(), traitSet,
          convert(project.getInput(), out), project.getProjects(),
          project.getRowType());
    }
  }

  /**
   * Rule to convert a {@link org.apache.calcite.rel.core.Sort} to a
   * {@link CassandraSort}.
   *
   * @see #SORT
   */
  public static class CassandraSortRule
      extends RelRule<CassandraSortRule.CassandraSortRuleConfig> {
    /** Creates a CassandraSortRule. */
    protected CassandraSortRule(CassandraSortRuleConfig config) {
      super(config);
    }

    public RelNode convert(Sort sort, CassandraFilter filter) {
      final RelTraitSet traitSet =
          sort.getTraitSet().replace(CassandraRel.CONVENTION)
              .replace(sort.getCollation());
      return new CassandraSort(sort.getCluster(), traitSet,
          convert(sort.getInput(), traitSet.replace(RelCollations.EMPTY)),
          sort.getCollation());
    }

    @Override public boolean matches(RelOptRuleCall call) {
      final Sort sort = call.rel(0);
      final CassandraFilter filter = call.rel(2);
      return collationsCompatible(sort.getCollation(), filter.getImplicitCollation());
    }

    /** Check if it is possible to exploit native CQL sorting for a given collation.
     *
     * @return True if it is possible to achieve this sort in Cassandra
     */
    private static boolean collationsCompatible(RelCollation sortCollation,
        RelCollation implicitCollation) {
      List<RelFieldCollation> sortFieldCollations = sortCollation.getFieldCollations();
      List<RelFieldCollation> implicitFieldCollations = implicitCollation.getFieldCollations();

      if (sortFieldCollations.size() > implicitFieldCollations.size()) {
        return false;
      }
      if (sortFieldCollations.isEmpty()) {
        return true;
      }

      // Check if we need to reverse the order of the implicit collation
      boolean reversed = sortFieldCollations.get(0).getDirection().reverse().lax()
          == implicitFieldCollations.get(0).getDirection();

      for (int i = 0; i < sortFieldCollations.size(); i++) {
        RelFieldCollation sorted = sortFieldCollations.get(i);
        RelFieldCollation implied = implicitFieldCollations.get(i);

        // Check that the fields being sorted match
        if (sorted.getFieldIndex() != implied.getFieldIndex()) {
          return false;
        }

        // Either all fields must be sorted in the same direction
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
      Sort sort = call.rel(0);
      CassandraFilter filter = call.rel(2);
      call.transformTo(convert(sort, filter));
    }

    /** Deprecated in favor of CassandraSortRuleConfig. **/
    @Deprecated
    public interface Config extends CassandraSortRuleConfig { }

    /** Rule configuration. */
    @Value.Immutable
    public interface CassandraSortRuleConfig extends RelRule.Config {
      CassandraSortRuleConfig DEFAULT = ImmutableCassandraSortRuleConfig.builder()
          .withOperandSupplier(b0 ->
              b0.operand(Sort.class)
                  // Limits are handled by CassandraLimit
                  .predicate(sort ->
                      sort.offset == null && sort.fetch == null)
                  .oneInput(b1 ->
                      b1.operand(CassandraToEnumerableConverter.class)
                          .oneInput(b2 ->
                              b2.operand(CassandraFilter.class)
                                  // We can only use implicit sorting within a
                                  // single partition
                                  .predicate(
                                      CassandraFilter::isSinglePartition)
                                  .anyInputs()))).build();

      @Override default CassandraSortRule toRule() {
        return new CassandraSortRule(this);
      }
    }
  }

  /**
   * Rule to convert a
   * {@link org.apache.calcite.adapter.enumerable.EnumerableLimit} to a
   * {@link CassandraLimit}.
   *
   * @see #LIMIT
   */
  public static class CassandraLimitRule
      extends RelRule<CassandraLimitRule.CassandraLimitRuleConfig> {
    /** Creates a CassandraLimitRule. */
    protected CassandraLimitRule(CassandraLimitRuleConfig config) {
      super(config);
    }

    public RelNode convert(EnumerableLimit limit) {
      final RelTraitSet traitSet =
          limit.getTraitSet().replace(CassandraRel.CONVENTION);
      return new CassandraLimit(limit.getCluster(), traitSet,
        convert(limit.getInput(), CassandraRel.CONVENTION), limit.offset, limit.fetch);
    }

    @Override public void onMatch(RelOptRuleCall call) {
      EnumerableLimit limit = call.rel(0);
      call.transformTo(convert(limit));
    }

    /** Deprecated in favor of CassandraLimitRuleConfig. **/
    @Deprecated
    public interface Config extends CassandraLimitRuleConfig { }

    /** Rule configuration. */
    @Value.Immutable
    public interface CassandraLimitRuleConfig extends RelRule.Config {
      CassandraLimitRuleConfig DEFAULT = ImmutableCassandraLimitRuleConfig.builder()
          .withOperandSupplier(b0 ->
              b0.operand(EnumerableLimit.class)
                  .oneInput(b1 ->
                      b1.operand(CassandraToEnumerableConverter.class)
                          .anyInputs())).build();

      @Override default CassandraLimitRule toRule() {
        return new CassandraLimitRule(this);
      }
    }
  }
}
