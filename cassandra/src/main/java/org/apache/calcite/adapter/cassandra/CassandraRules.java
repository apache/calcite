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

import org.apache.calcite.adapter.enumerable.EnumerableLimit;
import org.apache.calcite.adapter.java.JavaTypeFactory;
import org.apache.calcite.plan.Convention;
import org.apache.calcite.plan.RelOptRule;
import org.apache.calcite.plan.RelOptRuleCall;
import org.apache.calcite.plan.RelOptRuleOperand;
import org.apache.calcite.plan.RelOptUtil;
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
import org.apache.calcite.rex.RexCall;
import org.apache.calcite.rex.RexInputRef;
import org.apache.calcite.rex.RexNode;
import org.apache.calcite.rex.RexVisitorImpl;
import org.apache.calcite.runtime.PredicateImpl;
import org.apache.calcite.sql.SqlKind;
import org.apache.calcite.sql.validate.SqlValidatorUtil;
import org.apache.calcite.util.Pair;

import com.google.common.base.Predicate;
import com.google.common.base.Predicates;

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

  public static final RelOptRule[] RULES = {
      CassandraFilterRule.INSTANCE,
      CassandraProjectRule.INSTANCE,
      CassandraSortRule.INSTANCE,
      CassandraLimitRule.INSTANCE
  };

  static List<String> cassandraFieldNames(final RelDataType rowType) {
    return SqlValidatorUtil.uniquify(rowType.getFieldNames(),
        SqlValidatorUtil.EXPR_SUGGESTER, true);
  }

  /** Translator from {@link RexNode} to strings in Cassandra's expression
   * language. */
  static class RexToCassandraTranslator extends RexVisitorImpl<String> {
    private final JavaTypeFactory typeFactory;
    private final List<String> inFields;

    protected RexToCassandraTranslator(JavaTypeFactory typeFactory,
        List<String> inFields) {
      super(true);
      this.typeFactory = typeFactory;
      this.inFields = inFields;
    }

    @Override public String visitInputRef(RexInputRef inputRef) {
      return inFields.get(inputRef.getIndex());
    }
  }

  /** Base class for planner rules that convert a relational expression to
   * Cassandra calling convention. */
  abstract static class CassandraConverterRule extends ConverterRule {
    protected final Convention out;

    CassandraConverterRule(Class<? extends RelNode> clazz,
        String description) {
      this(clazz, Predicates.<RelNode>alwaysTrue(), description);
    }

    <R extends RelNode> CassandraConverterRule(Class<R> clazz,
        Predicate<? super R> predicate,
        String description) {
      super(clazz, predicate, Convention.NONE,
          CassandraRel.CONVENTION, RelFactories.LOGICAL_BUILDER, description);
      this.out = CassandraRel.CONVENTION;
    }
  }

  /**
   * Rule to convert a {@link org.apache.calcite.rel.logical.LogicalFilter} to a
   * {@link CassandraFilter}.
   */
  private static class CassandraFilterRule extends RelOptRule {
    private static final Predicate<LogicalFilter> PREDICATE =
        new PredicateImpl<LogicalFilter>() {
          public boolean test(LogicalFilter input) {
            // TODO: Check for an equality predicate on the partition key
            // Right now this just checks if we have a single top-level AND
            return RelOptUtil.disjunctions(input.getCondition()).size() == 1;
          }
        };

    private static final CassandraFilterRule INSTANCE = new CassandraFilterRule();

    private CassandraFilterRule() {
      super(operand(LogicalFilter.class, operand(CassandraTableScan.class, none())),
          "CassandraFilterRule");
    }

    @Override public boolean matches(RelOptRuleCall call) {
      // Get the condition from the filter operation
      LogicalFilter filter = call.rel(0);
      RexNode condition = filter.getCondition();

      // Get field names from the scan operation
      CassandraTableScan scan = call.rel(1);
      Pair<List<String>, List<String>> keyFields = scan.cassandraTable.getKeyFields();
      Set<String> partitionKeys = new HashSet<String>(keyFields.left);
      List<String> fieldNames = CassandraRules.cassandraFieldNames(filter.getInput().getRowType());

      List<RexNode> disjunctions = RelOptUtil.disjunctions(condition);
      if (disjunctions.size() != 1) {
        return false;
      } else {
        // Check that all conjunctions are primary key equalities
        condition = disjunctions.get(0);
        for (RexNode predicate : RelOptUtil.conjunctions(condition)) {
          if (!isEqualityOnKey(predicate, fieldNames, partitionKeys, keyFields.right)) {
            return false;
          }
        }
      }

      // Either all of the partition keys must be specified or none
      return partitionKeys.size() == keyFields.left.size() || partitionKeys.size() == 0;
    }

    /** Check if the node is a supported predicate (primary key equality).
     *
     * @param node Condition node to check
     * @param fieldNames Names of all columns in the table
     * @param partitionKeys Names of primary key columns
     * @param clusteringKeys Names of primary key columns
     * @return True if the node represents an equality predicate on a primary key
     */
    private boolean isEqualityOnKey(RexNode node, List<String> fieldNames,
        Set<String> partitionKeys, List<String> clusteringKeys) {
      if (node.getKind() != SqlKind.EQUALS) {
        return false;
      }

      RexCall call = (RexCall) node;
      final RexNode left = call.operands.get(0);
      final RexNode right = call.operands.get(1);
      String key = compareFieldWithLiteral(left, right, fieldNames);
      if (key == null) {
        key = compareFieldWithLiteral(right, left, fieldNames);
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
    private String compareFieldWithLiteral(RexNode left, RexNode right, List<String> fieldNames) {
      // FIXME Ignore casts for new and assume they aren't really necessary
      if (left.isA(SqlKind.CAST)) {
        left = ((RexCall) left).getOperands().get(0);
      }

      if (left.isA(SqlKind.INPUT_REF) && right.isA(SqlKind.LITERAL)) {
        final RexInputRef left1 = (RexInputRef) left;
        String name = fieldNames.get(left1.getIndex());
        return name;
      } else {
        return null;
      }
    }

    /** @see org.apache.calcite.rel.convert.ConverterRule */
    public void onMatch(RelOptRuleCall call) {
      LogicalFilter filter = call.rel(0);
      CassandraTableScan scan = call.rel(1);
      if (filter.getTraitSet().contains(Convention.NONE)) {
        final RelNode converted = convert(filter, scan);
        if (converted != null) {
          call.transformTo(converted);
        }
      }
    }

    public RelNode convert(LogicalFilter filter, CassandraTableScan scan) {
      final RelTraitSet traitSet = filter.getTraitSet().replace(CassandraRel.CONVENTION);
      final Pair<List<String>, List<String>> keyFields = scan.cassandraTable.getKeyFields();
      return new CassandraFilter(
          filter.getCluster(),
          traitSet,
          convert(filter.getInput(), CassandraRel.CONVENTION),
          filter.getCondition(),
          keyFields.left,
          keyFields.right,
          scan.cassandraTable.getClusteringOrder());
    }
  }

  /**
   * Rule to convert a {@link org.apache.calcite.rel.logical.LogicalProject}
   * to a {@link CassandraProject}.
   */
  private static class CassandraProjectRule extends CassandraConverterRule {
    private static final CassandraProjectRule INSTANCE = new CassandraProjectRule();

    private CassandraProjectRule() {
      super(LogicalProject.class, "CassandraProjectRule");
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

    public RelNode convert(RelNode rel) {
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
   */
  private static class CassandraSortRule extends RelOptRule {
    private static final Predicate<Sort> SORT_PREDICATE =
        new PredicateImpl<Sort>() {
          public boolean test(Sort input) {
            // Limits are handled by CassandraLimit
            return input.offset == null && input.fetch == null;
          }
        };
    private static final Predicate<CassandraFilter> FILTER_PREDICATE =
        new PredicateImpl<CassandraFilter>() {
          public boolean test(CassandraFilter input) {
            // We can only use implicit sorting within a single partition
            return input.isSinglePartition();
          }
        };
    private static final RelOptRuleOperand CASSANDRA_OP =
        operand(CassandraToEnumerableConverter.class,
        operand(CassandraFilter.class, null, FILTER_PREDICATE, any()));

    private static final CassandraSortRule INSTANCE = new CassandraSortRule();

    private CassandraSortRule() {
      super(operand(Sort.class, null, SORT_PREDICATE, CASSANDRA_OP), "CassandraSortRule");
    }

    public RelNode convert(Sort sort, CassandraFilter filter) {
      final RelTraitSet traitSet =
          sort.getTraitSet().replace(CassandraRel.CONVENTION)
              .replace(sort.getCollation());
      return new CassandraSort(sort.getCluster(), traitSet,
          convert(sort.getInput(), traitSet.replace(RelCollations.EMPTY)),
          sort.getCollation());
    }

    public boolean matches(RelOptRuleCall call) {
      final Sort sort = call.rel(0);
      final CassandraFilter filter = call.rel(2);
      return collationsCompatible(sort.getCollation(), filter.getImplicitCollation());
    }

    /** Check if it is possible to exploit native CQL sorting for a given collation.
     *
     * @return True if it is possible to achieve this sort in Cassandra
     */
    private boolean collationsCompatible(RelCollation sortCollation,
        RelCollation implicitCollation) {
      List<RelFieldCollation> sortFieldCollations = sortCollation.getFieldCollations();
      List<RelFieldCollation> implicitFieldCollations = implicitCollation.getFieldCollations();

      if (sortFieldCollations.size() > implicitFieldCollations.size()) {
        return false;
      }
      if (sortFieldCollations.size() == 0) {
        return true;
      }

      // Check if we need to reverse the order of the implicit collation
      boolean reversed = reverseDirection(sortFieldCollations.get(0).getDirection())
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
            || (reversed && reverseDirection(sortDirection) != implicitDirection)) {
          return false;
        }
      }

      return true;
    }

    /** Find the reverse of a given collation direction.
     *
     * @return Reverse of the input direction
     */
    private RelFieldCollation.Direction reverseDirection(RelFieldCollation.Direction direction) {
      switch (direction) {
      case ASCENDING:
      case STRICTLY_ASCENDING:
        return RelFieldCollation.Direction.DESCENDING;
      case DESCENDING:
      case STRICTLY_DESCENDING:
        return RelFieldCollation.Direction.ASCENDING;
      default:
        return null;
      }
    }

    /** @see org.apache.calcite.rel.convert.ConverterRule */
    public void onMatch(RelOptRuleCall call) {
      final Sort sort = call.rel(0);
      CassandraFilter filter = call.rel(2);
      final RelNode converted = convert(sort, filter);
      if (converted != null) {
        call.transformTo(converted);
      }
    }
  }

  /**
   * Rule to convert a {@link org.apache.calcite.adapter.enumerable.EnumerableLimit} to a
   * {@link CassandraLimit}.
   */
  private static class CassandraLimitRule extends RelOptRule {
    private static final CassandraLimitRule INSTANCE = new CassandraLimitRule();

    private CassandraLimitRule() {
      super(operand(EnumerableLimit.class, operand(CassandraToEnumerableConverter.class, any())),
        "CassandraLimitRule");
    }

    public RelNode convert(EnumerableLimit limit) {
      final RelTraitSet traitSet =
          limit.getTraitSet().replace(CassandraRel.CONVENTION);
      return new CassandraLimit(limit.getCluster(), traitSet,
        convert(limit.getInput(), CassandraRel.CONVENTION), limit.offset, limit.fetch);
    }

    /** @see org.apache.calcite.rel.convert.ConverterRule */
    public void onMatch(RelOptRuleCall call) {
      final EnumerableLimit limit = call.rel(0);
      final RelNode converted = convert(limit);
      if (converted != null) {
        call.transformTo(converted);
      }
    }
  }
}

// End CassandraRules.java
