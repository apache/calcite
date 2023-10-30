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
package org.apache.calcite.interpreter;

import org.apache.calcite.DataContext;
import org.apache.calcite.adapter.enumerable.AggImplementor;
import org.apache.calcite.adapter.enumerable.RexImpTable;
import org.apache.calcite.linq4j.Enumerable;
import org.apache.calcite.plan.Convention;
import org.apache.calcite.plan.RelOptCluster;
import org.apache.calcite.plan.RelOptCost;
import org.apache.calcite.plan.RelOptPlanner;
import org.apache.calcite.plan.RelOptRule;
import org.apache.calcite.plan.RelOptRuleCall;
import org.apache.calcite.plan.RelOptTable;
import org.apache.calcite.plan.RelRule;
import org.apache.calcite.plan.RelTraitSet;
import org.apache.calcite.rel.InvalidRelException;
import org.apache.calcite.rel.RelCollation;
import org.apache.calcite.rel.RelCollationTraitDef;
import org.apache.calcite.rel.RelNode;
import org.apache.calcite.rel.RelWriter;
import org.apache.calcite.rel.convert.ConverterRule;
import org.apache.calcite.rel.core.Aggregate;
import org.apache.calcite.rel.core.AggregateCall;
import org.apache.calcite.rel.core.CorrelationId;
import org.apache.calcite.rel.core.Filter;
import org.apache.calcite.rel.core.Intersect;
import org.apache.calcite.rel.core.Join;
import org.apache.calcite.rel.core.JoinRelType;
import org.apache.calcite.rel.core.Match;
import org.apache.calcite.rel.core.Minus;
import org.apache.calcite.rel.core.Project;
import org.apache.calcite.rel.core.SetOp;
import org.apache.calcite.rel.core.Sort;
import org.apache.calcite.rel.core.TableScan;
import org.apache.calcite.rel.core.Union;
import org.apache.calcite.rel.core.Values;
import org.apache.calcite.rel.core.Window;
import org.apache.calcite.rel.logical.LogicalAggregate;
import org.apache.calcite.rel.logical.LogicalFilter;
import org.apache.calcite.rel.logical.LogicalIntersect;
import org.apache.calcite.rel.logical.LogicalJoin;
import org.apache.calcite.rel.logical.LogicalMatch;
import org.apache.calcite.rel.logical.LogicalProject;
import org.apache.calcite.rel.logical.LogicalTableScan;
import org.apache.calcite.rel.logical.LogicalUnion;
import org.apache.calcite.rel.logical.LogicalValues;
import org.apache.calcite.rel.logical.LogicalWindow;
import org.apache.calcite.rel.metadata.RelMdCollation;
import org.apache.calcite.rel.metadata.RelMetadataQuery;
import org.apache.calcite.rel.type.RelDataType;
import org.apache.calcite.rel.type.RelDataTypeFactory;
import org.apache.calcite.rel.type.RelDataTypeField;
import org.apache.calcite.rex.RexLiteral;
import org.apache.calcite.rex.RexNode;
import org.apache.calcite.schema.FilterableTable;
import org.apache.calcite.schema.ProjectableFilterableTable;
import org.apache.calcite.schema.ScannableTable;
import org.apache.calcite.schema.Table;
import org.apache.calcite.tools.RelBuilderFactory;
import org.apache.calcite.util.ImmutableBitSet;
import org.apache.calcite.util.ImmutableIntList;

import com.google.common.base.Preconditions;
import com.google.common.collect.ImmutableList;
import com.google.common.collect.ImmutableSet;

import org.checkerframework.checker.nullness.qual.Nullable;
import org.immutables.value.Value;

import java.util.List;
import java.util.Map;
import java.util.Objects;
import java.util.Set;
import java.util.SortedSet;

/**
 * Utilities pertaining to {@link BindableRel} and {@link BindableConvention}.
 */
@Value.Enclosing
public class Bindables {
  private Bindables() {}

  public static final RelOptRule BINDABLE_TABLE_SCAN_RULE =
      BindableTableScanRule.Config.DEFAULT.toRule();

  public static final RelOptRule BINDABLE_FILTER_RULE =
      BindableFilterRule.DEFAULT_CONFIG.toRule(BindableFilterRule.class);

  public static final RelOptRule BINDABLE_PROJECT_RULE =
      BindableProjectRule.DEFAULT_CONFIG.toRule(BindableProjectRule.class);

  public static final RelOptRule BINDABLE_SORT_RULE =
      BindableSortRule.DEFAULT_CONFIG.toRule(BindableSortRule.class);

  public static final RelOptRule BINDABLE_JOIN_RULE =
      BindableJoinRule.DEFAULT_CONFIG.toRule(BindableJoinRule.class);

  public static final RelOptRule BINDABLE_SET_OP_RULE =
      BindableSetOpRule.DEFAULT_CONFIG.toRule(BindableSetOpRule.class);

  // CHECKSTYLE: IGNORE 1
  /** @deprecated Use {@link #BINDABLE_SET_OP_RULE}. */
  @SuppressWarnings("MissingSummary")
  public static final RelOptRule BINDABLE_SETOP_RULE =
      BINDABLE_SET_OP_RULE;

  public static final RelOptRule BINDABLE_VALUES_RULE =
      BindableValuesRule.DEFAULT_CONFIG.toRule(BindableValuesRule.class);

  public static final RelOptRule BINDABLE_AGGREGATE_RULE =
      BindableAggregateRule.DEFAULT_CONFIG.toRule(BindableAggregateRule.class);

  public static final RelOptRule BINDABLE_WINDOW_RULE =
      BindableWindowRule.DEFAULT_CONFIG.toRule(BindableWindowRule.class);

  public static final RelOptRule BINDABLE_MATCH_RULE =
      BindableMatchRule.DEFAULT_CONFIG.toRule(BindableMatchRule.class);

  /** Rule that converts a relational expression from
   * {@link org.apache.calcite.plan.Convention#NONE}
   * to {@link org.apache.calcite.interpreter.BindableConvention}. */
  public static final NoneToBindableConverterRule FROM_NONE_RULE =
      NoneToBindableConverterRule.DEFAULT_CONFIG
          .toRule(NoneToBindableConverterRule.class);

  /** All rules that convert logical relational expression to bindable. */
  public static final ImmutableList<RelOptRule> RULES =
      ImmutableList.of(FROM_NONE_RULE,
          BINDABLE_TABLE_SCAN_RULE,
          BINDABLE_FILTER_RULE,
          BINDABLE_PROJECT_RULE,
          BINDABLE_SORT_RULE,
          BINDABLE_JOIN_RULE,
          BINDABLE_SET_OP_RULE,
          BINDABLE_VALUES_RULE,
          BINDABLE_AGGREGATE_RULE,
          BINDABLE_WINDOW_RULE,
          BINDABLE_MATCH_RULE);

  /** Helper method that converts a bindable relational expression into a
   * record iterator.
   *
   * <p>Any bindable can be compiled; if its input is also bindable, it becomes
   * part of the same compilation unit.
   */
  private static Enumerable<@Nullable Object[]> help(DataContext dataContext,
      BindableRel rel) {
    return new Interpreter(dataContext, rel);
  }

  /** Rule that converts a {@link org.apache.calcite.rel.core.TableScan}
   * to bindable convention.
   *
   * @see #BINDABLE_TABLE_SCAN_RULE */
  public static class BindableTableScanRule
      extends RelRule<BindableTableScanRule.Config> {
    /** Called from Config. */
    protected BindableTableScanRule(Config config) {
      super(config);
    }

    @Deprecated // to be removed before 2.0
    public BindableTableScanRule(RelBuilderFactory relBuilderFactory) {
      this(Config.DEFAULT.withRelBuilderFactory(relBuilderFactory)
          .as(Config.class));
    }

    @Override public void onMatch(RelOptRuleCall call) {
      final LogicalTableScan scan = call.rel(0);
      final RelOptTable table = scan.getTable();
      if (BindableTableScan.canHandle(table)) {
        call.transformTo(
            BindableTableScan.create(scan.getCluster(), table));
      }
    }

    /** Rule configuration. */
    @Value.Immutable
    public interface Config extends RelRule.Config {
      Config DEFAULT = ImmutableBindables.Config.of()
          .withOperandSupplier(b ->
              b.operand(LogicalTableScan.class).noInputs());

      @Override default BindableTableScanRule toRule() {
        return new BindableTableScanRule(this);
      }
    }
  }

  /** Scan of a table that implements {@link ScannableTable} and therefore can
   * be converted into an {@link Enumerable}. */
  public static class BindableTableScan
      extends TableScan implements BindableRel {
    public final ImmutableList<RexNode> filters;
    public final ImmutableIntList projects;

    /** Creates a BindableTableScan.
     *
     * <p>Use {@link #create} unless you know what you are doing. */
    BindableTableScan(RelOptCluster cluster, RelTraitSet traitSet,
        RelOptTable table, ImmutableList<RexNode> filters,
        ImmutableIntList projects) {
      super(cluster, traitSet, ImmutableList.of(), table);
      this.filters = Objects.requireNonNull(filters, "filters");
      this.projects = Objects.requireNonNull(projects, "projects");
      Preconditions.checkArgument(canHandle(table));
    }

    /** Creates a BindableTableScan. */
    public static BindableTableScan create(RelOptCluster cluster,
        RelOptTable relOptTable) {
      return create(cluster, relOptTable, ImmutableList.of(),
          identity(relOptTable));
    }

    /** Creates a BindableTableScan. */
    public static BindableTableScan create(RelOptCluster cluster,
        RelOptTable relOptTable, List<RexNode> filters,
        List<Integer> projects) {
      final Table table = relOptTable.unwrap(Table.class);
      final RelTraitSet traitSet =
          cluster.traitSetOf(BindableConvention.INSTANCE)
              .replaceIfs(RelCollationTraitDef.INSTANCE, () -> {
                if (table != null) {
                  return table.getStatistic().getCollations();
                }
                return ImmutableList.of();
              });
      return new BindableTableScan(cluster, traitSet, relOptTable,
          ImmutableList.copyOf(filters), ImmutableIntList.copyOf(projects));
    }

    @Override public RelDataType deriveRowType() {
      final RelDataTypeFactory.Builder builder =
          getCluster().getTypeFactory().builder();
      final List<RelDataTypeField> fieldList =
          table.getRowType().getFieldList();
      for (int project : projects) {
        builder.add(fieldList.get(project));
      }
      return builder.build();
    }

    @Override public Class<Object[]> getElementType() {
      return Object[].class;
    }

    @Override public RelWriter explainTerms(RelWriter pw) {
      return super.explainTerms(pw)
          .itemIf("filters", filters, !filters.isEmpty())
          .itemIf("projects", projects, !projects.equals(identity()));
    }

    @Override public @Nullable RelOptCost computeSelfCost(RelOptPlanner planner,
        RelMetadataQuery mq) {
      boolean noPushing = filters.isEmpty()
              && projects.size() == table.getRowType().getFieldCount();
      RelOptCost cost = super.computeSelfCost(planner, mq);
      if (noPushing || cost == null) {
        return cost;
      }
      // Cost factor for pushing filters
      double f = filters.isEmpty() ? 1d : 0.5d;

      // Cost factor for pushing fields
      // The "+ 2d" on top and bottom keeps the function fairly smooth.
      double p = ((double) projects.size() + 2d)
          / ((double) table.getRowType().getFieldCount() + 2d);

      // Multiply the cost by a factor that makes a scan more attractive if
      // filters and projects are pushed to the table scan
      return cost.multiplyBy(f * p * 0.01d);
    }

    public static boolean canHandle(RelOptTable table) {
      return table.maybeUnwrap(ScannableTable.class).isPresent()
          || table.maybeUnwrap(FilterableTable.class).isPresent()
          || table.maybeUnwrap(ProjectableFilterableTable.class).isPresent();
    }

    @Override public Enumerable<@Nullable Object[]> bind(DataContext dataContext) {
      return help(dataContext, this);
    }

    @Override public Node implement(InterpreterImplementor implementor) {
      throw new UnsupportedOperationException(); // TODO:
    }
  }

  /** Rule that converts a {@link Filter} to bindable convention.
   *
   * @see #BINDABLE_FILTER_RULE */
  public static class BindableFilterRule extends ConverterRule {
    /** Default configuration. */
    public static final Config DEFAULT_CONFIG = Config.INSTANCE
        .withConversion(LogicalFilter.class, f -> !f.containsOver(),
            Convention.NONE, BindableConvention.INSTANCE,
            "BindableFilterRule")
        .withRuleFactory(BindableFilterRule::new);

    /** Called from the Config. */
    protected BindableFilterRule(Config config) {
      super(config);
    }

    @Override public RelNode convert(RelNode rel) {
      final LogicalFilter filter = (LogicalFilter) rel;
      return BindableFilter.create(
          convert(filter.getInput(),
              filter.getInput().getTraitSet()
                  .replace(BindableConvention.INSTANCE)),
          filter.getCondition());
    }
  }

  /** Implementation of {@link org.apache.calcite.rel.core.Filter}
   * in bindable convention. */
  public static class BindableFilter extends Filter implements BindableRel {
    public BindableFilter(RelOptCluster cluster, RelTraitSet traitSet,
        RelNode input, RexNode condition) {
      super(cluster, traitSet, input, condition);
      assert getConvention() instanceof BindableConvention;
    }

    /** Creates a BindableFilter. */
    public static BindableFilter create(final RelNode input,
        RexNode condition) {
      final RelOptCluster cluster = input.getCluster();
      final RelMetadataQuery mq = cluster.getMetadataQuery();
      final RelTraitSet traitSet =
          cluster.traitSetOf(BindableConvention.INSTANCE)
              .replaceIfs(RelCollationTraitDef.INSTANCE,
                  () -> RelMdCollation.filter(mq, input));
      return new BindableFilter(cluster, traitSet, input, condition);
    }

    @Override public BindableFilter copy(RelTraitSet traitSet, RelNode input,
        RexNode condition) {
      return new BindableFilter(getCluster(), traitSet, input, condition);
    }

    @Override public Class<Object[]> getElementType() {
      return Object[].class;
    }

    @Override public Enumerable<@Nullable Object[]> bind(DataContext dataContext) {
      return help(dataContext, this);
    }

    @Override public Node implement(InterpreterImplementor implementor) {
      return new FilterNode(implementor.compiler, this);
    }
  }

  /**
   * Rule to convert a {@link org.apache.calcite.rel.logical.LogicalProject}
   * to a {@link BindableProject}.
   *
   * @see #BINDABLE_PROJECT_RULE
   */
  public static class BindableProjectRule extends ConverterRule {
    /** Default configuration. */
    public static final Config DEFAULT_CONFIG = Config.INSTANCE
        .withConversion(LogicalProject.class, p -> !p.containsOver(),
            Convention.NONE, BindableConvention.INSTANCE,
            "BindableProjectRule")
        .withRuleFactory(BindableProjectRule::new);

    /** Called from the Config. */
    protected BindableProjectRule(Config config) {
      super(config);
    }

    @Override public boolean matches(RelOptRuleCall call) {
      final LogicalProject project = call.rel(0);
      return project.getVariablesSet().isEmpty();
    }

    @Override public RelNode convert(RelNode rel) {
      final LogicalProject project = (LogicalProject) rel;
      return new BindableProject(rel.getCluster(),
          rel.getTraitSet().replace(BindableConvention.INSTANCE),
          convert(project.getInput(),
              project.getInput().getTraitSet()
                  .replace(BindableConvention.INSTANCE)),
          project.getProjects(),
          project.getRowType());
    }
  }

  /** Implementation of {@link org.apache.calcite.rel.core.Project} in
   * bindable calling convention. */
  public static class BindableProject extends Project implements BindableRel {
    public BindableProject(RelOptCluster cluster, RelTraitSet traitSet,
        RelNode input, List<? extends RexNode> projects, RelDataType rowType) {
      super(cluster, traitSet, ImmutableList.of(), input, projects, rowType, ImmutableSet.of());
      assert getConvention() instanceof BindableConvention;
    }

    @Override public BindableProject copy(RelTraitSet traitSet, RelNode input,
        List<RexNode> projects, RelDataType rowType) {
      return new BindableProject(getCluster(), traitSet, input,
          projects, rowType);
    }

    @Override public Class<Object[]> getElementType() {
      return Object[].class;
    }

    @Override public Enumerable<@Nullable Object[]> bind(DataContext dataContext) {
      return help(dataContext, this);
    }

    @Override public Node implement(InterpreterImplementor implementor) {
      return new ProjectNode(implementor.compiler, this);
    }
  }

  /**
   * Rule to convert an {@link org.apache.calcite.rel.core.Sort} to a
   * {@link org.apache.calcite.interpreter.Bindables.BindableSort}.
   *
   * @see #BINDABLE_SORT_RULE
   */
  public static class BindableSortRule extends ConverterRule {
    /** Default configuration. */
    public static final Config DEFAULT_CONFIG = Config.INSTANCE
        .withConversion(Sort.class, Convention.NONE,
            BindableConvention.INSTANCE, "BindableSortRule")
        .withRuleFactory(BindableSortRule::new);

    /** Called from the Config. */
    protected BindableSortRule(Config config) {
      super(config);
    }

    @Override public RelNode convert(RelNode rel) {
      final Sort sort = (Sort) rel;
      final RelTraitSet traitSet =
          sort.getTraitSet().replace(BindableConvention.INSTANCE);
      final RelNode input = sort.getInput();
      return new BindableSort(rel.getCluster(), traitSet,
          convert(input,
              input.getTraitSet().replace(BindableConvention.INSTANCE)),
          sort.getCollation(), sort.offset, sort.fetch);
    }
  }

  /** Implementation of {@link org.apache.calcite.rel.core.Sort}
   * bindable calling convention. */
  public static class BindableSort extends Sort implements BindableRel {
    public BindableSort(RelOptCluster cluster, RelTraitSet traitSet,
        RelNode input, RelCollation collation, @Nullable RexNode offset, @Nullable RexNode fetch) {
      super(cluster, traitSet, input, collation, offset, fetch);
      assert getConvention() instanceof BindableConvention;
    }

    @Override public BindableSort copy(RelTraitSet traitSet, RelNode newInput,
        RelCollation newCollation, @Nullable RexNode offset, @Nullable RexNode fetch) {
      return new BindableSort(getCluster(), traitSet, newInput, newCollation,
          offset, fetch);
    }

    @Override public Class<Object[]> getElementType() {
      return Object[].class;
    }

    @Override public Enumerable<@Nullable Object[]> bind(DataContext dataContext) {
      return help(dataContext, this);
    }

    @Override public Node implement(InterpreterImplementor implementor) {
      return new SortNode(implementor.compiler, this);
    }
  }

  /**
   * Rule to convert a {@link org.apache.calcite.rel.logical.LogicalJoin}
   * to a {@link BindableJoin}.
   *
   * @see #BINDABLE_JOIN_RULE
   */
  public static class BindableJoinRule extends ConverterRule {
    /** Default configuration. */
    public static final Config DEFAULT_CONFIG = Config.INSTANCE
        .withConversion(LogicalJoin.class, Convention.NONE,
            BindableConvention.INSTANCE, "BindableJoinRule")
        .withRuleFactory(BindableJoinRule::new);

    /** Called from the Config. */
    protected BindableJoinRule(Config config) {
      super(config);
    }

    @Override public RelNode convert(RelNode rel) {
      final LogicalJoin join = (LogicalJoin) rel;
      final BindableConvention out = BindableConvention.INSTANCE;
      final RelTraitSet traitSet = join.getTraitSet().replace(out);
      return new BindableJoin(rel.getCluster(), traitSet,
          convert(join.getLeft(),
              join.getLeft().getTraitSet()
                  .replace(BindableConvention.INSTANCE)),
          convert(join.getRight(),
              join.getRight().getTraitSet()
                  .replace(BindableConvention.INSTANCE)),
          join.getCondition(), join.getVariablesSet(), join.getJoinType());
    }
  }

  /** Implementation of {@link org.apache.calcite.rel.core.Join} in
   * bindable calling convention. */
  public static class BindableJoin extends Join implements BindableRel {
    /** Creates a BindableJoin. */
    protected BindableJoin(RelOptCluster cluster, RelTraitSet traitSet,
        RelNode left, RelNode right, RexNode condition,
        Set<CorrelationId> variablesSet, JoinRelType joinType) {
      super(cluster, traitSet, ImmutableList.of(), left, right,
          condition, variablesSet, joinType);
    }

    @Deprecated // to be removed before 2.0
    protected BindableJoin(RelOptCluster cluster, RelTraitSet traitSet,
        RelNode left, RelNode right, RexNode condition, JoinRelType joinType,
        Set<String> variablesStopped) {
      this(cluster, traitSet, left, right, condition,
          CorrelationId.setOf(variablesStopped), joinType);
    }

    @Override public BindableJoin copy(RelTraitSet traitSet, RexNode conditionExpr,
        RelNode left, RelNode right, JoinRelType joinType,
        boolean semiJoinDone) {
      return new BindableJoin(getCluster(), traitSet, left, right,
          conditionExpr, variablesSet, joinType);
    }

    @Override public Class<Object[]> getElementType() {
      return Object[].class;
    }

    @Override public Enumerable<@Nullable Object[]> bind(DataContext dataContext) {
      return help(dataContext, this);
    }

    @Override public Node implement(InterpreterImplementor implementor) {
      return new JoinNode(implementor.compiler, this);
    }
  }

  /**
   * Rule to convert an {@link SetOp} to a {@link BindableUnion}
   * or {@link BindableIntersect} or {@link BindableMinus}.
   *
   * @see #BINDABLE_SET_OP_RULE
   */
  public static class BindableSetOpRule extends ConverterRule {
    /** Default configuration. */
    public static final Config DEFAULT_CONFIG = Config.INSTANCE
        .withConversion(SetOp.class, Convention.NONE,
            BindableConvention.INSTANCE, "BindableSetOpRule")
        .withRuleFactory(BindableSetOpRule::new);

    /** Called from the Config. */
    protected BindableSetOpRule(Config config) {
      super(config);
    }

    @Override public RelNode convert(RelNode rel) {
      final SetOp setOp = (SetOp) rel;
      final BindableConvention out = BindableConvention.INSTANCE;
      final RelTraitSet traitSet = setOp.getTraitSet().replace(out);
      if (setOp instanceof LogicalUnion) {
        return new BindableUnion(rel.getCluster(), traitSet,
            convertList(setOp.getInputs(), out), setOp.all);
      } else if (setOp instanceof LogicalIntersect) {
        return new BindableIntersect(rel.getCluster(), traitSet,
            convertList(setOp.getInputs(), out), setOp.all);
      } else {
        return new BindableMinus(rel.getCluster(), traitSet,
            convertList(setOp.getInputs(), out), setOp.all);
      }
    }
  }

  /** Implementation of {@link org.apache.calcite.rel.core.Union} in
   * bindable calling convention. */
  public static class BindableUnion extends Union implements BindableRel {
    public BindableUnion(RelOptCluster cluster, RelTraitSet traitSet,
        List<RelNode> inputs, boolean all) {
      super(cluster, traitSet, inputs, all);
    }

    @Override public BindableUnion copy(RelTraitSet traitSet, List<RelNode> inputs,
        boolean all) {
      return new BindableUnion(getCluster(), traitSet, inputs, all);
    }

    @Override public Class<Object[]> getElementType() {
      return Object[].class;
    }

    @Override public Enumerable<@Nullable Object[]> bind(DataContext dataContext) {
      return help(dataContext, this);
    }

    @Override public Node implement(InterpreterImplementor implementor) {
      return new SetOpNode(implementor.compiler, this);
    }
  }

  /** Implementation of {@link org.apache.calcite.rel.core.Intersect} in
   * bindable calling convention. */
  public static class BindableIntersect extends Intersect implements BindableRel {
    public BindableIntersect(RelOptCluster cluster, RelTraitSet traitSet,
        List<RelNode> inputs, boolean all) {
      super(cluster, traitSet, inputs, all);
    }

    @Override public BindableIntersect copy(RelTraitSet traitSet, List<RelNode> inputs,
        boolean all) {
      return new BindableIntersect(getCluster(), traitSet, inputs, all);
    }

    @Override public Class<Object[]> getElementType() {
      return Object[].class;
    }

    @Override public Enumerable<@Nullable Object[]> bind(DataContext dataContext) {
      return help(dataContext, this);
    }

    @Override public Node implement(InterpreterImplementor implementor) {
      return new SetOpNode(implementor.compiler, this);
    }
  }

  /** Implementation of {@link org.apache.calcite.rel.core.Minus} in
   * bindable calling convention. */
  public static class BindableMinus extends Minus implements BindableRel {
    public BindableMinus(RelOptCluster cluster, RelTraitSet traitSet,
        List<RelNode> inputs, boolean all) {
      super(cluster, traitSet, inputs, all);
    }

    @Override public BindableMinus copy(RelTraitSet traitSet, List<RelNode> inputs, boolean all) {
      return new BindableMinus(getCluster(), traitSet, inputs, all);
    }

    @Override public Class<Object[]> getElementType() {
      return Object[].class;
    }

    @Override public Enumerable<@Nullable Object[]> bind(DataContext dataContext) {
      return help(dataContext, this);
    }

    @Override public Node implement(InterpreterImplementor implementor) {
      return new SetOpNode(implementor.compiler, this);
    }
  }

  /** Implementation of {@link org.apache.calcite.rel.core.Values}
   * in bindable calling convention. */
  public static class BindableValues extends Values implements BindableRel {
    BindableValues(RelOptCluster cluster, RelDataType rowType,
        ImmutableList<ImmutableList<RexLiteral>> tuples, RelTraitSet traitSet) {
      super(cluster, rowType, tuples, traitSet);
    }

    @Override public RelNode copy(RelTraitSet traitSet, List<RelNode> inputs) {
      assert inputs.isEmpty();
      return new BindableValues(getCluster(), getRowType(), tuples, traitSet);
    }

    @Override public Class<Object[]> getElementType() {
      return Object[].class;
    }

    @Override public Enumerable<@Nullable Object[]> bind(DataContext dataContext) {
      return help(dataContext, this);
    }

    @Override public Node implement(InterpreterImplementor implementor) {
      return new ValuesNode(implementor.compiler, this);
    }
  }

  /** Rule that converts a {@link Values} to bindable convention.
   *
   * @see #BINDABLE_VALUES_RULE */
  public static class BindableValuesRule extends ConverterRule {
    /** Default configuration. */
    public static final Config DEFAULT_CONFIG = Config.INSTANCE
        .withConversion(LogicalValues.class, Convention.NONE,
            BindableConvention.INSTANCE, "BindableValuesRule")
        .withRuleFactory(BindableValuesRule::new);

    /** Called from the Config. */
    protected BindableValuesRule(Config config) {
      super(config);
    }

    @Override public RelNode convert(RelNode rel) {
      LogicalValues values = (LogicalValues) rel;
      return new BindableValues(values.getCluster(), values.getRowType(),
          values.getTuples(),
          values.getTraitSet().replace(BindableConvention.INSTANCE));
    }
  }

  /** Implementation of {@link org.apache.calcite.rel.core.Aggregate}
   * in bindable calling convention. */
  public static class BindableAggregate extends Aggregate
      implements BindableRel {
    public BindableAggregate(
        RelOptCluster cluster,
        RelTraitSet traitSet,
        RelNode input,
        ImmutableBitSet groupSet,
        @Nullable List<ImmutableBitSet> groupSets,
        List<AggregateCall> aggCalls)
        throws InvalidRelException {
      super(cluster, traitSet, ImmutableList.of(), input, groupSet, groupSets, aggCalls);
      assert getConvention() instanceof BindableConvention;

      for (AggregateCall aggCall : aggCalls) {
        if (aggCall.isDistinct()) {
          throw new InvalidRelException(
              "distinct aggregation not supported");
        }
        if (aggCall.distinctKeys != null) {
          throw new InvalidRelException(
              "within-distinct aggregation not supported");
        }
        AggImplementor implementor2 =
            RexImpTable.INSTANCE.get(aggCall.getAggregation(), false);
        if (implementor2 == null) {
          throw new InvalidRelException(
              "aggregation " + aggCall.getAggregation() + " not supported");
        }
      }
    }

    @Deprecated // to be removed before 2.0
    public BindableAggregate(RelOptCluster cluster, RelTraitSet traitSet,
        RelNode input, boolean indicator, ImmutableBitSet groupSet,
        List<ImmutableBitSet> groupSets, List<AggregateCall> aggCalls)
        throws InvalidRelException {
      this(cluster, traitSet, input, groupSet, groupSets, aggCalls);
      checkIndicator(indicator);
    }

    @Override public BindableAggregate copy(RelTraitSet traitSet, RelNode input,
        ImmutableBitSet groupSet,
        @Nullable List<ImmutableBitSet> groupSets, List<AggregateCall> aggCalls) {
      try {
        return new BindableAggregate(getCluster(), traitSet, input,
            groupSet, groupSets, aggCalls);
      } catch (InvalidRelException e) {
        // Semantic error not possible. Must be a bug. Convert to
        // internal error.
        throw new AssertionError(e);
      }
    }

    @Override public Class<Object[]> getElementType() {
      return Object[].class;
    }

    @Override public Enumerable<@Nullable Object[]> bind(DataContext dataContext) {
      return help(dataContext, this);
    }

    @Override public Node implement(InterpreterImplementor implementor) {
      return new AggregateNode(implementor.compiler, this);
    }
  }

  /** Rule that converts an {@link Aggregate} to bindable convention.
   *
   * @see #BINDABLE_AGGREGATE_RULE */
  public static class BindableAggregateRule extends ConverterRule {
    /** Default configuration. */
    public static final Config DEFAULT_CONFIG = Config.INSTANCE
        .withConversion(LogicalAggregate.class, Convention.NONE,
            BindableConvention.INSTANCE, "BindableAggregateRule")
        .withRuleFactory(BindableAggregateRule::new);

    /** Called from the Config. */
    protected BindableAggregateRule(Config config) {
      super(config);
    }

    @Override public @Nullable RelNode convert(RelNode rel) {
      final LogicalAggregate agg = (LogicalAggregate) rel;
      final RelTraitSet traitSet =
          agg.getTraitSet().replace(BindableConvention.INSTANCE);
      try {
        return new BindableAggregate(rel.getCluster(), traitSet,
            convert(agg.getInput(), traitSet), false, agg.getGroupSet(),
            agg.getGroupSets(), agg.getAggCallList());
      } catch (InvalidRelException e) {
        RelOptPlanner.LOGGER.debug(e.toString());
        return null;
      }
    }
  }

  /** Implementation of {@link org.apache.calcite.rel.core.Window}
   * in bindable convention. */
  public static class BindableWindow extends Window implements BindableRel {
    /** Creates a BindableWindow. */
    BindableWindow(RelOptCluster cluster, RelTraitSet traitSet, RelNode input,
        List<RexLiteral> constants, RelDataType rowType, List<Group> groups) {
      super(cluster, traitSet, input, constants, rowType, groups);
    }

    @Override public RelNode copy(RelTraitSet traitSet, List<RelNode> inputs) {
      return new BindableWindow(getCluster(), traitSet, sole(inputs),
          constants, getRowType(), groups);
    }

    @Override public @Nullable RelOptCost computeSelfCost(RelOptPlanner planner,
        RelMetadataQuery mq) {
      RelOptCost cost = super.computeSelfCost(planner, mq);
      if (cost == null) {
        return null;
      }
      return cost.multiplyBy(BindableConvention.COST_MULTIPLIER);
    }

    @Override public Class<Object[]> getElementType() {
      return Object[].class;
    }

    @Override public Enumerable<@Nullable Object[]> bind(DataContext dataContext) {
      return help(dataContext, this);
    }

    @Override public Node implement(InterpreterImplementor implementor) {
      return new WindowNode(implementor.compiler, this);
    }
  }

  /** Rule to convert a {@link org.apache.calcite.rel.logical.LogicalWindow}
   * to a {@link BindableWindow}.
   *
   * @see #BINDABLE_WINDOW_RULE */
  public static class BindableWindowRule extends ConverterRule {
    /** Default configuration. */
    public static final Config DEFAULT_CONFIG = Config.INSTANCE
        .withConversion(LogicalWindow.class, Convention.NONE,
            BindableConvention.INSTANCE, "BindableWindowRule")
        .withRuleFactory(BindableWindowRule::new);

    /** Called from the Config. */
    protected BindableWindowRule(Config config) {
      super(config);
    }

    @Override public RelNode convert(RelNode rel) {
      final LogicalWindow winAgg = (LogicalWindow) rel;
      final RelTraitSet traitSet =
          winAgg.getTraitSet().replace(BindableConvention.INSTANCE);
      final RelNode input = winAgg.getInput();
      final RelNode convertedInput =
          convert(input,
              input.getTraitSet().replace(BindableConvention.INSTANCE));
      return new BindableWindow(rel.getCluster(), traitSet, convertedInput,
          winAgg.getConstants(), winAgg.getRowType(), winAgg.groups);
    }
  }

  /** Implementation of {@link org.apache.calcite.rel.core.Match}
   * in bindable convention. */
  public static class BindableMatch extends Match implements BindableRel {
    /** Singleton instance of BindableMatch. */
    BindableMatch(RelOptCluster cluster, RelTraitSet traitSet, RelNode input,
        RelDataType rowType, RexNode pattern, boolean strictStart,
        boolean strictEnd, Map<String, RexNode> patternDefinitions,
        Map<String, RexNode> measures, RexNode after,
        Map<String, ? extends SortedSet<String>> subsets, boolean allRows,
        ImmutableBitSet partitionKeys, RelCollation orderKeys,
        @Nullable RexNode interval) {
      super(cluster, traitSet, input, rowType, pattern, strictStart, strictEnd,
          patternDefinitions, measures, after, subsets, allRows, partitionKeys,
          orderKeys, interval);
    }

    @Override public RelNode copy(RelTraitSet traitSet, List<RelNode> inputs) {
      return new BindableMatch(getCluster(), traitSet, inputs.get(0), getRowType(),
          pattern, strictStart, strictEnd, patternDefinitions, measures, after,
          subsets, allRows, partitionKeys, orderKeys, interval);
    }

    @Override public Class<Object[]> getElementType() {
      return Object[].class;
    }

    @Override public Enumerable<@Nullable Object[]> bind(DataContext dataContext) {
      return help(dataContext, this);
    }

    @Override public Node implement(InterpreterImplementor implementor) {
      return new MatchNode(implementor.compiler, this);
    }
  }

  /**
   * Rule to convert a {@link org.apache.calcite.rel.logical.LogicalMatch}
   * to a {@link BindableMatch}.
   *
   * @see #BINDABLE_MATCH_RULE
   */
  public static class BindableMatchRule extends ConverterRule {
    /** Default configuration. */
    public static final Config DEFAULT_CONFIG = Config.INSTANCE
        .withConversion(LogicalMatch.class, Convention.NONE,
            BindableConvention.INSTANCE, "BindableMatchRule")
        .withRuleFactory(BindableMatchRule::new);

    /** Called from the Config. */
    protected BindableMatchRule(Config config) {
      super(config);
    }

    @Override public RelNode convert(RelNode rel) {
      final LogicalMatch match = (LogicalMatch) rel;
      final RelTraitSet traitSet =
          match.getTraitSet().replace(BindableConvention.INSTANCE);
      final RelNode input = match.getInput();
      final RelNode convertedInput =
          convert(input,
              input.getTraitSet().replace(BindableConvention.INSTANCE));
      return new BindableMatch(rel.getCluster(), traitSet, convertedInput,
          match.getRowType(), match.getPattern(), match.isStrictStart(),
          match.isStrictEnd(), match.getPatternDefinitions(),
          match.getMeasures(), match.getAfter(), match.getSubsets(),
          match.isAllRows(), match.getPartitionKeys(), match.getOrderKeys(),
          match.getInterval());
    }
  }

}
