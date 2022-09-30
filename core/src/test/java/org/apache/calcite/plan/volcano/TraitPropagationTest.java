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
package org.apache.calcite.plan.volcano;

import org.apache.calcite.adapter.java.JavaTypeFactory;
import org.apache.calcite.config.CalciteSystemProperty;
import org.apache.calcite.jdbc.CalcitePrepare;
import org.apache.calcite.plan.Convention;
import org.apache.calcite.plan.ConventionTraitDef;
import org.apache.calcite.plan.RelOptAbstractTable;
import org.apache.calcite.plan.RelOptCluster;
import org.apache.calcite.plan.RelOptCost;
import org.apache.calcite.plan.RelOptPlanner;
import org.apache.calcite.plan.RelOptRule;
import org.apache.calcite.plan.RelOptRuleCall;
import org.apache.calcite.plan.RelOptSchema;
import org.apache.calcite.plan.RelOptUtil;
import org.apache.calcite.plan.RelRule;
import org.apache.calcite.plan.RelTrait;
import org.apache.calcite.plan.RelTraitSet;
import org.apache.calcite.plan.volcano.AbstractConverter.ExpandConversionRule;
import org.apache.calcite.prepare.CalciteCatalogReader;
import org.apache.calcite.rel.AbstractRelNode;
import org.apache.calcite.rel.RelCollation;
import org.apache.calcite.rel.RelCollationTraitDef;
import org.apache.calcite.rel.RelCollations;
import org.apache.calcite.rel.RelFieldCollation;
import org.apache.calcite.rel.RelNode;
import org.apache.calcite.rel.convert.ConverterRule;
import org.apache.calcite.rel.core.Aggregate;
import org.apache.calcite.rel.core.AggregateCall;
import org.apache.calcite.rel.core.Project;
import org.apache.calcite.rel.core.Sort;
import org.apache.calcite.rel.logical.LogicalAggregate;
import org.apache.calcite.rel.logical.LogicalProject;
import org.apache.calcite.rel.logical.LogicalTableScan;
import org.apache.calcite.rel.metadata.RelMdCollation;
import org.apache.calcite.rel.metadata.RelMetadataQuery;
import org.apache.calcite.rel.rules.CoreRules;
import org.apache.calcite.rel.type.RelDataType;
import org.apache.calcite.rel.type.RelDataTypeFactory;
import org.apache.calcite.rex.RexBuilder;
import org.apache.calcite.rex.RexNode;
import org.apache.calcite.schema.SchemaPlus;
import org.apache.calcite.schema.Statistic;
import org.apache.calcite.schema.Statistics;
import org.apache.calcite.schema.Table;
import org.apache.calcite.schema.impl.AbstractTable;
import org.apache.calcite.server.CalciteServerStatement;
import org.apache.calcite.sql.SqlExplainFormat;
import org.apache.calcite.sql.SqlExplainLevel;
import org.apache.calcite.sql.fun.SqlStdOperatorTable;
import org.apache.calcite.sql.type.SqlTypeName;
import org.apache.calcite.tools.FrameworkConfig;
import org.apache.calcite.tools.Frameworks;
import org.apache.calcite.tools.RuleSet;
import org.apache.calcite.tools.RuleSets;
import org.apache.calcite.util.ImmutableBitSet;

import com.google.common.collect.ImmutableList;
import com.google.common.collect.ImmutableSet;

import org.checkerframework.checker.nullness.qual.Nullable;
import org.immutables.value.Value;
import org.junit.jupiter.api.Test;

import java.sql.Connection;
import java.sql.DriverManager;
import java.util.Collections;
import java.util.List;
import java.util.Properties;

import static org.junit.jupiter.api.Assertions.assertEquals;

/**
 * Tests that determine whether trait propagation work in Volcano Planner.
 */
class TraitPropagationTest {
  static final Convention PHYSICAL =
      new Convention.Impl("PHYSICAL", Phys.class);
  static final RelCollation COLLATION =
      RelCollations.of(
          new RelFieldCollation(0,
              RelFieldCollation.Direction.ASCENDING,
              RelFieldCollation.NullDirection.FIRST));

  static final RuleSet RULES =
      RuleSets.ofList(PhysAggRule.INSTANCE,
          PhysProjRule.INSTANCE,
          PhysTableRule.INSTANCE,
          PhysSortRule.INSTANCE,
          CoreRules.SORT_REMOVE,
          ExpandConversionRule.INSTANCE);

  @Test void testOne() throws Exception {
    RelNode planned = run(new PropAction(), RULES);
    if (CalciteSystemProperty.DEBUG.value()) {
      System.out.println(
          RelOptUtil.dumpPlan("LOGICAL PLAN", planned, SqlExplainFormat.TEXT,
              SqlExplainLevel.ALL_ATTRIBUTES));
    }
    final RelMetadataQuery mq = planned.getCluster().getMetadataQuery();
    assertEquals(3, 0, mq.getCumulativeCost(planned).getRows(),
        "Sortedness was not propagated");
  }

  /**
   * Materialized anonymous class for simplicity.
   */
  private static class PropAction {
    public RelNode apply(RelOptCluster cluster, RelOptSchema relOptSchema,
        SchemaPlus rootSchema) {
      final RelDataTypeFactory typeFactory = cluster.getTypeFactory();
      final RexBuilder rexBuilder = cluster.getRexBuilder();
      final RelOptPlanner planner = cluster.getPlanner();

      final RelDataType stringType = typeFactory.createJavaType(String.class);
      final RelDataType integerType = typeFactory.createJavaType(Integer.class);
      final RelDataType sqlBigInt =
          typeFactory.createSqlType(SqlTypeName.BIGINT);

      // SELECT * from T;
      final Table table = new AbstractTable() {
        public RelDataType getRowType(RelDataTypeFactory typeFactory) {
          return typeFactory.builder()
              .add("s", stringType)
              .add("i", integerType).build();
        }

        @Override public Statistic getStatistic() {
          return Statistics.of(100d, ImmutableList.of(),
              ImmutableList.of(COLLATION));
        }
      };

      final RelOptAbstractTable t1 = new RelOptAbstractTable(relOptSchema,
          "t1", table.getRowType(typeFactory)) {
        @Override public <T> T unwrap(Class<T> clazz) {
          return clazz.isInstance(table)
              ? clazz.cast(table)
              : super.unwrap(clazz);
        }
      };

      final RelNode rt1 = LogicalTableScan.create(cluster, t1, ImmutableList.of());

      // project s column
      RelNode project = LogicalProject.create(rt1,
          ImmutableList.of(),
          ImmutableList.of(
              (RexNode) rexBuilder.makeInputRef(stringType, 0),
              rexBuilder.makeInputRef(integerType, 1)),
          typeFactory.builder().add("s", stringType).add("i", integerType)
              .build(),
          ImmutableSet.of());

      // aggregate on s, count
      AggregateCall aggCall = AggregateCall.create(SqlStdOperatorTable.COUNT,
          false, false, false, Collections.singletonList(1), -1,
          null, RelCollations.EMPTY, sqlBigInt, "cnt");
      RelNode agg = new LogicalAggregate(cluster,
          cluster.traitSetOf(Convention.NONE), ImmutableList.of(), project,
          ImmutableBitSet.of(0), null, Collections.singletonList(aggCall));

      final RelNode rootRel = agg;

      RelOptUtil.dumpPlan("LOGICAL PLAN", rootRel, SqlExplainFormat.TEXT,
          SqlExplainLevel.DIGEST_ATTRIBUTES);

      RelTraitSet desiredTraits = rootRel.getTraitSet().replace(PHYSICAL);
      final RelNode rootRel2 = planner.changeTraits(rootRel, desiredTraits);
      planner.setRoot(rootRel2);
      return planner.findBestExp();
    }
  }

  // RULES

  /** Rule for PhysAgg. */
  public static class PhysAggRule extends RelRule<PhysAggRule.Config> {
    static final PhysAggRule INSTANCE = ImmutablePhysAggRuleConfig.builder()
        .build()
        .withOperandSupplier(b ->
            b.operand(LogicalAggregate.class).anyInputs())
        .withDescription("PhysAgg")
        .as(Config.class)
        .toRule();

    PhysAggRule(Config config) {
      super(config);
    }

    @Override public void onMatch(RelOptRuleCall call) {
      RelTraitSet empty = call.getPlanner().emptyTraitSet();
      LogicalAggregate rel = call.rel(0);
      assert rel.getGroupSet().cardinality() == 1;
      int aggIndex = rel.getGroupSet().iterator().next();
      RelTrait collation = RelCollations.of(
          new RelFieldCollation(aggIndex,
              RelFieldCollation.Direction.ASCENDING,
              RelFieldCollation.NullDirection.FIRST));
      RelTraitSet desiredTraits = empty.replace(PHYSICAL).replace(collation);
      RelNode convertedInput = convert(rel.getInput(), desiredTraits);
      call.transformTo(
          new PhysAgg(rel.getCluster(), empty.replace(PHYSICAL),
              convertedInput, rel.getGroupSet(),
              rel.getGroupSets(), rel.getAggCallList()));
    }

    /** Rule configuration. */
    @Value.Immutable
    @Value.Style(init = "with*", typeImmutable = "ImmutablePhysAggRuleConfig")
    public interface Config extends RelRule.Config {
      @Override default PhysAggRule toRule() {
        return new PhysAggRule(this);
      }
    }
  }

  /** Rule for PhysProj. */
  public static class PhysProjRule extends RelRule<PhysProjRule.Config> {
    static final PhysProjRule INSTANCE =
        ImmutablePhysProjRuleConfig.builder()
            .withSubsetHack(false)
            .build()
            .withOperandSupplier(b0 ->
                b0.operand(LogicalProject.class).oneInput(b1 ->
                    b1.operand(RelNode.class).anyInputs()))
            .withDescription("PhysProj")
            .as(Config.class)
            .toRule();

    protected PhysProjRule(Config config) {
      super(config);
    }

    @Override public void onMatch(RelOptRuleCall call) {
      LogicalProject rel = call.rel(0);
      RelNode rawInput = call.rel(1);
      RelNode input = convert(rawInput, PHYSICAL);

      if (config.subsetHack() && input instanceof RelSubset) {
        RelSubset subset = (RelSubset) input;
        for (RelNode child : subset.getRels()) {
          // skip logical nodes
          if (child.getTraitSet().getTrait(ConventionTraitDef.INSTANCE)
              == Convention.NONE) {
            continue;
          } else {
            RelTraitSet outcome = child.getTraitSet().replace(PHYSICAL);
            call.transformTo(
                new PhysProj(rel.getCluster(), outcome, convert(child, outcome),
                    rel.getProjects(), rel.getRowType()));
          }
        }
      } else {
        call.transformTo(
            PhysProj.create(input, rel.getProjects(), rel.getRowType()));
      }
    }

    /** Rule configuration. */
    @Value.Immutable
    @Value.Style(init = "with*", typeImmutable = "ImmutablePhysProjRuleConfig")
    public interface Config extends RelRule.Config {
      @Override default PhysProjRule toRule() {
        return new PhysProjRule(this);
      }

      boolean subsetHack();

      /** Sets {@link #subsetHack()}. */
      Config withSubsetHack(boolean subsetHack);

    }
  }

  /** Rule for PhysSort. */
  private static class PhysSortRule extends ConverterRule {
    static final PhysSortRule INSTANCE = Config.INSTANCE
        .withConversion(Sort.class, Convention.NONE, PHYSICAL, "PhysSortRule")
        .withRuleFactory(PhysSortRule::new)
        .toRule(PhysSortRule.class);

    PhysSortRule(Config config) {
      super(config);
    }

    @Override public RelNode convert(RelNode rel) {
      final Sort sort = (Sort) rel;
      final RelNode input = convert(sort.getInput(),
          rel.getCluster().traitSetOf(PHYSICAL));
      return new PhysSort(
          rel.getCluster(),
          input.getTraitSet().plus(sort.getCollation()),
          convert(input, input.getTraitSet().replace(PHYSICAL)),
          sort.getCollation(),
          null,
          null);
    }
  }

  /** Rule for PhysTable. */
  public static class PhysTableRule
      extends RelRule<PhysTableRule.Config> {
    static final PhysTableRule INSTANCE = ImmutablePhysTableRuleConfig.builder().build()
        .withOperandSupplier(b ->
            b.operand(LogicalTableScan.class).noInputs())
        .withDescription("PhysScan")
        .as(Config.class)
        .toRule();

    PhysTableRule(Config config) {
      super(config);
    }

    @Override public void onMatch(RelOptRuleCall call) {
      LogicalTableScan rel = call.rel(0);
      call.transformTo(new PhysTable(rel.getCluster()));
    }

    /** Rule configuration. */
    @Value.Immutable
    @Value.Style(init = "with*", typeImmutable = "ImmutablePhysTableRuleConfig")
    public interface Config extends RelRule.Config {
      @Override default PhysTableRule toRule() {
        return new PhysTableRule(this);
      }
    }
  }

  /* RELS */
  /** Market interface for Phys nodes. */
  private interface Phys extends RelNode { }

  /** Physical Aggregate RelNode. */
  private static class PhysAgg extends Aggregate implements Phys {
    PhysAgg(RelOptCluster cluster, RelTraitSet traitSet, RelNode input,
        ImmutableBitSet groupSet,
        List<ImmutableBitSet> groupSets, List<AggregateCall> aggCalls) {
      super(cluster, traitSet, ImmutableList.of(), input, groupSet, groupSets, aggCalls);
    }

    public Aggregate copy(RelTraitSet traitSet, RelNode input,
        ImmutableBitSet groupSet,
        @Nullable List<ImmutableBitSet> groupSets, List<AggregateCall> aggCalls) {
      return new PhysAgg(getCluster(), traitSet, input, groupSet,
          groupSets, aggCalls);
    }

    public @Nullable RelOptCost computeSelfCost(RelOptPlanner planner,
        RelMetadataQuery mq) {
      return planner.getCostFactory().makeCost(1, 1, 1);
    }
  }

  /** Physical Project RelNode. */
  private static class PhysProj extends Project implements Phys {
    PhysProj(RelOptCluster cluster, RelTraitSet traits, RelNode child,
        List<RexNode> exps, RelDataType rowType) {
      super(cluster, traits, ImmutableList.of(), child, exps, rowType, ImmutableSet.of());
    }

    public static PhysProj create(final RelNode input,
        final List<RexNode> projects, RelDataType rowType) {
      final RelOptCluster cluster = input.getCluster();
      final RelMetadataQuery mq = cluster.getMetadataQuery();
      final RelTraitSet traitSet =
          cluster.traitSet().replace(PHYSICAL)
              .replaceIfs(
                  RelCollationTraitDef.INSTANCE,
                  () -> RelMdCollation.project(mq, input, projects));
      return new PhysProj(cluster, traitSet, input, projects, rowType);
    }

    public PhysProj copy(RelTraitSet traitSet, RelNode input,
        List<RexNode> exps, RelDataType rowType) {
      return new PhysProj(getCluster(), traitSet, input, exps, rowType);
    }

    public @Nullable RelOptCost computeSelfCost(RelOptPlanner planner,
        RelMetadataQuery mq) {
      return planner.getCostFactory().makeCost(1, 1, 1);
    }
  }

  /** Physical Sort RelNode. */
  private static class PhysSort extends Sort implements Phys {
    PhysSort(RelOptCluster cluster, RelTraitSet traits, RelNode child,
        RelCollation collation, RexNode offset,
        RexNode fetch) {
      super(cluster, traits, child, collation, offset, fetch);

    }

    public PhysSort copy(RelTraitSet traitSet, RelNode newInput,
        RelCollation newCollation, RexNode offset,
        RexNode fetch) {
      return new PhysSort(getCluster(), traitSet, newInput, newCollation,
          offset, fetch);
    }

    public @Nullable RelOptCost computeSelfCost(RelOptPlanner planner,
        RelMetadataQuery mq) {
      return planner.getCostFactory().makeCost(1, 1, 1);
    }
  }

  /** Physical Table RelNode. */
  private static class PhysTable extends AbstractRelNode implements Phys {
    PhysTable(RelOptCluster cluster) {
      super(cluster, cluster.traitSet().replace(PHYSICAL).replace(COLLATION));
      RelDataTypeFactory typeFactory = cluster.getTypeFactory();
      final RelDataType stringType = typeFactory.createJavaType(String.class);
      final RelDataType integerType = typeFactory.createJavaType(Integer.class);
      this.rowType = typeFactory.builder().add("s", stringType)
          .add("i", integerType).build();
    }

    public @Nullable RelOptCost computeSelfCost(RelOptPlanner planner,
        RelMetadataQuery mq) {
      return planner.getCostFactory().makeCost(1, 1, 1);
    }
  }

  // Created so that we can control when the TraitDefs are defined (e.g.
  // before the cluster is created).
  private static RelNode run(PropAction action, RuleSet rules)
      throws Exception {

    FrameworkConfig config = Frameworks.newConfigBuilder()
        .ruleSets(rules).build();

    final Properties info = new Properties();
    final Connection connection = DriverManager
        .getConnection("jdbc:calcite:", info);
    final CalciteServerStatement statement = connection
        .createStatement().unwrap(CalciteServerStatement.class);
    final CalcitePrepare.Context prepareContext =
          statement.createPrepareContext();
    final JavaTypeFactory typeFactory = prepareContext.getTypeFactory();
    CalciteCatalogReader catalogReader =
          new CalciteCatalogReader(prepareContext.getRootSchema(),
              prepareContext.getDefaultSchemaPath(),
              typeFactory,
              prepareContext.config());
    final RexBuilder rexBuilder = new RexBuilder(typeFactory);
    final RelOptPlanner planner = new VolcanoPlanner(config.getCostFactory(),
        config.getContext());

    // set up rules before we generate cluster
    planner.clearRelTraitDefs();
    planner.addRelTraitDef(RelCollationTraitDef.INSTANCE);
    planner.addRelTraitDef(ConventionTraitDef.INSTANCE);

    planner.clear();
    for (RelOptRule r : rules) {
      planner.addRule(r);
    }

    final RelOptCluster cluster = RelOptCluster.create(planner, rexBuilder);
    return action.apply(cluster, catalogReader,
        prepareContext.getRootSchema().plus());
  }
}
