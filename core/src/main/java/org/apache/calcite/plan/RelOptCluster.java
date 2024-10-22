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
package org.apache.calcite.plan;

import org.apache.calcite.rel.RelNode;
import org.apache.calcite.rel.core.CorrelationId;
import org.apache.calcite.rel.hint.HintStrategyTable;
import org.apache.calcite.rel.metadata.DefaultRelMetadataProvider;
import org.apache.calcite.rel.metadata.JaninoRelMetadataProvider;
import org.apache.calcite.rel.metadata.MetadataFactory;
import org.apache.calcite.rel.metadata.RelMetadataProvider;
import org.apache.calcite.rel.metadata.RelMetadataQuery;
import org.apache.calcite.rel.metadata.RelMetadataQueryBase;
import org.apache.calcite.rel.type.RelDataTypeFactory;
import org.apache.calcite.rex.RexBuilder;
import org.apache.calcite.rex.RexNode;

import org.checkerframework.checker.initialization.qual.UnknownInitialization;
import org.checkerframework.checker.nullness.qual.EnsuresNonNull;
import org.checkerframework.checker.nullness.qual.Nullable;

import java.util.HashMap;
import java.util.Map;
import java.util.concurrent.atomic.AtomicInteger;
import java.util.function.Supplier;

import static org.apache.calcite.linq4j.Nullness.castNonNull;

import static java.util.Objects.requireNonNull;

/**
 * An environment for related relational expressions during the
 * optimization of a query.
 */
public class RelOptCluster {
  //~ Instance fields --------------------------------------------------------

  private final RelDataTypeFactory typeFactory;
  private final RelOptPlanner planner;
  private final AtomicInteger nextCorrel;
  private final Map<String, RelNode> mapCorrelToRel;
  private RexNode originalExpression;
  private final RexBuilder rexBuilder;
  private RelMetadataProvider metadataProvider;
  @Deprecated // to be removed before 2.0
  private MetadataFactory metadataFactory;
  private @Nullable HintStrategyTable hintStrategies;
  private final RelTraitSet emptyTraitSet;
  private @Nullable RelMetadataQuery mq;
  private Supplier<RelMetadataQuery> mqSupplier;

  //~ Constructors -----------------------------------------------------------

  /**
   * Creates a cluster.
   */
  @Deprecated // to be removed before 2.0
  RelOptCluster(
      RelOptQuery query,
      RelOptPlanner planner,
      RelDataTypeFactory typeFactory,
      RexBuilder rexBuilder) {
    this(planner, typeFactory, rexBuilder, query.nextCorrel,
        query.mapCorrelToRel);
  }

  /**
   * Creates a cluster.
   *
   * <p>For use only from {@link #create} and {@link RelOptQuery}.
   */
  RelOptCluster(RelOptPlanner planner, RelDataTypeFactory typeFactory,
      RexBuilder rexBuilder, AtomicInteger nextCorrel,
      Map<String, RelNode> mapCorrelToRel) {
    this.nextCorrel = nextCorrel;
    this.mapCorrelToRel = mapCorrelToRel;
    this.planner = requireNonNull(planner, "planner");
    this.typeFactory = requireNonNull(typeFactory, "typeFactory");
    this.rexBuilder = rexBuilder;
    this.originalExpression = rexBuilder.makeLiteral("?");

    // set up a default rel metadata provider,
    // giving the planner first crack at everything
    setMetadataProvider(DefaultRelMetadataProvider.INSTANCE);
    setMetadataQuerySupplier(RelMetadataQuery::instance);
    this.emptyTraitSet = planner.emptyTraitSet();
    assert emptyTraitSet.size() == planner.getRelTraitDefs().size();
  }

  /** Creates a cluster. */
  public static RelOptCluster create(RelOptPlanner planner,
      RexBuilder rexBuilder) {
    return new RelOptCluster(planner, rexBuilder.getTypeFactory(),
        rexBuilder, new AtomicInteger(0), new HashMap<>());
  }

  //~ Methods ----------------------------------------------------------------

  @Deprecated // to be removed before 2.0
  public RelOptQuery getQuery() {
    return new RelOptQuery(castNonNull(planner), nextCorrel, mapCorrelToRel);
  }

  @Deprecated // to be removed before 2.0
  public RexNode getOriginalExpression() {
    return originalExpression;
  }

  @Deprecated // to be removed before 2.0
  public void setOriginalExpression(RexNode originalExpression) {
    this.originalExpression = originalExpression;
  }

  public RelOptPlanner getPlanner() {
    return planner;
  }

  public RelDataTypeFactory getTypeFactory() {
    return typeFactory;
  }

  public RexBuilder getRexBuilder() {
    return rexBuilder;
  }

  public @Nullable RelMetadataProvider getMetadataProvider() {
    return metadataProvider;
  }

  /**
   * Overrides the default metadata provider for this cluster.
   *
   * @param metadataProvider custom provider
   */
  @EnsuresNonNull({"this.metadataProvider", "this.metadataFactory"})
  @SuppressWarnings("deprecation")
  public void setMetadataProvider(
      @UnknownInitialization RelOptCluster this,
      RelMetadataProvider metadataProvider) {
    this.metadataProvider = metadataProvider;
    this.metadataFactory =
        new org.apache.calcite.rel.metadata.MetadataFactoryImpl(metadataProvider);
    // Wrap the metadata provider as a JaninoRelMetadataProvider
    // and set it to the ThreadLocal,
    // JaninoRelMetadataProvider is required by the RelMetadataQuery.
    RelMetadataQueryBase.THREAD_PROVIDERS
        .set(JaninoRelMetadataProvider.of(metadataProvider));
  }

  /**
   * Returns a {@link MetadataFactory}.
   *
   * @deprecated Use {@link #getMetadataQuery()}.
   */
  @Deprecated // to be removed before 2.0
  public MetadataFactory getMetadataFactory() {
    return metadataFactory;
  }

  /**
   * Sets up the customized {@link RelMetadataQuery} instance supplier that to
   * use during rule planning.
   *
   * <p>Note that the {@code mqSupplier} should return
   * a fresh new {@link RelMetadataQuery} instance because the instance would be
   * cached in this cluster, and we may invalidate and re-generate it
   * for each {@link RelOptRuleCall} cycle.
   */
  @EnsuresNonNull("this.mqSupplier")
  public void setMetadataQuerySupplier(
      @UnknownInitialization RelOptCluster this,
      Supplier<RelMetadataQuery> mqSupplier) {
    this.mqSupplier = mqSupplier;
  }

  /**
   * Returns the current RelMetadataQuery.
   *
   * <p>This method might be changed or moved in future.
   * If you have a {@link RelOptRuleCall} available,
   * for example if you are in a {@link RelOptRule#onMatch(RelOptRuleCall)}
   * method, then use {@link RelOptRuleCall#getMetadataQuery()} instead. */
  public RelMetadataQuery getMetadataQuery() {
    if (mq == null) {
      mq = castNonNull(mqSupplier).get();
    }
    return mq;
  }

  /**
   * Returns the supplier of RelMetadataQuery.
   */
  public Supplier<RelMetadataQuery> getMetadataQuerySupplier() {
    return this.mqSupplier;
  }

  /**
   * Should be called whenever the current {@link RelMetadataQuery} becomes
   * invalid. Typically invoked from {@link RelOptRuleCall#transformTo}.
   */
  public void invalidateMetadataQuery() {
    mq = null;
  }

  /**
   * Sets up the hint propagation strategies to be used during rule planning.
   *
   * <p>Use <code>RelOptNode.getCluster().getHintStrategies()</code> to fetch
   * the hint strategies.
   *
   * <p>Note that this method is only for internal use; the cluster {@code hintStrategies}
   * would be always set up with the instance configured by
   * {@link org.apache.calcite.sql2rel.SqlToRelConverter.Config}.
   *
   * @param hintStrategies The specified hint strategies to override the default one(empty)
   */
  public void setHintStrategies(HintStrategyTable hintStrategies) {
    requireNonNull(hintStrategies, "hintStrategies");
    this.hintStrategies = hintStrategies;
  }

  /**
   * Returns the hint strategies of this cluster. It is immutable during the whole planning phrase.
   */
  public HintStrategyTable getHintStrategies() {
    if (this.hintStrategies == null) {
      this.hintStrategies = HintStrategyTable.EMPTY;
    }
    return this.hintStrategies;
  }

  /**
   * Constructs a new id for a correlating variable. It is unique within the
   * whole query.
   */
  public CorrelationId createCorrel() {
    return new CorrelationId(nextCorrel.getAndIncrement());
  }

  /** Returns the default trait set for this cluster. */
  public RelTraitSet traitSet() {
    return emptyTraitSet;
  }

  // CHECKSTYLE: IGNORE 2
  /** @deprecated For {@code traitSetOf(t1, t2)},
   * use {@link #traitSet}().replace(t1).replace(t2). */
  @Deprecated // to be removed before 2.0
  public RelTraitSet traitSetOf(RelTrait... traits) {
    RelTraitSet traitSet = emptyTraitSet;
    for (RelTrait trait : traits) {
      traitSet = traitSet.replace(trait);
    }
    return traitSet;
  }

  public RelTraitSet traitSetOf(RelTrait trait) {
    return emptyTraitSet.replace(trait);
  }
}
