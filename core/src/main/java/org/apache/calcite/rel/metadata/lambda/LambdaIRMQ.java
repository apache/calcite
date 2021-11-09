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
package org.apache.calcite.rel.metadata.lambda;

import org.apache.calcite.plan.RelOptCost;
import org.apache.calcite.plan.RelOptPredicateList;
import org.apache.calcite.plan.volcano.VolcanoPlanner;
import org.apache.calcite.rel.RelCollation;
import org.apache.calcite.rel.RelDistribution;
import org.apache.calcite.rel.RelNode;
import org.apache.calcite.rel.metadata.CyclicMetadataException;
import org.apache.calcite.rel.metadata.DelegatingMetadataRel;
import org.apache.calcite.rel.metadata.NullSentinel;
import org.apache.calcite.rel.metadata.RelColumnOrigin;
import org.apache.calcite.rel.metadata.RelMetadataQuery;
import org.apache.calcite.rex.RexNode;
import org.apache.calcite.rex.RexTableInputRef;
import org.apache.calcite.sql.SqlExplainLevel;
import org.apache.calcite.util.ImmutableBitSet;

import com.google.common.collect.HashBasedTable;
import com.google.common.collect.ImmutableList;
import com.google.common.collect.Multimap;
import com.google.common.collect.Table;

import org.checkerframework.checker.nullness.qual.Nullable;

import java.util.List;
import java.util.Map;
import java.util.Set;
import java.util.concurrent.ExecutionException;

/**
 * A metadata retriever that moves through a list of possible handlers until it finds one
 * that returns a non-null value (or depletes the list, returning null).
 *
 * <p>This caches all RelNode instance/argument tuples so the same work isn't done twice.
 * <p>This monitors for cycles and returns null in those cases.
 * <p>This will automatically identify and recurse on DelegatingMetadataRel nodes.
 * <p>When caching arguments, RexNodes are converted to strings since equals() is shallow.
 * This e
 */
class LambdaIRMQ implements IRelMetadataQuery {
  private final RelMetadataQuery top;
  private final LambdaProvider handlerProvider;
  private final Table<RelNode, Object, Object> metadataCache = HashBasedTable.create();

  LambdaIRMQ(LambdaProvider handlerProvider, RelMetadataQuery top) {
    this.top = top;
    this.handlerProvider = handlerProvider;
  }

  private static RelNode recurseDelegates(RelNode r) {
    while (r instanceof DelegatingMetadataRel) {
      r = ((DelegatingMetadataRel) r).getMetadataDelegateRel();
    }
    return r;
  }

  public boolean clearCache(RelNode rel) {
    Map<Object, Object> row = metadataCache.row(rel);
    if (row.isEmpty()) {
      return false;
    }

    row.clear();
    return true;
  }

  private <T, L extends MetadataLambda> T findLambdas(
      Class<? extends RelNode> clazz,
      Class<L> methodInterface) {
    try {
      return (T) handlerProvider.get(clazz, methodInterface);
    } catch (ExecutionException e) {
      Throwable cause = e.getCause();
      if (cause instanceof RuntimeException) {
        throw (RuntimeException) cause;
      }
      throw new RuntimeException(cause);
    }
  }

  protected final  <T, A extends MetadataLambda.Arg0Lambda & MetadataLambda> T retrieve(
      RelNode r,
      Class<A> methodInterface) {
    r = recurseDelegates(r);
    final Object key = methodInterface;
    Object v = check(r, key);
    if (v != null) {
      return (T) v;
    }
    metadataCache.put(r, key, NullSentinel.ACTIVE);
    try {
      final Object x = invoke(r, methodInterface);
      metadataCache.put(r, key, NullSentinel.mask(x));
      return (T) x;
    } catch (java.lang.Exception e) {
      metadataCache.row(r).clear();
      throw e;
    }
  }

  private <T, A extends MetadataLambda.Arg0Lambda & MetadataLambda> T invoke(
      RelNode r,
      Class<A> methodInterface) {
    List<MetadataLambda.Arg0Lambda<RelNode, T>> lambdas =
        findLambdas(r.getClass(), methodInterface);
    if (lambdas.isEmpty()) {
      return null;
    }

    for (MetadataLambda.Arg0Lambda<RelNode, T> lambda : lambdas) {
      T val = lambda.call(r, top);
      if (val != null) {
        return val;
      }
    }

    return null;
  }

  private <T, A extends MetadataLambda.Arg1Lambda & MetadataLambda> T retrieve(
      RelNode r,
      Class<A> methodInterface,
      Object o0) {
    r = recurseDelegates(r);
    final List key;
    key = org.apache.calcite.runtime.FlatLists.of(methodInterface, keyifyArg(o0));
    Object v = check(r, key);
    if (v != null) {
      return (T) v;
    }
    metadataCache.put(r, key, NullSentinel.ACTIVE);
    try {
      final Object x = invoke(r, methodInterface, o0);
      metadataCache.put(r, key, NullSentinel.mask(x));
      return (T) x;
    } catch (java.lang.Exception e) {
      metadataCache.row(r).clear();
      throw e;
    }
  }

  private <T, A extends MetadataLambda.Arg1Lambda & MetadataLambda> T invoke(
      RelNode r,
      Class<A> methodInterface,
      Object o0) {
    List<MetadataLambda.Arg1Lambda<RelNode, Object, T>> lambdas =
        findLambdas(r.getClass(), methodInterface);
    if (lambdas.isEmpty()) {
      return null;
    }

    for (MetadataLambda.Arg1Lambda<RelNode, Object, T> lambda : lambdas) {
      T val = lambda.call(r, top, o0);
      if (val != null) {
        return val;
      }
    }

    return null;
  }

  private <T, A extends MetadataLambda.Arg2Lambda & MetadataLambda> T retrieve(
      RelNode r,
      Class<A> methodInterface,
      Object o0,
      Object o1) {
    r = recurseDelegates(r);
    final List key;
    key = org.apache.calcite.runtime.FlatLists.of(methodInterface, keyifyArg(o0), keyifyArg(o1));
    Object v = check(r, key);
    if (v != null) {
      return (T) v;
    }
    metadataCache.put(r, key, NullSentinel.ACTIVE);
    try {
      final Object x = invoke(r, methodInterface, o0, o1);
      metadataCache.put(r, key, NullSentinel.mask(x));
      return (T) x;
    } catch (java.lang.Exception e) {
      metadataCache.row(r).clear();
      throw e;
    }
  }

  private <T, A extends MetadataLambda.Arg2Lambda & MetadataLambda> T invoke(
      RelNode r,
      Class<A> methodInterface,
      Object o0,
      Object o1) {
    List<MetadataLambda.Arg2Lambda<RelNode, Object, Object, T>> lambdas =
        findLambdas(r.getClass(), methodInterface);
    if (lambdas.isEmpty()) {
      return null;
    }

    for (MetadataLambda.Arg2Lambda<RelNode, Object, Object, T> lambda : lambdas) {
      T val = lambda.call(r, top, o0, o1);
      if (val != null) {
        return val;
      }
    }

    return null;
  }

  private static Object keyifyArg(Object arg) {
    if (arg instanceof RexNode) {
      // RexNodes need to be converted to strings to support use in a key.
      return arg.toString();
    }
    return arg;
  }

  private Object check(RelNode r, Object key) {
    final Object v = metadataCache.get(r, key);
    if (v == null) {
      return null;
    }
    if (v == NullSentinel.ACTIVE) {
      throw new CyclicMetadataException();
    }
    if (v == NullSentinel.INSTANCE) {
      return null;
    }
    return v;
  }


  public @Nullable Multimap<Class<? extends RelNode>, RelNode> getNodeTypes(final RelNode rel) {
    return retrieve(rel, MetadataLambda.NodeTypes.class);
  }

  public Double getRowCount(final RelNode rel) {
    return retrieve(rel, MetadataLambda.RowCount.class);
  }

  public @Nullable Double getMaxRowCount(final RelNode rel) {
    return retrieve(rel, MetadataLambda.MaxRowCount.class);
  }

  public @Nullable Double getMinRowCount(final RelNode rel) {
    return retrieve(rel, MetadataLambda.MinRowCount.class);
  }

  public @Nullable RelOptCost getCumulativeCost(final RelNode rel) {
    return retrieve(rel, MetadataLambda.CumulativeCost.class);
  }

  public @Nullable RelOptCost getNonCumulativeCost(final RelNode rel) {
    return retrieve(rel, MetadataLambda.NonCumulativeCost.class);
  }

  public @Nullable Double getPercentageOriginalRows(final RelNode rel) {
    return retrieve(rel, MetadataLambda.PercentageOriginalRows.class);
  }

  public @Nullable Set<RelColumnOrigin> getColumnOrigins(final RelNode rel, final int column) {
    return retrieve(rel, MetadataLambda.ColumnOrigins.class, column);
  }

  public @Nullable Set<RexNode> getExpressionLineage(final RelNode rel, final RexNode expression) {
    return retrieve(rel, MetadataLambda.ExpressionLineage.class, expression);
  }

  public @Nullable Set<RexTableInputRef.RelTableRef> getTableReferences(final RelNode rel) {
    return retrieve(rel, MetadataLambda.TableReferences.class);
  }

  public @Nullable Double getSelectivity(final RelNode rel, @Nullable final RexNode predicate) {
    return retrieve(rel, MetadataLambda.Selectivity.class, predicate);
  }

  public @Nullable Set<ImmutableBitSet> getUniqueKeys(
      final RelNode rel, final boolean ignoreNulls) {
    return retrieve(rel, MetadataLambda.UniqueKeys.class, ignoreNulls);
  }

  public @Nullable Boolean areColumnsUnique(
      final RelNode rel, final ImmutableBitSet columns, final boolean ignoreNulls) {
    return retrieve(rel, MetadataLambda.ColumnsUnique.class, columns, ignoreNulls);
  }

  public @Nullable ImmutableList<RelCollation> collations(final RelNode rel) {
    return retrieve(rel, MetadataLambda.Collations.class);
  }

  public @Nullable Double getPopulationSize(final RelNode rel, final ImmutableBitSet groupKey) {
    return retrieve(rel, MetadataLambda.PopulationSize.class, groupKey);
  }

  public @Nullable Double getAverageRowSize(final RelNode rel) {
    return retrieve(rel, MetadataLambda.AverageRowSize.class);
  }

  public @Nullable List<@Nullable Double> getAverageColumnSizes(final RelNode rel) {
    return retrieve(rel, MetadataLambda.AverageColumnSizes.class);
  }

  public @Nullable Boolean isPhaseTransition(final RelNode rel) {
    return retrieve(rel, MetadataLambda.PhaseTransition.class);
  }

  public @Nullable Integer splitCount(final RelNode rel) {
    return retrieve(rel, MetadataLambda.SplitCount.class);
  }

  public @Nullable Double memory(final RelNode rel) {
    return retrieve(rel, MetadataLambda.Memory.class);
  }

  public @Nullable Double cumulativeMemoryWithinPhase(final RelNode rel) {
    return retrieve(rel, MetadataLambda.CumulativeMemoryWithinPhase.class);
  }

  public @Nullable Double cumulativeMemoryWithinPhaseSplit(final RelNode rel) {
    return retrieve(rel, MetadataLambda.CumulativeMemoryWithinPhaseSplit.class);
  }

  public @Nullable Double getDistinctRowCount(
      final RelNode rel, final ImmutableBitSet groupKey, @Nullable final RexNode predicate) {
    return retrieve(rel, MetadataLambda.DistinctRowCount.class, groupKey, predicate);
  }

  public RelOptPredicateList getPulledUpPredicates(final RelNode rel) {
    return retrieve(rel, MetadataLambda.PulledUpPredicates.class);
  }

  public @Nullable RelOptPredicateList getAllPredicates(final RelNode rel) {
    return retrieve(rel, MetadataLambda.AllPredicates.class);
  }

  public Boolean isVisibleInExplain(final RelNode rel, final SqlExplainLevel explainLevel) {
    return retrieve(rel, MetadataLambda.VisibleInExplain.class, explainLevel);
  }

  public @Nullable RelDistribution getDistribution(final RelNode rel) {
    return retrieve(rel, MetadataLambda.Distribution.class);
  }

  public @Nullable RelOptCost getLowerBoundCost(final RelNode rel, final VolcanoPlanner planner) {
    return retrieve(rel, MetadataLambda.LowerBoundCost.class, planner);
  }

}
