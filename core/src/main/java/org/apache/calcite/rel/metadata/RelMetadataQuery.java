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
package org.apache.calcite.rel.metadata;

import org.apache.calcite.plan.RelOptCost;
import org.apache.calcite.plan.RelOptPredicateList;
import org.apache.calcite.plan.RelOptTable;
import org.apache.calcite.plan.volcano.VolcanoPlanner;
import org.apache.calcite.rel.RelCollation;
import org.apache.calcite.rel.RelDistribution;
import org.apache.calcite.rel.RelDistributions;
import org.apache.calcite.rel.RelNode;
import org.apache.calcite.rex.RexNode;
import org.apache.calcite.rex.RexTableInputRef.RelTableRef;
import org.apache.calcite.sql.SqlExplainLevel;
import org.apache.calcite.util.ImmutableBitSet;

import com.google.common.collect.ImmutableList;
import com.google.common.collect.Iterables;
import com.google.common.collect.Multimap;

import org.checkerframework.checker.nullness.qual.Nullable;

import java.util.Collections;
import java.util.List;
import java.util.Set;

import static org.apache.calcite.linq4j.Nullness.castNonNull;

import static java.util.Objects.requireNonNull;

/**
 * RelMetadataQuery provides a strongly-typed facade on top of
 * {@link RelMetadataProvider} for the set of relational expression metadata
 * queries defined as standard within Calcite. The Javadoc on these methods
 * serves as their primary specification.
 *
 * <p>To add a new standard query <code>Xyz</code> to this interface, follow
 * these steps:
 *
 * <ol>
 * <li>Add a static method <code>getXyz</code> specification to this class.
 * <li>Add unit tests to {@code org.apache.calcite.test.RelMetadataTest}.
 * <li>Write a new provider class <code>RelMdXyz</code> in this package. Follow
 * the pattern from an existing class such as {@link RelMdColumnOrigins},
 * overloading on all of the logical relational expressions to which the query
 * applies.
 * <li>Add a {@code SOURCE} static member, similar to
 *     {@link RelMdColumnOrigins#SOURCE}.
 * <li>Register the {@code SOURCE} object in {@link DefaultRelMetadataProvider}.
 * <li>Get unit tests working.
 * </ol>
 *
 * <p>Because relational expression metadata is extensible, extension projects
 * can define similar facades in order to specify access to custom metadata.
 * Please do not add queries here (nor on {@link RelNode}) which lack meaning
 * outside of your extension.
 *
 * <p>Besides adding new metadata queries, extension projects may need to add
 * custom providers for the standard queries in order to handle additional
 * relational expressions (either logical or physical). In either case, the
 * process is the same: write a reflective provider and chain it on to an
 * instance of {@link DefaultRelMetadataProvider}, pre-pending it to the default
 * providers. Then supply that instance to the planner via the appropriate
 * plugin mechanism.
 */
public class RelMetadataQuery extends RelMetadataQueryBase {
  private static final RelMetadataQuery EMPTY = new RelMetadataQuery(false);

  private BuiltInMetadata.Collation.Handler collationHandler;
  private BuiltInMetadata.ColumnOrigin.Handler columnOriginHandler;
  private BuiltInMetadata.ExpressionLineage.Handler expressionLineageHandler;
  private BuiltInMetadata.TableReferences.Handler tableReferencesHandler;
  private BuiltInMetadata.ColumnUniqueness.Handler columnUniquenessHandler;
  private BuiltInMetadata.CumulativeCost.Handler cumulativeCostHandler;
  private BuiltInMetadata.DistinctRowCount.Handler distinctRowCountHandler;
  private BuiltInMetadata.Distribution.Handler distributionHandler;
  private BuiltInMetadata.ExplainVisibility.Handler explainVisibilityHandler;
  private BuiltInMetadata.MaxRowCount.Handler maxRowCountHandler;
  private BuiltInMetadata.MinRowCount.Handler minRowCountHandler;
  private BuiltInMetadata.Memory.Handler memoryHandler;
  private BuiltInMetadata.NonCumulativeCost.Handler nonCumulativeCostHandler;
  private BuiltInMetadata.Parallelism.Handler parallelismHandler;
  private BuiltInMetadata.PercentageOriginalRows.Handler percentageOriginalRowsHandler;
  private BuiltInMetadata.PopulationSize.Handler populationSizeHandler;
  private BuiltInMetadata.Predicates.Handler predicatesHandler;
  private BuiltInMetadata.AllPredicates.Handler allPredicatesHandler;
  private BuiltInMetadata.NodeTypes.Handler nodeTypesHandler;
  private BuiltInMetadata.RowCount.Handler rowCountHandler;
  private BuiltInMetadata.Selectivity.Handler selectivityHandler;
  private BuiltInMetadata.Size.Handler sizeHandler;
  private BuiltInMetadata.UniqueKeys.Handler uniqueKeysHandler;
  private BuiltInMetadata.LowerBoundCost.Handler lowerBoundCostHandler;

  /**
   * Creates the instance with {@link JaninoRelMetadataProvider} instance
   * from {@link #THREAD_PROVIDERS} and {@link #EMPTY} as a prototype.
   */
  protected RelMetadataQuery() {
    this(castNonNull(THREAD_PROVIDERS.get()), EMPTY);
  }

  /** Creates and initializes the instance that will serve as a prototype for
   * all other instances. */
  private RelMetadataQuery(@SuppressWarnings("unused") boolean dummy) {
    super(null);
    this.collationHandler = initialHandler(BuiltInMetadata.Collation.Handler.class);
    this.columnOriginHandler = initialHandler(BuiltInMetadata.ColumnOrigin.Handler.class);
    this.expressionLineageHandler = initialHandler(BuiltInMetadata.ExpressionLineage.Handler.class);
    this.tableReferencesHandler = initialHandler(BuiltInMetadata.TableReferences.Handler.class);
    this.columnUniquenessHandler = initialHandler(BuiltInMetadata.ColumnUniqueness.Handler.class);
    this.cumulativeCostHandler = initialHandler(BuiltInMetadata.CumulativeCost.Handler.class);
    this.distinctRowCountHandler = initialHandler(BuiltInMetadata.DistinctRowCount.Handler.class);
    this.distributionHandler = initialHandler(BuiltInMetadata.Distribution.Handler.class);
    this.explainVisibilityHandler = initialHandler(BuiltInMetadata.ExplainVisibility.Handler.class);
    this.maxRowCountHandler = initialHandler(BuiltInMetadata.MaxRowCount.Handler.class);
    this.minRowCountHandler = initialHandler(BuiltInMetadata.MinRowCount.Handler.class);
    this.memoryHandler = initialHandler(BuiltInMetadata.Memory.Handler.class);
    this.nonCumulativeCostHandler = initialHandler(BuiltInMetadata.NonCumulativeCost.Handler.class);
    this.parallelismHandler = initialHandler(BuiltInMetadata.Parallelism.Handler.class);
    this.percentageOriginalRowsHandler =
        initialHandler(BuiltInMetadata.PercentageOriginalRows.Handler.class);
    this.populationSizeHandler = initialHandler(BuiltInMetadata.PopulationSize.Handler.class);
    this.predicatesHandler = initialHandler(BuiltInMetadata.Predicates.Handler.class);
    this.allPredicatesHandler = initialHandler(BuiltInMetadata.AllPredicates.Handler.class);
    this.nodeTypesHandler = initialHandler(BuiltInMetadata.NodeTypes.Handler.class);
    this.rowCountHandler = initialHandler(BuiltInMetadata.RowCount.Handler.class);
    this.selectivityHandler = initialHandler(BuiltInMetadata.Selectivity.Handler.class);
    this.sizeHandler = initialHandler(BuiltInMetadata.Size.Handler.class);
    this.uniqueKeysHandler = initialHandler(BuiltInMetadata.UniqueKeys.Handler.class);
    this.lowerBoundCostHandler = initialHandler(BuiltInMetadata.LowerBoundCost.Handler.class);
  }

  private RelMetadataQuery(JaninoRelMetadataProvider metadataProvider,
      RelMetadataQuery prototype) {
    super(requireNonNull(metadataProvider, "metadataProvider"));
    this.collationHandler = prototype.collationHandler;
    this.columnOriginHandler = prototype.columnOriginHandler;
    this.expressionLineageHandler = prototype.expressionLineageHandler;
    this.tableReferencesHandler = prototype.tableReferencesHandler;
    this.columnUniquenessHandler = prototype.columnUniquenessHandler;
    this.cumulativeCostHandler = prototype.cumulativeCostHandler;
    this.distinctRowCountHandler = prototype.distinctRowCountHandler;
    this.distributionHandler = prototype.distributionHandler;
    this.explainVisibilityHandler = prototype.explainVisibilityHandler;
    this.maxRowCountHandler = prototype.maxRowCountHandler;
    this.minRowCountHandler = prototype.minRowCountHandler;
    this.memoryHandler = prototype.memoryHandler;
    this.nonCumulativeCostHandler = prototype.nonCumulativeCostHandler;
    this.parallelismHandler = prototype.parallelismHandler;
    this.percentageOriginalRowsHandler = prototype.percentageOriginalRowsHandler;
    this.populationSizeHandler = prototype.populationSizeHandler;
    this.predicatesHandler = prototype.predicatesHandler;
    this.allPredicatesHandler = prototype.allPredicatesHandler;
    this.nodeTypesHandler = prototype.nodeTypesHandler;
    this.rowCountHandler = prototype.rowCountHandler;
    this.selectivityHandler = prototype.selectivityHandler;
    this.sizeHandler = prototype.sizeHandler;
    this.uniqueKeysHandler = prototype.uniqueKeysHandler;
    this.lowerBoundCostHandler = prototype.lowerBoundCostHandler;
  }

  //~ Methods ----------------------------------------------------------------

  /**
   * Returns an instance of RelMetadataQuery. It ensures that cycles do not
   * occur while computing metadata.
   */
  public static RelMetadataQuery instance() {
    return new RelMetadataQuery();
  }

  /**
   * Returns the
   * {@link BuiltInMetadata.NodeTypes#getNodeTypes()}
   * statistic.
   *
   * @param rel the relational expression
   */
  public @Nullable Multimap<Class<? extends RelNode>, RelNode> getNodeTypes(RelNode rel) {
    for (;;) {
      try {
        return nodeTypesHandler.getNodeTypes(rel, this);
      } catch (JaninoRelMetadataProvider.NoHandler e) {
        nodeTypesHandler = revise(e.relClass, BuiltInMetadata.NodeTypes.DEF);
      } catch (CyclicMetadataException e) {
        return null;
      }
    }
  }

  /**
   * Returns the
   * {@link BuiltInMetadata.RowCount#getRowCount()}
   * statistic.
   *
   * @param rel the relational expression
   * @return estimated row count, or null if no reliable estimate can be
   * determined
   */
  public /* @Nullable: CALCITE-4263 */ Double getRowCount(RelNode rel) {
    for (;;) {
      try {
        Double result = rowCountHandler.getRowCount(rel, this);
        return RelMdUtil.validateResult(castNonNull(result));
      } catch (JaninoRelMetadataProvider.NoHandler e) {
        rowCountHandler = revise(e.relClass, BuiltInMetadata.RowCount.DEF);
      }
    }
  }

  /**
   * Returns the
   * {@link BuiltInMetadata.MaxRowCount#getMaxRowCount()}
   * statistic.
   *
   * @param rel the relational expression
   * @return max row count
   */
  public @Nullable Double getMaxRowCount(RelNode rel) {
    for (;;) {
      try {
        return maxRowCountHandler.getMaxRowCount(rel, this);
      } catch (JaninoRelMetadataProvider.NoHandler e) {
        maxRowCountHandler =
            revise(e.relClass, BuiltInMetadata.MaxRowCount.DEF);
      }
    }
  }

  /**
   * Returns the
   * {@link BuiltInMetadata.MinRowCount#getMinRowCount()}
   * statistic.
   *
   * @param rel the relational expression
   * @return max row count
   */
  public @Nullable Double getMinRowCount(RelNode rel) {
    for (;;) {
      try {
        return minRowCountHandler.getMinRowCount(rel, this);
      } catch (JaninoRelMetadataProvider.NoHandler e) {
        minRowCountHandler =
            revise(e.relClass, BuiltInMetadata.MinRowCount.DEF);
      }
    }
  }

  /**
   * Returns the
   * {@link BuiltInMetadata.CumulativeCost#getCumulativeCost()}
   * statistic.
   *
   * @param rel the relational expression
   * @return estimated cost, or null if no reliable estimate can be determined
   */
  public @Nullable RelOptCost getCumulativeCost(RelNode rel) {
    for (;;) {
      try {
        return cumulativeCostHandler.getCumulativeCost(rel, this);
      } catch (JaninoRelMetadataProvider.NoHandler e) {
        cumulativeCostHandler =
            revise(e.relClass, BuiltInMetadata.CumulativeCost.DEF);
      }
    }
  }

  /**
   * Returns the
   * {@link BuiltInMetadata.NonCumulativeCost#getNonCumulativeCost()}
   * statistic.
   *
   * @param rel the relational expression
   * @return estimated cost, or null if no reliable estimate can be determined
   */
  public @Nullable RelOptCost getNonCumulativeCost(RelNode rel) {
    for (;;) {
      try {
        return nonCumulativeCostHandler.getNonCumulativeCost(rel, this);
      } catch (JaninoRelMetadataProvider.NoHandler e) {
        nonCumulativeCostHandler =
            revise(e.relClass, BuiltInMetadata.NonCumulativeCost.DEF);
      }
    }
  }

  /**
   * Returns the
   * {@link BuiltInMetadata.PercentageOriginalRows#getPercentageOriginalRows()}
   * statistic.
   *
   * @param rel the relational expression
   * @return estimated percentage (between 0.0 and 1.0), or null if no
   * reliable estimate can be determined
   */
  public @Nullable Double getPercentageOriginalRows(RelNode rel) {
    for (;;) {
      try {
        Double result =
            percentageOriginalRowsHandler.getPercentageOriginalRows(rel, this);
        return RelMdUtil.validatePercentage(result);
      } catch (JaninoRelMetadataProvider.NoHandler e) {
        percentageOriginalRowsHandler =
            revise(e.relClass, BuiltInMetadata.PercentageOriginalRows.DEF);
      }
    }
  }

  /**
   * Returns the
   * {@link BuiltInMetadata.ColumnOrigin#getColumnOrigins(int)}
   * statistic.
   *
   * @param rel           the relational expression
   * @param column 0-based ordinal for output column of interest
   * @return set of origin columns, or null if this information cannot be
   * determined (whereas empty set indicates definitely no origin columns at
   * all)
   */
  public @Nullable Set<RelColumnOrigin> getColumnOrigins(RelNode rel, int column) {
    for (;;) {
      try {
        return columnOriginHandler.getColumnOrigins(rel, this, column);
      } catch (JaninoRelMetadataProvider.NoHandler e) {
        columnOriginHandler =
            revise(e.relClass, BuiltInMetadata.ColumnOrigin.DEF);
      }
    }
  }

  /**
   * Determines the origin of a column.
   *
   * @see #getColumnOrigins(org.apache.calcite.rel.RelNode, int)
   *
   * @param rel the RelNode of the column
   * @param column the offset of the column whose origin we are trying to
   * determine
   *
   * @return the origin of a column
   */
  public @Nullable RelColumnOrigin getColumnOrigin(RelNode rel, int column) {
    final Set<RelColumnOrigin> origins = getColumnOrigins(rel, column);
    if (origins == null || origins.size() != 1) {
      return null;
    }
    final RelColumnOrigin origin = Iterables.getOnlyElement(origins);
    return origin;
  }

  /**
   * Determines the origin of a column.
   */
  public @Nullable Set<RexNode> getExpressionLineage(RelNode rel, RexNode expression) {
    for (;;) {
      try {
        return expressionLineageHandler.getExpressionLineage(rel, this, expression);
      } catch (JaninoRelMetadataProvider.NoHandler e) {
        expressionLineageHandler =
            revise(e.relClass, BuiltInMetadata.ExpressionLineage.DEF);
      }
    }
  }

  /**
   * Determines the tables used by a plan.
   */
  public @Nullable Set<RelTableRef> getTableReferences(RelNode rel) {
    for (;;) {
      try {
        return tableReferencesHandler.getTableReferences(rel, this);
      } catch (JaninoRelMetadataProvider.NoHandler e) {
        tableReferencesHandler =
            revise(e.relClass, BuiltInMetadata.TableReferences.DEF);
      }
    }
  }

  /**
   * Determines the origin of a {@link RelNode}, provided it maps to a single
   * table, optionally with filtering and projection.
   *
   * @param rel the RelNode
   *
   * @return the table, if the RelNode is a simple table; otherwise null
   */
  public @Nullable RelOptTable getTableOrigin(RelNode rel) {
    // Determine the simple origin of the first column in the
    // RelNode.  If it's simple, then that means that the underlying
    // table is also simple, even if the column itself is derived.
    if (rel.getRowType().getFieldCount() == 0) {
      return null;
    }
    final Set<RelColumnOrigin> colOrigins = getColumnOrigins(rel, 0);
    if (colOrigins == null || colOrigins.size() == 0) {
      return null;
    }
    return colOrigins.iterator().next().getOriginTable();
  }

  /**
   * Returns the
   * {@link BuiltInMetadata.Selectivity#getSelectivity(RexNode)}
   * statistic.
   *
   * @param rel       the relational expression
   * @param predicate predicate whose selectivity is to be estimated against
   *                  {@code rel}'s output
   * @return estimated selectivity (between 0.0 and 1.0), or null if no
   * reliable estimate can be determined
   */
  public @Nullable Double getSelectivity(RelNode rel, @Nullable RexNode predicate) {
    for (;;) {
      try {
        Double result = selectivityHandler.getSelectivity(rel, this, predicate);
        return RelMdUtil.validatePercentage(result);
      } catch (JaninoRelMetadataProvider.NoHandler e) {
        selectivityHandler =
            revise(e.relClass, BuiltInMetadata.Selectivity.DEF);
      }
    }
  }

  /**
   * Returns the
   * {@link BuiltInMetadata.UniqueKeys#getUniqueKeys(boolean)}
   * statistic.
   *
   * @param rel the relational expression
   * @return set of keys, or null if this information cannot be determined
   * (whereas empty set indicates definitely no keys at all)
   */
  public @Nullable Set<ImmutableBitSet> getUniqueKeys(RelNode rel) {
    return getUniqueKeys(rel, false);
  }

  /**
   * Returns the
   * {@link BuiltInMetadata.UniqueKeys#getUniqueKeys(boolean)}
   * statistic.
   *
   * @param rel         the relational expression
   * @param ignoreNulls if true, ignore null values when determining
   *                    whether the keys are unique
   *
   * @return set of keys, or null if this information cannot be determined
   * (whereas empty set indicates definitely no keys at all)
   */
  public @Nullable Set<ImmutableBitSet> getUniqueKeys(RelNode rel,
      boolean ignoreNulls) {
    for (;;) {
      try {
        return uniqueKeysHandler.getUniqueKeys(rel, this, ignoreNulls);
      } catch (JaninoRelMetadataProvider.NoHandler e) {
        uniqueKeysHandler =
            revise(e.relClass, BuiltInMetadata.UniqueKeys.DEF);
      }
    }
  }

  /**
   * Returns whether the rows of a given relational expression are distinct.
   * This is derived by applying the
   * {@link BuiltInMetadata.ColumnUniqueness#areColumnsUnique(org.apache.calcite.util.ImmutableBitSet, boolean)}
   * statistic over all columns.
   *
   * @param rel     the relational expression
   *
   * @return true or false depending on whether the rows are unique, or
   * null if not enough information is available to make that determination
   */
  public @Nullable Boolean areRowsUnique(RelNode rel) {
    final ImmutableBitSet columns =
        ImmutableBitSet.range(rel.getRowType().getFieldCount());
    return areColumnsUnique(rel, columns, false);
  }

  /**
   * Returns the
   * {@link BuiltInMetadata.ColumnUniqueness#areColumnsUnique(ImmutableBitSet, boolean)}
   * statistic.
   *
   * @param rel     the relational expression
   * @param columns column mask representing the subset of columns for which
   *                uniqueness will be determined
   *
   * @return true or false depending on whether the columns are unique, or
   * null if not enough information is available to make that determination
   */
  public @Nullable Boolean areColumnsUnique(RelNode rel, ImmutableBitSet columns) {
    return areColumnsUnique(rel, columns, false);
  }

  /**
   * Returns the
   * {@link BuiltInMetadata.ColumnUniqueness#areColumnsUnique(ImmutableBitSet, boolean)}
   * statistic.
   *
   * @param rel         the relational expression
   * @param columns     column mask representing the subset of columns for which
   *                    uniqueness will be determined
   * @param ignoreNulls if true, ignore null values when determining column
   *                    uniqueness
   * @return true or false depending on whether the columns are unique, or
   * null if not enough information is available to make that determination
   */
  public @Nullable Boolean areColumnsUnique(RelNode rel, ImmutableBitSet columns,
      boolean ignoreNulls) {
    for (;;) {
      try {
        return columnUniquenessHandler.areColumnsUnique(rel, this, columns,
            ignoreNulls);
      } catch (JaninoRelMetadataProvider.NoHandler e) {
        columnUniquenessHandler =
            revise(e.relClass, BuiltInMetadata.ColumnUniqueness.DEF);
      }
    }
  }

  /**
   * Returns the
   * {@link BuiltInMetadata.Collation#collations()}
   * statistic.
   *
   * @param rel         the relational expression
   * @return List of sorted column combinations, or
   * null if not enough information is available to make that determination
   */
  public @Nullable ImmutableList<RelCollation> collations(RelNode rel) {
    for (;;) {
      try {
        return collationHandler.collations(rel, this);
      } catch (JaninoRelMetadataProvider.NoHandler e) {
        collationHandler = revise(e.relClass, BuiltInMetadata.Collation.DEF);
      }
    }
  }

  /**
   * Returns the
   * {@link BuiltInMetadata.Distribution#distribution()}
   * statistic.
   *
   * @param rel         the relational expression
   * @return List of sorted column combinations, or
   * null if not enough information is available to make that determination
   */
  public RelDistribution distribution(RelNode rel) {
    for (;;) {
      try {
        RelDistribution distribution = distributionHandler.distribution(rel, this);
        //noinspection ConstantConditions
        if (distribution == null) {
          return RelDistributions.ANY;
        }
        return distribution;
      } catch (JaninoRelMetadataProvider.NoHandler e) {
        distributionHandler =
            revise(e.relClass, BuiltInMetadata.Distribution.DEF);
      }
    }
  }

  /**
   * Returns the
   * {@link BuiltInMetadata.PopulationSize#getPopulationSize(ImmutableBitSet)}
   * statistic.
   *
   * @param rel      the relational expression
   * @param groupKey column mask representing the subset of columns for which
   *                 the row count will be determined
   * @return distinct row count for the given groupKey, or null if no reliable
   * estimate can be determined
   *
   */
  public @Nullable Double getPopulationSize(RelNode rel,
      ImmutableBitSet groupKey) {
    for (;;) {
      try {
        Double result =
            populationSizeHandler.getPopulationSize(rel, this, groupKey);
        return RelMdUtil.validateResult(result);
      } catch (JaninoRelMetadataProvider.NoHandler e) {
        populationSizeHandler =
            revise(e.relClass, BuiltInMetadata.PopulationSize.DEF);
      }
    }
  }

  /**
   * Returns the
   * {@link BuiltInMetadata.Size#averageRowSize()}
   * statistic.
   *
   * @param rel      the relational expression
   * @return average size of a row, in bytes, or null if not known
     */
  public @Nullable Double getAverageRowSize(RelNode rel) {
    for (;;) {
      try {
        return sizeHandler.averageRowSize(rel, this);
      } catch (JaninoRelMetadataProvider.NoHandler e) {
        sizeHandler = revise(e.relClass, BuiltInMetadata.Size.DEF);
      }
    }
  }

  /**
   * Returns the
   * {@link BuiltInMetadata.Size#averageColumnSizes()}
   * statistic.
   *
   * @param rel      the relational expression
   * @return a list containing, for each column, the average size of a column
   * value, in bytes. Each value or the entire list may be null if the
   * metadata is not available
   */
  public @Nullable List<@Nullable Double> getAverageColumnSizes(RelNode rel) {
    for (;;) {
      try {
        return sizeHandler.averageColumnSizes(rel, this);
      } catch (JaninoRelMetadataProvider.NoHandler e) {
        sizeHandler = revise(e.relClass, BuiltInMetadata.Size.DEF);
      }
    }
  }

  /** As {@link #getAverageColumnSizes(org.apache.calcite.rel.RelNode)} but
   * never returns a null list, only ever a list of nulls. */
  public List<@Nullable Double> getAverageColumnSizesNotNull(RelNode rel) {
    final @Nullable List<@Nullable Double> averageColumnSizes = getAverageColumnSizes(rel);
    return averageColumnSizes == null
        ? Collections.nCopies(rel.getRowType().getFieldCount(), null)
        : averageColumnSizes;
  }

  /**
   * Returns the
   * {@link BuiltInMetadata.Parallelism#isPhaseTransition()}
   * statistic.
   *
   * @param rel      the relational expression
   * @return whether each physical operator implementing this relational
   * expression belongs to a different process than its inputs, or null if not
   * known
   */
  public @Nullable Boolean isPhaseTransition(RelNode rel) {
    for (;;) {
      try {
        return parallelismHandler.isPhaseTransition(rel, this);
      } catch (JaninoRelMetadataProvider.NoHandler e) {
        parallelismHandler =
            revise(e.relClass, BuiltInMetadata.Parallelism.DEF);
      }
    }
  }

  /**
   * Returns the
   * {@link BuiltInMetadata.Parallelism#splitCount()}
   * statistic.
   *
   * @param rel      the relational expression
   * @return the number of distinct splits of the data, or null if not known
   */
  public @Nullable Integer splitCount(RelNode rel) {
    for (;;) {
      try {
        return parallelismHandler.splitCount(rel, this);
      } catch (JaninoRelMetadataProvider.NoHandler e) {
        parallelismHandler =
            revise(e.relClass, BuiltInMetadata.Parallelism.DEF);
      }
    }
  }

  /**
   * Returns the
   * {@link BuiltInMetadata.Memory#memory()}
   * statistic.
   *
   * @param rel      the relational expression
   * @return the expected amount of memory, in bytes, required by a physical
   * operator implementing this relational expression, across all splits,
   * or null if not known
   */
  public @Nullable Double memory(RelNode rel) {
    for (;;) {
      try {
        return memoryHandler.memory(rel, this);
      } catch (JaninoRelMetadataProvider.NoHandler e) {
        memoryHandler = revise(e.relClass, BuiltInMetadata.Memory.DEF);
      }
    }
  }

  /**
   * Returns the
   * {@link BuiltInMetadata.Memory#cumulativeMemoryWithinPhase()}
   * statistic.
   *
   * @param rel      the relational expression
   * @return the cumulative amount of memory, in bytes, required by the
   * physical operator implementing this relational expression, and all other
   * operators within the same phase, across all splits, or null if not known
   */
  public @Nullable Double cumulativeMemoryWithinPhase(RelNode rel) {
    for (;;) {
      try {
        return memoryHandler.cumulativeMemoryWithinPhase(rel, this);
      } catch (JaninoRelMetadataProvider.NoHandler e) {
        memoryHandler = revise(e.relClass, BuiltInMetadata.Memory.DEF);
      }
    }
  }

  /**
   * Returns the
   * {@link BuiltInMetadata.Memory#cumulativeMemoryWithinPhaseSplit()}
   * statistic.
   *
   * @param rel      the relational expression
   * @return the expected cumulative amount of memory, in bytes, required by
   * the physical operator implementing this relational expression, and all
   * operators within the same phase, within each split, or null if not known
   */
  public @Nullable Double cumulativeMemoryWithinPhaseSplit(RelNode rel) {
    for (;;) {
      try {
        return memoryHandler.cumulativeMemoryWithinPhaseSplit(rel, this);
      } catch (JaninoRelMetadataProvider.NoHandler e) {
        memoryHandler = revise(e.relClass, BuiltInMetadata.Memory.DEF);
      }
    }
  }

  /**
   * Returns the
   * {@link BuiltInMetadata.DistinctRowCount#getDistinctRowCount(ImmutableBitSet, RexNode)}
   * statistic.
   *
   * @param rel       the relational expression
   * @param groupKey  column mask representing group by columns
   * @param predicate pre-filtered predicates
   * @return distinct row count for groupKey, filtered by predicate, or null
   * if no reliable estimate can be determined
   */
  public @Nullable Double getDistinctRowCount(
      RelNode rel,
      ImmutableBitSet groupKey,
      @Nullable RexNode predicate) {
    for (;;) {
      try {
        Double result =
            distinctRowCountHandler.getDistinctRowCount(rel, this, groupKey,
                predicate);
        return RelMdUtil.validateResult(result);
      } catch (JaninoRelMetadataProvider.NoHandler e) {
        distinctRowCountHandler =
            revise(e.relClass, BuiltInMetadata.DistinctRowCount.DEF);
      }
    }
  }

  /**
   * Returns the
   * {@link BuiltInMetadata.Predicates#getPredicates()}
   * statistic.
   *
   * @param rel the relational expression
   * @return Predicates that can be pulled above this RelNode
   */
  public RelOptPredicateList getPulledUpPredicates(RelNode rel) {
    for (;;) {
      try {
        RelOptPredicateList result = predicatesHandler.getPredicates(rel, this);
        return result != null ? result : RelOptPredicateList.EMPTY;
      } catch (JaninoRelMetadataProvider.NoHandler e) {
        predicatesHandler = revise(e.relClass, BuiltInMetadata.Predicates.DEF);
      }
    }
  }

  /**
   * Returns the
   * {@link BuiltInMetadata.AllPredicates#getAllPredicates()}
   * statistic.
   *
   * @param rel the relational expression
   * @return All predicates within and below this RelNode
   */
  public @Nullable RelOptPredicateList getAllPredicates(RelNode rel) {
    for (;;) {
      try {
        return allPredicatesHandler.getAllPredicates(rel, this);
      } catch (JaninoRelMetadataProvider.NoHandler e) {
        allPredicatesHandler = revise(e.relClass, BuiltInMetadata.AllPredicates.DEF);
      }
    }
  }

  /**
   * Returns the
   * {@link BuiltInMetadata.ExplainVisibility#isVisibleInExplain(SqlExplainLevel)}
   * statistic.
   *
   * @param rel          the relational expression
   * @param explainLevel level of detail
   * @return true for visible, false for invisible; if no metadata is available,
   * defaults to true
   */
  public Boolean isVisibleInExplain(RelNode rel,
      SqlExplainLevel explainLevel) {
    for (;;) {
      try {
        Boolean b = explainVisibilityHandler.isVisibleInExplain(rel, this,
            explainLevel);
        return b == null || b;
      } catch (JaninoRelMetadataProvider.NoHandler e) {
        explainVisibilityHandler =
            revise(e.relClass, BuiltInMetadata.ExplainVisibility.DEF);
      }
    }
  }

  /**
   * Returns the
   * {@link BuiltInMetadata.Distribution#distribution()}
   * statistic.
   *
   * @param rel the relational expression
   *
   * @return description of how the rows in the relational expression are
   * physically distributed
   */
  public @Nullable RelDistribution getDistribution(RelNode rel) {
    for (;;) {
      try {
        return distributionHandler.distribution(rel, this);
      } catch (JaninoRelMetadataProvider.NoHandler e) {
        distributionHandler = revise(e.relClass, BuiltInMetadata.Distribution.DEF);
      }
    }
  }

  /**
   * Returns the lower bound cost of a RelNode.
   */
  public @Nullable RelOptCost getLowerBoundCost(RelNode rel, VolcanoPlanner planner) {
    for (;;) {
      try {
        return lowerBoundCostHandler.getLowerBoundCost(rel, this, planner);
      } catch (JaninoRelMetadataProvider.NoHandler e) {
        lowerBoundCostHandler =
            revise(e.relClass, BuiltInMetadata.LowerBoundCost.DEF);
      }
    }
  }
}
