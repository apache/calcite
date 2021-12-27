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

import com.google.common.base.Supplier;
import com.google.common.base.Suppliers;
import com.google.common.collect.ImmutableList;
import com.google.common.collect.Iterables;
import com.google.common.collect.Multimap;

import org.checkerframework.checker.nullness.qual.Nullable;

import java.util.Collections;
import java.util.List;
import java.util.Set;

import static org.apache.calcite.linq4j.Nullness.castNonNull;

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
  // An empty prototype. Only initialize on first use.
  private static final Supplier<RelMetadataQuery> EMPTY =
      Suppliers.memoize(() -> new RelMetadataQuery(false));

  private BuiltInMetadata.CollationHandler collationHandler;
  private BuiltInMetadata.ColumnOriginHandler columnOriginHandler;
  private BuiltInMetadata.ExpressionLineageHandler expressionLineageHandler;
  private BuiltInMetadata.TableReferencesHandler tableReferencesHandler;
  private BuiltInMetadata.ColumnUniquenessHandler columnUniquenessHandler;
  private BuiltInMetadata.CumulativeCostHandler cumulativeCostHandler;
  private BuiltInMetadata.DistinctRowCountHandler distinctRowCountHandler;
  private BuiltInMetadata.DistributionHandler distributionHandler;
  private BuiltInMetadata.ExplainVisibilityHandler explainVisibilityHandler;
  private BuiltInMetadata.MaxRowCountHandler maxRowCountHandler;
  private BuiltInMetadata.MinRowCountHandler minRowCountHandler;
  private BuiltInMetadata.MemoryHandler memoryHandler;
  private BuiltInMetadata.NonCumulativeCostHandler nonCumulativeCostHandler;
  private BuiltInMetadata.ParallelismHandler parallelismHandler;
  private BuiltInMetadata.PercentageOriginalRowsHandler percentageOriginalRowsHandler;
  private BuiltInMetadata.PopulationSizeHandler populationSizeHandler;
  private BuiltInMetadata.PredicatesHandler predicatesHandler;
  private BuiltInMetadata.AllPredicatesHandler allPredicatesHandler;
  private BuiltInMetadata.NodeTypesHandler nodeTypesHandler;
  private BuiltInMetadata.RowCountHandler rowCountHandler;
  private BuiltInMetadata.SelectivityHandler selectivityHandler;
  private BuiltInMetadata.SizeHandler sizeHandler;
  private BuiltInMetadata.UniqueKeysHandler uniqueKeysHandler;
  private BuiltInMetadata.LowerBoundCostHandler lowerBoundCostHandler;

  /**
   * Creates the instance with {@link JaninoRelMetadataProvider} instance
   * from {@link #THREAD_PROVIDERS} and {@link #EMPTY} as a prototype.
   */
  protected RelMetadataQuery() {
    this(castNonNull(THREAD_PROVIDERS.get()), EMPTY.get());
  }

  /**
   * Create a RelMetadataQuery with a given {@link MetadataHandlerProvider}.
   * @param provider The provider to use for construction.
   */
  public RelMetadataQuery(MetadataHandlerProvider provider) {
    super(provider);
    this.collationHandler = provider.handler(BuiltInMetadata.CollationHandler.class);
    this.columnOriginHandler = provider.handler(BuiltInMetadata.ColumnOriginHandler.class);
    this.expressionLineageHandler =
        provider.handler(BuiltInMetadata.ExpressionLineageHandler.class);
    this.tableReferencesHandler = provider.handler(BuiltInMetadata.TableReferencesHandler.class);
    this.columnUniquenessHandler = provider.handler(BuiltInMetadata.ColumnUniquenessHandler.class);
    this.cumulativeCostHandler = provider.handler(BuiltInMetadata.CumulativeCostHandler.class);
    this.distinctRowCountHandler = provider.handler(BuiltInMetadata.DistinctRowCountHandler.class);
    this.distributionHandler = provider.handler(BuiltInMetadata.DistributionHandler.class);
    this.explainVisibilityHandler =
        provider.handler(BuiltInMetadata.ExplainVisibilityHandler.class);
    this.maxRowCountHandler = provider.handler(BuiltInMetadata.MaxRowCountHandler.class);
    this.minRowCountHandler = provider.handler(BuiltInMetadata.MinRowCountHandler.class);
    this.memoryHandler = provider.handler(BuiltInMetadata.MemoryHandler.class);
    this.nonCumulativeCostHandler =
        provider.handler(BuiltInMetadata.NonCumulativeCostHandler.class);
    this.parallelismHandler = provider.handler(BuiltInMetadata.ParallelismHandler.class);
    this.percentageOriginalRowsHandler =
        provider.handler(BuiltInMetadata.PercentageOriginalRowsHandler.class);
    this.populationSizeHandler = provider.handler(BuiltInMetadata.PopulationSizeHandler.class);
    this.predicatesHandler = provider.handler(BuiltInMetadata.PredicatesHandler.class);
    this.allPredicatesHandler = provider.handler(BuiltInMetadata.AllPredicatesHandler.class);
    this.nodeTypesHandler = provider.handler(BuiltInMetadata.NodeTypesHandler.class);
    this.rowCountHandler = provider.handler(BuiltInMetadata.RowCountHandler.class);
    this.selectivityHandler = provider.handler(BuiltInMetadata.SelectivityHandler.class);
    this.sizeHandler = provider.handler(BuiltInMetadata.SizeHandler.class);
    this.uniqueKeysHandler = provider.handler(BuiltInMetadata.UniqueKeysHandler.class);
    this.lowerBoundCostHandler = provider.handler(BuiltInMetadata.LowerBoundCostHandler.class);
  }

  /** Creates and initializes the instance that will serve as a prototype for
   * all other instances in the Janino case. */
  @SuppressWarnings("deprecation")
  private RelMetadataQuery(@SuppressWarnings("unused") boolean dummy) {
    super(null);
    this.collationHandler = initialHandler(BuiltInMetadata.CollationHandler.class);
    this.columnOriginHandler = initialHandler(BuiltInMetadata.ColumnOriginHandler.class);
    this.expressionLineageHandler = initialHandler(BuiltInMetadata.ExpressionLineageHandler.class);
    this.tableReferencesHandler = initialHandler(BuiltInMetadata.TableReferencesHandler.class);
    this.columnUniquenessHandler = initialHandler(BuiltInMetadata.ColumnUniquenessHandler.class);
    this.cumulativeCostHandler = initialHandler(BuiltInMetadata.CumulativeCostHandler.class);
    this.distinctRowCountHandler = initialHandler(BuiltInMetadata.DistinctRowCountHandler.class);
    this.distributionHandler = initialHandler(BuiltInMetadata.DistributionHandler.class);
    this.explainVisibilityHandler = initialHandler(BuiltInMetadata.ExplainVisibilityHandler.class);
    this.maxRowCountHandler = initialHandler(BuiltInMetadata.MaxRowCountHandler.class);
    this.minRowCountHandler = initialHandler(BuiltInMetadata.MinRowCountHandler.class);
    this.memoryHandler = initialHandler(BuiltInMetadata.MemoryHandler.class);
    this.nonCumulativeCostHandler = initialHandler(BuiltInMetadata.NonCumulativeCostHandler.class);
    this.parallelismHandler = initialHandler(BuiltInMetadata.ParallelismHandler.class);
    this.percentageOriginalRowsHandler =
        initialHandler(BuiltInMetadata.PercentageOriginalRowsHandler.class);
    this.populationSizeHandler = initialHandler(BuiltInMetadata.PopulationSizeHandler.class);
    this.predicatesHandler = initialHandler(BuiltInMetadata.PredicatesHandler.class);
    this.allPredicatesHandler = initialHandler(BuiltInMetadata.AllPredicatesHandler.class);
    this.nodeTypesHandler = initialHandler(BuiltInMetadata.NodeTypesHandler.class);
    this.rowCountHandler = initialHandler(BuiltInMetadata.RowCountHandler.class);
    this.selectivityHandler = initialHandler(BuiltInMetadata.SelectivityHandler.class);
    this.sizeHandler = initialHandler(BuiltInMetadata.SizeHandler.class);
    this.uniqueKeysHandler = initialHandler(BuiltInMetadata.UniqueKeysHandler.class);
    this.lowerBoundCostHandler = initialHandler(BuiltInMetadata.LowerBoundCostHandler.class);
  }

  private RelMetadataQuery(
      MetadataHandlerProvider metadataHandlerProvider,
      RelMetadataQuery prototype) {
    super(metadataHandlerProvider);
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
      } catch (MetadataHandlerProvider.NoHandler e) {
        nodeTypesHandler = revise(BuiltInMetadata.NodeTypesHandler.class);
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
      } catch (MetadataHandlerProvider.NoHandler e) {
        rowCountHandler = revise(BuiltInMetadata.RowCountHandler.class);
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
      } catch (MetadataHandlerProvider.NoHandler e) {
        maxRowCountHandler = revise(BuiltInMetadata.MaxRowCountHandler.class);
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
      } catch (MetadataHandlerProvider.NoHandler e) {
        minRowCountHandler = revise(BuiltInMetadata.MinRowCountHandler.class);
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
      } catch (MetadataHandlerProvider.NoHandler e) {
        cumulativeCostHandler = revise(BuiltInMetadata.CumulativeCostHandler.class);
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
      } catch (MetadataHandlerProvider.NoHandler e) {
        nonCumulativeCostHandler = revise(BuiltInMetadata.NonCumulativeCostHandler.class);
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
      } catch (MetadataHandlerProvider.NoHandler e) {
        percentageOriginalRowsHandler =
            revise(BuiltInMetadata.PercentageOriginalRowsHandler.class);
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
   * determined (whereas empty set indicates Handler.classinitely no origin columns at
   * all)
   */
  public @Nullable Set<RelColumnOrigin> getColumnOrigins(RelNode rel, int column) {
    for (;;) {
      try {
        return columnOriginHandler.getColumnOrigins(rel, this, column);
      } catch (MetadataHandlerProvider.NoHandler e) {
        columnOriginHandler = revise(BuiltInMetadata.ColumnOriginHandler.class);
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
      } catch (MetadataHandlerProvider.NoHandler e) {
        expressionLineageHandler = revise(BuiltInMetadata.ExpressionLineageHandler.class);
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
      } catch (MetadataHandlerProvider.NoHandler e) {
        tableReferencesHandler = revise(BuiltInMetadata.TableReferencesHandler.class);
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
      } catch (MetadataHandlerProvider.NoHandler e) {
        selectivityHandler = revise(BuiltInMetadata.SelectivityHandler.class);
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
   * (whereas empty set indicates Handler.classinitely no keys at all)
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
      } catch (MetadataHandlerProvider.NoHandler e) {
        uniqueKeysHandler = revise(BuiltInMetadata.UniqueKeysHandler.class);
      }
    }
  }

  /**
   * Returns whether the rows of a given relational expression are distinct,
   * optionally ignoring NULL values.
   *
   * <p>This is derived by applying the
   * {@link BuiltInMetadata.ColumnUniqueness#areColumnsUnique(org.apache.calcite.util.ImmutableBitSet, boolean)}
   * statistic over all columns. If
   * {@link BuiltInMetadata.MaxRowCount#getMaxRowCount()}
   * is less than or equal to one, we shortcut the process and declare the rows
   * unique.
   *
   * @param rel     the relational expression
   * @param ignoreNulls if true, ignore null values when determining column
   *                    uniqueness
   *
   * @return whether the rows are unique, or
   * null if not enough information is available to make that determination
   */
  public @Nullable Boolean areRowsUnique(RelNode rel, boolean ignoreNulls) {
    Double maxRowCount = this.getMaxRowCount(rel);
    if (maxRowCount != null && maxRowCount <= 1D) {
      return true;
    }
    final ImmutableBitSet columns =
        ImmutableBitSet.range(rel.getRowType().getFieldCount());
    return areColumnsUnique(rel, columns, ignoreNulls);
  }

  /**
   * Returns whether the rows of a given relational expression are distinct.
   *
   * <p>Derived by calling {@link #areRowsUnique(RelNode, boolean)}.
   *
   * @param rel     the relational expression
   *
   * @return whether the rows are unique, or
   * null if not enough information is available to make that determination
   */
  public @Nullable Boolean areRowsUnique(RelNode rel) {
    return areRowsUnique(rel, false);
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
      } catch (MetadataHandlerProvider.NoHandler e) {
        columnUniquenessHandler = revise(BuiltInMetadata.ColumnUniquenessHandler.class);
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
      } catch (MetadataHandlerProvider.NoHandler e) {
        collationHandler = revise(BuiltInMetadata.CollationHandler.class);
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
      } catch (MetadataHandlerProvider.NoHandler e) {
        distributionHandler = revise(BuiltInMetadata.DistributionHandler.class);
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
      } catch (MetadataHandlerProvider.NoHandler e) {
        populationSizeHandler = revise(BuiltInMetadata.PopulationSizeHandler.class);
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
      } catch (MetadataHandlerProvider.NoHandler e) {
        sizeHandler = revise(BuiltInMetadata.SizeHandler.class);
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
      } catch (MetadataHandlerProvider.NoHandler e) {
        sizeHandler = revise(BuiltInMetadata.SizeHandler.class);
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
      } catch (MetadataHandlerProvider.NoHandler e) {
        parallelismHandler = revise(BuiltInMetadata.ParallelismHandler.class);
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
      } catch (MetadataHandlerProvider.NoHandler e) {
        parallelismHandler = revise(BuiltInMetadata.ParallelismHandler.class);
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
      } catch (MetadataHandlerProvider.NoHandler e) {
        memoryHandler = revise(BuiltInMetadata.MemoryHandler.class);
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
      } catch (MetadataHandlerProvider.NoHandler e) {
        memoryHandler = revise(BuiltInMetadata.MemoryHandler.class);
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
      } catch (MetadataHandlerProvider.NoHandler e) {
        memoryHandler = revise(BuiltInMetadata.MemoryHandler.class);
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
      } catch (MetadataHandlerProvider.NoHandler e) {
        distinctRowCountHandler = revise(BuiltInMetadata.DistinctRowCountHandler.class);
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
      } catch (MetadataHandlerProvider.NoHandler e) {
        predicatesHandler = revise(BuiltInMetadata.PredicatesHandler.class);
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
      } catch (MetadataHandlerProvider.NoHandler e) {
        allPredicatesHandler = revise(BuiltInMetadata.AllPredicatesHandler.class);
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
      } catch (MetadataHandlerProvider.NoHandler e) {
        explainVisibilityHandler = revise(BuiltInMetadata.ExplainVisibilityHandler.class);
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
      } catch (MetadataHandlerProvider.NoHandler e) {
        distributionHandler = revise(BuiltInMetadata.DistributionHandler.class);
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
      } catch (MetadataHandlerProvider.NoHandler e) {
        lowerBoundCostHandler = revise(BuiltInMetadata.LowerBoundCostHandler.class);
      }
    }
  }
}
