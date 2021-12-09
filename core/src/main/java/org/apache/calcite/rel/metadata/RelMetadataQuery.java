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
import java.util.function.Function;
import java.util.function.Supplier;

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
  @Deprecated
  private static final RelMetadataQuery PROTOTYPE =
      new RelMetadataQuery(LegacyJaninoMetadataHandlerProvider.INSTANCE);

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
   * Creates the instance using the Handler.classualt prototype.
   */
  @Deprecated
  protected RelMetadataQuery() {
    this(PROTOTYPE);
  }

  /** Creates and initializes the instance that will serve as a prototype for
   * all other instances. */
  protected RelMetadataQuery(MetadataHandlerProvider metadataHandlerProvider) {
    super(metadataHandlerProvider);
    this.collationHandler =
        metadataHandlerProvider.initialHandler(BuiltInMetadata.Collation.Handler.class);
    this.columnOriginHandler =
        metadataHandlerProvider.initialHandler(BuiltInMetadata.ColumnOrigin.Handler.class);
    this.expressionLineageHandler =
        metadataHandlerProvider.initialHandler(BuiltInMetadata.ExpressionLineage.Handler.class);
    this.tableReferencesHandler =
        metadataHandlerProvider.initialHandler(BuiltInMetadata.TableReferences.Handler.class);
    this.columnUniquenessHandler =
        metadataHandlerProvider.initialHandler(BuiltInMetadata.ColumnUniqueness.Handler.class);
    this.cumulativeCostHandler =
        metadataHandlerProvider.initialHandler(BuiltInMetadata.CumulativeCost.Handler.class);
    this.distinctRowCountHandler =
        metadataHandlerProvider.initialHandler(BuiltInMetadata.DistinctRowCount.Handler.class);
    this.distributionHandler =
        metadataHandlerProvider.initialHandler(BuiltInMetadata.Distribution.Handler.class);
    this.explainVisibilityHandler =
        metadataHandlerProvider.initialHandler(BuiltInMetadata.ExplainVisibility.Handler.class);
    this.maxRowCountHandler =
        metadataHandlerProvider.initialHandler(BuiltInMetadata.MaxRowCount.Handler.class);
    this.minRowCountHandler =
        metadataHandlerProvider.initialHandler(BuiltInMetadata.MinRowCount.Handler.class);
    this.memoryHandler =
        metadataHandlerProvider.initialHandler(BuiltInMetadata.Memory.Handler.class);
    this.nonCumulativeCostHandler =
        metadataHandlerProvider.initialHandler(BuiltInMetadata.NonCumulativeCost.Handler.class);
    this.parallelismHandler =
        metadataHandlerProvider.initialHandler(BuiltInMetadata.Parallelism.Handler.class);
    this.percentageOriginalRowsHandler =
        metadataHandlerProvider.initialHandler(
            BuiltInMetadata.PercentageOriginalRows.Handler.class);
    this.populationSizeHandler =
        metadataHandlerProvider.initialHandler(BuiltInMetadata.PopulationSize.Handler.class);
    this.predicatesHandler =
        metadataHandlerProvider.initialHandler(BuiltInMetadata.Predicates.Handler.class);
    this.allPredicatesHandler =
        metadataHandlerProvider.initialHandler(BuiltInMetadata.AllPredicates.Handler.class);
    this.nodeTypesHandler =
        metadataHandlerProvider.initialHandler(BuiltInMetadata.NodeTypes.Handler.class);
    this.rowCountHandler =
        metadataHandlerProvider.initialHandler(BuiltInMetadata.RowCount.Handler.class);
    this.selectivityHandler =
        metadataHandlerProvider.initialHandler(BuiltInMetadata.Selectivity.Handler.class);
    this.sizeHandler =
        metadataHandlerProvider.initialHandler(BuiltInMetadata.Size.Handler.class);
    this.uniqueKeysHandler =
        metadataHandlerProvider.initialHandler(BuiltInMetadata.UniqueKeys.Handler.class);
    this.lowerBoundCostHandler =
        metadataHandlerProvider.initialHandler(BuiltInMetadata.LowerBoundCost.Handler.class);
  }

  protected RelMetadataQuery(RelMetadataQuery prototype) {
    super(prototype.metadataHandlerProvider);
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
    return new RelMetadataQuery(PROTOTYPE);
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
        nodeTypesHandler = metadataHandlerProvider.revise(BuiltInMetadata.NodeTypes.Handler.class);
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
        rowCountHandler = metadataHandlerProvider.revise(BuiltInMetadata.RowCount.Handler.class);
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
        maxRowCountHandler =
            metadataHandlerProvider.revise(BuiltInMetadata.MaxRowCount.Handler.class);
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
        minRowCountHandler =
            metadataHandlerProvider.revise(BuiltInMetadata.MinRowCount.Handler.class);
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
        cumulativeCostHandler =
            metadataHandlerProvider.revise(BuiltInMetadata.CumulativeCost.Handler.class);
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
        nonCumulativeCostHandler =
            metadataHandlerProvider.revise(BuiltInMetadata.NonCumulativeCost.Handler.class);
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
            metadataHandlerProvider.revise(BuiltInMetadata.PercentageOriginalRows.Handler.class);
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
        columnOriginHandler =
            metadataHandlerProvider.revise(BuiltInMetadata.ColumnOrigin.Handler.class);
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
        expressionLineageHandler =
            metadataHandlerProvider.revise(BuiltInMetadata.ExpressionLineage.Handler.class);
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
        tableReferencesHandler =
            metadataHandlerProvider.revise(BuiltInMetadata.TableReferences.Handler.class);
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
        selectivityHandler =
            metadataHandlerProvider.revise(BuiltInMetadata.Selectivity.Handler.class);
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
   * (whereas empty set indicates Handler.classinitely no keys at all)
   */
  public @Nullable Set<ImmutableBitSet> getUniqueKeys(RelNode rel,
      boolean ignoreNulls) {
    for (;;) {
      try {
        return uniqueKeysHandler.getUniqueKeys(rel, this, ignoreNulls);
      } catch (MetadataHandlerProvider.NoHandler e) {
        uniqueKeysHandler =
            metadataHandlerProvider.revise(BuiltInMetadata.UniqueKeys.Handler.class);
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
        columnUniquenessHandler =
            metadataHandlerProvider.revise(BuiltInMetadata.ColumnUniqueness.Handler.class);
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
        collationHandler =
            metadataHandlerProvider.revise(BuiltInMetadata.Collation.Handler.class);
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
        distributionHandler =
            metadataHandlerProvider.revise(BuiltInMetadata.Distribution.Handler.class);
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
        populationSizeHandler =
            metadataHandlerProvider.revise(BuiltInMetadata.PopulationSize.Handler.class);
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
        sizeHandler = metadataHandlerProvider.revise(BuiltInMetadata.Size.Handler.class);
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
        sizeHandler = metadataHandlerProvider.revise(BuiltInMetadata.Size.Handler.class);
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
        parallelismHandler =
            metadataHandlerProvider.revise(BuiltInMetadata.Parallelism.Handler.class);
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
        parallelismHandler =
            metadataHandlerProvider.revise(BuiltInMetadata.Parallelism.Handler.class);
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
        memoryHandler = metadataHandlerProvider.revise(BuiltInMetadata.Memory.Handler.class);
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
        memoryHandler = metadataHandlerProvider.revise(BuiltInMetadata.Memory.Handler.class);
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
        memoryHandler = metadataHandlerProvider.revise(BuiltInMetadata.Memory.Handler.class);
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
        distinctRowCountHandler =
            metadataHandlerProvider.revise(BuiltInMetadata.DistinctRowCount.Handler.class);
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
        predicatesHandler =
            metadataHandlerProvider.revise(BuiltInMetadata.Predicates.Handler.class);
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
        allPredicatesHandler =
            metadataHandlerProvider.revise(BuiltInMetadata.AllPredicates.Handler.class);
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
   * Handler.classaults to true
   */
  public Boolean isVisibleInExplain(RelNode rel,
      SqlExplainLevel explainLevel) {
    for (;;) {
      try {
        Boolean b = explainVisibilityHandler.isVisibleInExplain(rel, this,
            explainLevel);
        return b == null || b;
      } catch (MetadataHandlerProvider.NoHandler e) {
        explainVisibilityHandler =
            metadataHandlerProvider.revise(BuiltInMetadata.ExplainVisibility.Handler.class);
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
        distributionHandler =
            metadataHandlerProvider.revise(BuiltInMetadata.Distribution.Handler.class);
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
        lowerBoundCostHandler =
            metadataHandlerProvider.revise(BuiltInMetadata.LowerBoundCost.Handler.class);
      }
    }
  }

  public static Builder<RelMetadataQuery> builder() {
    return new Builder<>(RelMetadataQuery::new, RelMetadataQuery::new,
        DefaultRelMetadataProvider.INSTANCE);
  }

  public static <MDQ extends RelMetadataQuery> Builder<MDQ> builder(
      Function<MetadataHandlerProvider, MDQ> createPrototypeQuery,
      Function<MDQ, MDQ> createQueryFromPrototype) {

    return new Builder<>(createPrototypeQuery, createQueryFromPrototype,
        DefaultRelMetadataProvider.INSTANCE);
  }

  /**
   * Used to create custom {@link RelMetadataQuery} and bind {@link RelMetadataProvider}.
   * @param <MDQ> Custom Metadata query type
   */
  public static class Builder<MDQ extends RelMetadataQuery> {
    private final Function<MetadataHandlerProvider, MDQ> createPrototypeQuery;
    private final Function<MDQ, MDQ> createQueryFromPrototype;
    private final RelMetadataProvider relMetadataProvider;


    private Builder(
        Function<MetadataHandlerProvider, MDQ> prototypeFunction,
        Function<MDQ, MDQ> queryFunction,
        RelMetadataProvider provider) {
      this.createPrototypeQuery = prototypeFunction;
      this.createQueryFromPrototype = queryFunction;
      this.relMetadataProvider = provider;
    }

    public Builder<MDQ> withProviders(RelMetadataProvider...relMetadataProviders) {
      RelMetadataProvider newProvider = ChainedRelMetadataProvider.of(
          ImmutableList.<RelMetadataProvider>builder()
              .add(relMetadataProviders)
              .add(this.relMetadataProvider)
              .build());
      return new Builder<>(createPrototypeQuery, createQueryFromPrototype,
          newProvider);
    }

    public Supplier<RelMetadataQuery> createSupplier() {
      MDQ prototype = createPrototypeQuery.apply(
          new JaninoMetadataHandlerProvider(relMetadataProvider));
      return () -> createQueryFromPrototype.apply(prototype);
    }
  }
}
