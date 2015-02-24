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
import org.apache.calcite.rel.RelCollation;
import org.apache.calcite.rel.RelDistribution;
import org.apache.calcite.rel.RelNode;
import org.apache.calcite.rex.RexNode;
import org.apache.calcite.sql.SqlExplainLevel;
import org.apache.calcite.util.ImmutableBitSet;

import com.google.common.collect.ImmutableList;
import com.google.common.collect.Iterables;

import java.util.Collections;
import java.util.List;
import java.util.Set;

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
 * instance of {@link DefaultRelMetadataProvider}, prepending it to the default
 * providers. Then supply that instance to the planner via the appropriate
 * plugin mechanism.
 */
public abstract class RelMetadataQuery {
  //~ Methods ----------------------------------------------------------------

  /**
   * Returns the
   * {@link BuiltInMetadata.RowCount#getRowCount()}
   * statistic.
   *
   * @param rel the relational expression
   * @return estimated row count, or null if no reliable estimate can be
   * determined
   */
  public static Double getRowCount(RelNode rel) {
    final BuiltInMetadata.RowCount metadata =
        rel.metadata(BuiltInMetadata.RowCount.class);
    Double result = metadata.getRowCount();
    return validateResult(result);
  }

  /**
   * Returns the
   * {@link BuiltInMetadata.CumulativeCost#getCumulativeCost()}
   * statistic.
   *
   * @param rel the relational expression
   * @return estimated cost, or null if no reliable estimate can be determined
   */
  public static RelOptCost getCumulativeCost(RelNode rel) {
    final BuiltInMetadata.CumulativeCost metadata =
        rel.metadata(BuiltInMetadata.CumulativeCost.class);
    return metadata.getCumulativeCost();
  }

  /**
   * Returns the
   * {@link BuiltInMetadata.NonCumulativeCost#getNonCumulativeCost()}
   * statistic.
   *
   * @param rel the relational expression
   * @return estimated cost, or null if no reliable estimate can be determined
   */
  public static RelOptCost getNonCumulativeCost(RelNode rel) {
    final BuiltInMetadata.NonCumulativeCost metadata =
        rel.metadata(BuiltInMetadata.NonCumulativeCost.class);
    return metadata.getNonCumulativeCost();
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
  public static Double getPercentageOriginalRows(RelNode rel) {
    final BuiltInMetadata.PercentageOriginalRows metadata =
        rel.metadata(BuiltInMetadata.PercentageOriginalRows.class);
    Double result = metadata.getPercentageOriginalRows();
    assert isPercentage(result, true);
    return result;
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
  public static Set<RelColumnOrigin> getColumnOrigins(RelNode rel, int column) {
    final BuiltInMetadata.ColumnOrigin metadata =
        rel.metadata(BuiltInMetadata.ColumnOrigin.class);
    return metadata.getColumnOrigins(column);
  }

  /**
   * Determines the origin of a column, provided the column maps to a single
   * column that isn't derived.
   *
   * @see #getColumnOrigins(org.apache.calcite.rel.RelNode, int)
   *
   * @param rel the RelNode of the column
   * @param column the offset of the column whose origin we are trying to
   * determine
   *
   * @return the origin of a column provided it's a simple column; otherwise,
   * returns null
   */
  public static RelColumnOrigin getColumnOrigin(RelNode rel, int column) {
    final Set<RelColumnOrigin> origins = getColumnOrigins(rel, column);
    if (origins == null || origins.size() != 1) {
      return null;
    }
    final RelColumnOrigin origin = Iterables.getOnlyElement(origins);
    return origin.isDerived() ? null : origin;
  }

  /**
   * Determines the origin of a {@link RelNode}, provided it maps to a single
   * table, optionally with filtering and projection.
   *
   * @param rel the RelNode
   *
   * @return the table, if the RelNode is a simple table; otherwise null
   */
  public static RelOptTable getTableOrigin(RelNode rel) {
    // Determine the simple origin of the first column in the
    // RelNode.  If it's simple, then that means that the underlying
    // table is also simple, even if the column itself is derived.
    final Set<RelColumnOrigin> colOrigins =
        getColumnOrigins(rel, 0);
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
   *                  rel's output
   * @return estimated selectivity (between 0.0 and 1.0), or null if no
   * reliable estimate can be determined
   */
  public static Double getSelectivity(RelNode rel, RexNode predicate) {
    final BuiltInMetadata.Selectivity metadata =
        rel.metadata(BuiltInMetadata.Selectivity.class);
    Double result = metadata.getSelectivity(predicate);
    assert isPercentage(result, true);
    return result;
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
  public static Set<ImmutableBitSet> getUniqueKeys(RelNode rel) {
    final BuiltInMetadata.UniqueKeys metadata =
        rel.metadata(BuiltInMetadata.UniqueKeys.class);
    return metadata.getUniqueKeys(false);
  }

  /**
   * Returns the
   * {@link BuiltInMetadata.UniqueKeys#getUniqueKeys(boolean)}
   * statistic.
   *
   * @param rel         the relational expression
   * @param ignoreNulls if true, ignore null values when determining
   *                    whether the keys are unique
   * @return set of keys, or null if this information cannot be determined
   * (whereas empty set indicates definitely no keys at all)
   */
  public static Set<ImmutableBitSet> getUniqueKeys(RelNode rel,
      boolean ignoreNulls) {
    final BuiltInMetadata.UniqueKeys metadata =
        rel.metadata(BuiltInMetadata.UniqueKeys.class);
    return metadata.getUniqueKeys(ignoreNulls);
  }

  /**
   * Returns the
   * {@link BuiltInMetadata.ColumnUniqueness#areColumnsUnique(org.apache.calcite.util.ImmutableBitSet, boolean)}
   * statistic.
   *
   * @param rel     the relational expression
   * @param columns column mask representing the subset of columns for which
   *                uniqueness will be determined
   * @return true or false depending on whether the columns are unique, or
   * null if not enough information is available to make that determination
   */
  public static Boolean areColumnsUnique(RelNode rel, ImmutableBitSet columns) {
    final BuiltInMetadata.ColumnUniqueness metadata =
        rel.metadata(BuiltInMetadata.ColumnUniqueness.class);
    return metadata.areColumnsUnique(columns, false);
  }

  /**
   * Returns the
   * {@link BuiltInMetadata.ColumnUniqueness#areColumnsUnique(org.apache.calcite.util.ImmutableBitSet, boolean)}
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
  public static Boolean areColumnsUnique(RelNode rel, ImmutableBitSet columns,
      boolean ignoreNulls) {
    final BuiltInMetadata.ColumnUniqueness metadata =
        rel.metadata(BuiltInMetadata.ColumnUniqueness.class);
    return metadata.areColumnsUnique(columns, ignoreNulls);
  }

  /**
   * Returns the
   * {@link org.apache.calcite.rel.metadata.BuiltInMetadata.Collation#collations()}
   * statistic.
   *
   * @param rel         the relational expression
   * @return List of sorted column combinations, or
   * null if not enough information is available to make that determination
   */
  public static ImmutableList<RelCollation> collations(RelNode rel) {
    final BuiltInMetadata.Collation metadata =
        rel.metadata(BuiltInMetadata.Collation.class);
    return metadata.collations();
  }

  /**
   * Returns the
   * {@link org.apache.calcite.rel.metadata.BuiltInMetadata.Distribution#distribution()}
   * statistic.
   *
   * @param rel         the relational expression
   * @return List of sorted column combinations, or
   * null if not enough information is available to make that determination
   */
  public static RelDistribution distribution(RelNode rel) {
    final BuiltInMetadata.Distribution metadata =
        rel.metadata(BuiltInMetadata.Distribution.class);
    return metadata.distribution();
  }

  /**
   * Returns the
   * {@link BuiltInMetadata.PopulationSize#getPopulationSize(org.apache.calcite.util.ImmutableBitSet)}
   * statistic.
   *
   * @param rel      the relational expression
   * @param groupKey column mask representing the subset of columns for which
   *                 the row count will be determined
   * @return distinct row count for the given groupKey, or null if no reliable
   * estimate can be determined
   *
   */
  public static Double getPopulationSize(RelNode rel,
      ImmutableBitSet groupKey) {
    final BuiltInMetadata.PopulationSize metadata =
        rel.metadata(BuiltInMetadata.PopulationSize.class);
    Double result = metadata.getPopulationSize(groupKey);
    return validateResult(result);
  }

  /**
   * Returns the
   * {@link org.apache.calcite.rel.metadata.BuiltInMetadata.Size#averageRowSize()}
   * statistic.
   *
   * @param rel      the relational expression
   * @return average size of a row, in bytes, or null if not known
     */
  public static Double getAverageRowSize(RelNode rel) {
    final BuiltInMetadata.Size metadata =
        rel.metadata(BuiltInMetadata.Size.class);
    return metadata.averageRowSize();
  }

  /**
   * Returns the
   * {@link org.apache.calcite.rel.metadata.BuiltInMetadata.Size#averageColumnSizes()}
   * statistic.
   *
   * @param rel      the relational expression
   * @return a list containing, for each column, the average size of a column
   * value, in bytes. Each value or the entire list may be null if the
   * metadata is not available
   */
  public static List<Double> getAverageColumnSizes(RelNode rel) {
    final BuiltInMetadata.Size metadata =
        rel.metadata(BuiltInMetadata.Size.class);
    return metadata.averageColumnSizes();
  }

  /** As {@link #getAverageColumnSizes(org.apache.calcite.rel.RelNode)} but
   * never returns a null list, only ever a list of nulls. */
  public static List<Double> getAverageColumnSizesNotNull(RelNode rel) {
    final BuiltInMetadata.Size metadata =
        rel.metadata(BuiltInMetadata.Size.class);
    final List<Double> averageColumnSizes = metadata.averageColumnSizes();
    return averageColumnSizes == null
        ? Collections.<Double>nCopies(rel.getRowType().getFieldCount(), null)
        : averageColumnSizes;
  }

  /**
   * Returns the
   * {@link org.apache.calcite.rel.metadata.BuiltInMetadata.Parallelism#isPhaseTransition()}
   * statistic.
   *
   * @param rel      the relational expression
   * @return whether each physical operator implementing this relational
   * expression belongs to a different process than its inputs, or null if not
   * known
   */
  public static Boolean isPhaseTransition(RelNode rel) {
    final BuiltInMetadata.Parallelism metadata =
        rel.metadata(BuiltInMetadata.Parallelism.class);
    return metadata.isPhaseTransition();
  }

  /**
   * Returns the
   * {@link org.apache.calcite.rel.metadata.BuiltInMetadata.Parallelism#splitCount()}
   * statistic.
   *
   * @param rel      the relational expression
   * @return the number of distinct splits of the data, or null if not known
   */
  public static Integer splitCount(RelNode rel) {
    final BuiltInMetadata.Parallelism metadata =
        rel.metadata(BuiltInMetadata.Parallelism.class);
    return metadata.splitCount();
  }

  /**
   * Returns the
   * {@link org.apache.calcite.rel.metadata.BuiltInMetadata.Memory#memory()}
   * statistic.
   *
   * @param rel      the relational expression
   * @return the expected amount of memory, in bytes, required by a physical
   * operator implementing this relational expression, across all splits,
   * or null if not known
   */
  public static Double memory(RelNode rel) {
    final BuiltInMetadata.Memory metadata =
        rel.metadata(BuiltInMetadata.Memory.class);
    return metadata.memory();
  }

  /**
   * Returns the
   * {@link org.apache.calcite.rel.metadata.BuiltInMetadata.Memory#cumulativeMemoryWithinPhase()}
   * statistic.
   *
   * @param rel      the relational expression
   * @return the cumulative amount of memory, in bytes, required by the
   * physical operator implementing this relational expression, and all other
   * operators within the same phase, across all splits, or null if not known
   */
  public static Double cumulativeMemoryWithinPhase(RelNode rel) {
    final BuiltInMetadata.Memory metadata =
        rel.metadata(BuiltInMetadata.Memory.class);
    return metadata.cumulativeMemoryWithinPhase();
  }

  /**
   * Returns the
   * {@link org.apache.calcite.rel.metadata.BuiltInMetadata.Memory#cumulativeMemoryWithinPhaseSplit()}
   * statistic.
   *
   * @param rel      the relational expression
   * @return the expected cumulative amount of memory, in bytes, required by
   * the physical operator implementing this relational expression, and all
   * operators within the same phase, within each split, or null if not known
   */
  public static Double cumulativeMemoryWithinPhaseSplit(RelNode rel) {
    final BuiltInMetadata.Memory metadata =
        rel.metadata(BuiltInMetadata.Memory.class);
    return metadata.cumulativeMemoryWithinPhaseSplit();
  }

  /**
   * Returns the
   * {@link BuiltInMetadata.DistinctRowCount#getDistinctRowCount(org.apache.calcite.util.ImmutableBitSet, org.apache.calcite.rex.RexNode)}
   * statistic.
   *
   * @param rel       the relational expression
   * @param groupKey  column mask representing group by columns
   * @param predicate pre-filtered predicates
   * @return distinct row count for groupKey, filtered by predicate, or null
   * if no reliable estimate can be determined
   */
  public static Double getDistinctRowCount(
      RelNode rel,
      ImmutableBitSet groupKey,
      RexNode predicate) {
    final BuiltInMetadata.DistinctRowCount metadata =
        rel.metadata(BuiltInMetadata.DistinctRowCount.class);
    Double result = metadata.getDistinctRowCount(groupKey, predicate);
    return validateResult(result);
  }

  /**
   * Returns the
   * {@link org.apache.calcite.rel.metadata.BuiltInMetadata.Predicates#getPredicates()}
   * statistic.
   *
   * @param rel the relational expression
   * @return Predicates that can be pulled above this RelNode
   */
  public static RelOptPredicateList getPulledUpPredicates(RelNode rel) {
    final BuiltInMetadata.Predicates metadata =
        rel.metadata(BuiltInMetadata.Predicates.class);
    return metadata.getPredicates();
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
  public static boolean isVisibleInExplain(RelNode rel,
      SqlExplainLevel explainLevel) {
    final BuiltInMetadata.ExplainVisibility metadata =
        rel.metadata(BuiltInMetadata.ExplainVisibility.class);
    Boolean b = metadata.isVisibleInExplain(explainLevel);
    return b == null || b;
  }

  private static boolean isPercentage(Double result, boolean fail) {
    if (result != null) {
      final double d = result;
      if (d < 0.0) {
        assert !fail;
        return false;
      }
      if (d > 1.0) {
        assert !fail;
        return false;
      }
    }
    return true;
  }

  private static boolean isNonNegative(Double result, boolean fail) {
    if (result != null) {
      final double d = result;
      if (d < 0.0) {
        assert !fail;
        return false;
      }
    }
    return true;
  }

  private static Double validateResult(Double result) {
    if (result == null) {
      return null;
    }

    // Never let the result go below 1, as it will result in incorrect
    // calculations if the row-count is used as the denominator in a
    // division expression.  Also, cap the value at the max double value
    // to avoid calculations using infinity.
    if (result.isInfinite()) {
      result = Double.MAX_VALUE;
    }
    assert isNonNegative(result, true);
    if (result < 1.0) {
      result = 1.0;
    }
    return result;
  }
}

// End RelMetadataQuery.java
