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
package org.eigenbase.rel.metadata;

import java.util.*;

import org.eigenbase.rel.*;
import org.eigenbase.relopt.*;
import org.eigenbase.rex.*;
import org.eigenbase.sql.*;
import org.eigenbase.stat.*;

import com.google.common.collect.Iterables;

/**
 * RelMetadataQuery provides a strongly-typed facade on top of {@link
 * RelMetadataProvider} for the set of relational expression metadata queries
 * defined as standard within Eigenbase. The Javadoc on these methods serves as
 * their primary specification.
 *
 * <p>To add a new standard query <code>Xyz</code> to this interface, follow
 * these steps:
 *
 * <ol>
 * <li>Add a static method <code>getXyz</code> specification to this class.
 * <li>Add unit tests to {@code org.eigenbase.test.RelMetadataTest}.
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
   * Returns statistics for a relational expression. These statistics include
   * features such as row counts, or column distributions. Stats are typically
   * collected by sampling a table. They might also be inferred from a rel's
   * history. Certain rels, such as filters, might generate stats from their
   * inputs.
   *
   * @param rel the relational expression.
   * @return a statistics object, if statistics are available, or null
   * otherwise
   */
  @Deprecated
  public static RelStatSource getStatistics(RelNode rel) {
    throw new UnsupportedOperationException();
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
   * @see #getColumnOrigins(org.eigenbase.rel.RelNode, int)
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
  public static Set<BitSet> getUniqueKeys(RelNode rel) {
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
  public static Set<BitSet> getUniqueKeys(RelNode rel, boolean ignoreNulls) {
    final BuiltInMetadata.UniqueKeys metadata =
        rel.metadata(BuiltInMetadata.UniqueKeys.class);
    return metadata.getUniqueKeys(ignoreNulls);
  }

  /**
   * Returns the
   * {@link BuiltInMetadata.ColumnUniqueness#areColumnsUnique(BitSet, boolean)}
   * statistic.
   *
   * @param rel     the relational expression
   * @param columns column mask representing the subset of columns for which
   *                uniqueness will be determined
   * @return true or false depending on whether the columns are unique, or
   * null if not enough information is available to make that determination
   */
  public static Boolean areColumnsUnique(RelNode rel, BitSet columns) {
    final BuiltInMetadata.ColumnUniqueness metadata =
        rel.metadata(BuiltInMetadata.ColumnUniqueness.class);
    return metadata.areColumnsUnique(columns, false);
  }

  /**
   * Returns the
   * {@link BuiltInMetadata.ColumnUniqueness#areColumnsUnique(BitSet, boolean)}
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
  public static Boolean areColumnsUnique(RelNode rel, BitSet columns,
      boolean ignoreNulls) {
    final BuiltInMetadata.ColumnUniqueness metadata =
        rel.metadata(BuiltInMetadata.ColumnUniqueness.class);
    return metadata.areColumnsUnique(columns, ignoreNulls);
  }

  /**
   * Returns the
   * {@link BuiltInMetadata.PopulationSize#getPopulationSize(BitSet)}
   * statistic.
   *
   * @param rel      the relational expression
   * @param groupKey column mask representing the subset of columns for which
   *                 the row count will be determined
   * @return distinct row count for the given groupKey, or null if no reliable
   * estimate can be determined
   */
  public static Double getPopulationSize(RelNode rel, BitSet groupKey) {
    final BuiltInMetadata.PopulationSize metadata =
        rel.metadata(BuiltInMetadata.PopulationSize.class);
    Double result = metadata.getPopulationSize(groupKey);
    return validateResult(result);
  }

  /**
   * Returns the
   * {@link BuiltInMetadata.DistinctRowCount#getDistinctRowCount(BitSet, RexNode)}
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
      BitSet groupKey,
      RexNode predicate) {
    final BuiltInMetadata.DistinctRowCount metadata =
        rel.metadata(BuiltInMetadata.DistinctRowCount.class);
    Double result = metadata.getDistinctRowCount(groupKey, predicate);
    return validateResult(result);
  }

  /**
   * Returns the
   * {@link org.eigenbase.rel.metadata.BuiltInMetadata.Predicates#getPredicates()}
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
