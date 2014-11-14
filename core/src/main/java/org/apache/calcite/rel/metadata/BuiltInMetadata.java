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
import org.eigenbase.relopt.RelOptCost;
import org.eigenbase.relopt.RelOptPredicateList;
import org.eigenbase.rex.RexNode;
import org.eigenbase.sql.SqlExplainLevel;

/**
 * Contains the interfaces for several common forms of metadata.
 */
public abstract class BuiltInMetadata {

  /** Metadata about the selectivity of a predicate. */
  public interface Selectivity extends Metadata {
    /**
     * Estimates the percentage of an expression's output rows which satisfy a
     * given predicate. Returns null to indicate that no reliable estimate can
     * be produced.
     *
     * @param predicate predicate whose selectivity is to be estimated against
     *                  rel's output
     * @return estimated selectivity (between 0.0 and 1.0), or null if no
     * reliable estimate can be determined
     */
    Double getSelectivity(RexNode predicate);
  }

  /** Metadata about which combinations of columns are unique identifiers. */
  public interface UniqueKeys extends Metadata {
    /**
     * Determines the set of unique minimal keys for this expression. A key is
     * represented as a {@link BitSet}, where each bit position represents a
     * 0-based output column ordinal.
     *
     * <p>Note that {@link RelNode#isDistinct} should return {@code true} if and
     * only if at least one key is known.</p>
     *
     * <p>Nulls can be ignored if the relational expression has filtered out
     * null values.
     *
     * @param ignoreNulls if true, ignore null values when determining
     *                    whether the keys are unique
     * @return set of keys, or null if this information cannot be determined
     * (whereas empty set indicates definitely no keys at all)
     */
    Set<BitSet> getUniqueKeys(boolean ignoreNulls);
  }

  /** Metadata about whether a set of columns uniquely identifies a row. */
  public interface ColumnUniqueness extends Metadata {
    /**
     * Determines whether a specified set of columns from a specified relational
     * expression are unique.
     *
     * <p>Nulls can be ignored if the relational expression has filtered out
     * null values.</p>
     *
     * @param columns column mask representing the subset of columns for which
     *                uniqueness will be determined
     * @param ignoreNulls if true, ignore null values when determining column
     *                    uniqueness
     * @return whether the columns are unique, or
     * null if not enough information is available to make that determination
     */
    Boolean areColumnsUnique(BitSet columns, boolean ignoreNulls);
  }

  /** Metadata about the number of rows returned by a relational expression. */
  public interface RowCount extends Metadata {
    /**
     * Estimates the number of rows which will be returned by a relational
     * expression. The default implementation for this query asks the rel itself
     * via {@link RelNode#getRows}, but metadata providers can override this
     * with their own cost models.
     *
     * @return estimated row count, or null if no reliable estimate can be
     * determined
     */
    Double getRowCount();
  }

  /** Metadata about the number of distinct rows returned by a set of columns
   * in a relational expression. */
  public interface DistinctRowCount extends Metadata {
    /**
     * Estimates the number of rows which would be produced by a GROUP BY on the
     * set of columns indicated by groupKey, where the input to the GROUP BY has
     * been pre-filtered by predicate. This quantity (leaving out predicate) is
     * often referred to as cardinality (as in gender being a "low-cardinality
     * column").
     *
     * @param groupKey  column mask representing group by columns
     * @param predicate pre-filtered predicates
     * @return distinct row count for groupKey, filtered by predicate, or null
     * if no reliable estimate can be determined
     */
    Double getDistinctRowCount(BitSet groupKey, RexNode predicate);
  }

  /** Metadata about the proportion of original rows that remain in a relational
   * expression. */
  public interface PercentageOriginalRows extends Metadata {
    /**
     * Estimates the percentage of the number of rows actually produced by a
     * relational expression out of the number of rows it would produce if all
     * single-table filter conditions were removed.
     *
     * @return estimated percentage (between 0.0 and 1.0), or null if no
     * reliable estimate can be determined
     */
    Double getPercentageOriginalRows();
  }

  /** Metadata about the number of distinct values in the original source of a
   * column or set of columns. */
  public interface PopulationSize extends Metadata {
    /**
     * Estimates the distinct row count in the original source for the given
     * {@code groupKey}, ignoring any filtering being applied by the expression.
     * Typically, "original source" means base table, but for derived columns,
     * the estimate may come from a non-leaf rel such as a ProjectRel.
     *
     * @param groupKey column mask representing the subset of columns for which
     *                 the row count will be determined
     * @return distinct row count for the given groupKey, or null if no reliable
     * estimate can be determined
     */
    Double getPopulationSize(BitSet groupKey);
  }

  /** Metadata about the origins of columns. */
  public interface ColumnOrigin extends Metadata {
    /**
     * For a given output column of an expression, determines all columns of
     * underlying tables which contribute to result values. An output column may
     * have more than one origin due to expressions such as UnionRel and
     * ProjectRel. The optimizer may use this information for catalog access
     * (e.g. index availability).
     *
     * @param outputColumn 0-based ordinal for output column of interest
     * @return set of origin columns, or null if this information cannot be
     * determined (whereas empty set indicates definitely no origin columns at
     * all)
     */
    Set<RelColumnOrigin> getColumnOrigins(int outputColumn);
  }

  /** Metadata about the cost of evaluating a relational expression, including
   * all of its inputs. */
  public interface CumulativeCost extends Metadata {
    /**
     * Estimates the cost of executing a relational expression, including the
     * cost of its inputs. The default implementation for this query adds
     * {@link NonCumulativeCost#getNonCumulativeCost} to the cumulative cost of
     * each input, but metadata providers can override this with their own cost
     * models, e.g. to take into account interactions between expressions.
     *
     * @return estimated cost, or null if no reliable estimate can be
     * determined
     */
    RelOptCost getCumulativeCost();
  }

  /** Metadata about the cost of evaluating a relational expression, not
   * including its inputs. */
  public interface NonCumulativeCost extends Metadata {
    /**
     * Estimates the cost of executing a relational expression, not counting the
     * cost of its inputs. (However, the non-cumulative cost is still usually
     * dependent on the row counts of the inputs.) The default implementation
     * for this query asks the rel itself via {@link RelNode#computeSelfCost},
     * but metadata providers can override this with their own cost models.
     *
     * @return estimated cost, or null if no reliable estimate can be
     * determined
     */
    RelOptCost getNonCumulativeCost();
  }

  /** Metadata about whether a relational expression should appear in a plan. */
  public interface ExplainVisibility extends Metadata {
    /**
     * Determines whether a relational expression should be visible in EXPLAIN
     * PLAN output at a particular level of detail.
     *
     * @param explainLevel level of detail
     * @return true for visible, false for invisible
     */
    Boolean isVisibleInExplain(SqlExplainLevel explainLevel);
  }

  /** Metadata about the predicates that hold in the rows emitted from a
   * relational expression. */
  public interface Predicates extends Metadata {
    /**
     * Derives the predicates that hold on rows emitted from a relational
     * expression.
     *
     * @return Predicate list
     */
    RelOptPredicateList getPredicates();
  }

  /** The built-in forms of metadata. */
  interface All extends Selectivity, UniqueKeys, RowCount, DistinctRowCount,
      PercentageOriginalRows, ColumnUniqueness, ColumnOrigin, Predicates {
  }
}

// End BuiltInMetadata.java
