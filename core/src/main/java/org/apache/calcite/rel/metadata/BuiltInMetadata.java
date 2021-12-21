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
import org.apache.calcite.plan.RelOptPlanner;
import org.apache.calcite.plan.RelOptPredicateList;
import org.apache.calcite.plan.volcano.VolcanoPlanner;
import org.apache.calcite.rel.RelCollation;
import org.apache.calcite.rel.RelDistribution;
import org.apache.calcite.rel.RelNode;
import org.apache.calcite.rex.RexNode;
import org.apache.calcite.rex.RexTableInputRef;
import org.apache.calcite.rex.RexTableInputRef.RelTableRef;
import org.apache.calcite.sql.SqlExplainLevel;
import org.apache.calcite.util.BuiltInMethod;
import org.apache.calcite.util.ImmutableBitSet;

import com.google.common.collect.ImmutableList;
import com.google.common.collect.Multimap;

import org.checkerframework.checker.nullness.qual.Nullable;

import java.util.List;
import java.util.Set;

/**
 * Contains the interfaces for several common forms of metadata.
 */
public abstract class BuiltInMetadata {

  /** Metadata about the selectivity of a predicate. */
  @Deprecated // to be removed before 2.0
  public interface Selectivity extends Metadata {
    MetadataDef<Selectivity> DEF = MetadataDef.of(Selectivity.class,
        Selectivity.Handler.class, BuiltInMethod.SELECTIVITY.method);

    /**
     * Estimates the percentage of an expression's output rows which satisfy a
     * given predicate. Returns null to indicate that no reliable estimate can
     * be produced.
     *
     * @param predicate predicate whose selectivity is to be estimated against
     *                  rel's output
     * @return estimated selectivity (between 0.0 and 1.0), or null if no
     * reliable estimate can be determined
     *
     * @deprecated use {@link SelectivityHandler#getSelectivity(RelNode, RelMetadataQuery, RexNode)}
     */
    @Deprecated // to be removed before 2.0
    @Nullable Double getSelectivity(@Nullable RexNode predicate);

    /**
     * Handler API.
     *
     * @deprecated use {@link SelectivityHandler}
     */
    @FunctionalInterface
    @Deprecated // to be removed before 2.0
    interface Handler extends SelectivityHandler {
      @Deprecated // to be removed before 2.0
      @Override default MetadataDef<Selectivity> getDef() {
        return DEF;
      }
    }
  }

  /** MetadataHandler about the selectivity of a predicate. */
  @FunctionalInterface
  public interface SelectivityHandler extends MetadataHandler {
    /**
     * Estimates the percentage of an expression's output rows which satisfy a
     * given predicate. Returns null to indicate that no reliable estimate can
     * be produced.
     *
     * @param r to find selectivity for
     * @param mq context
     * @param predicate predicate whose selectivity is to be estimated against
     *                  rel's output
     * @return estimated selectivity (between 0.0 and 1.0), or null if no
     * reliable estimate can be determined
     */
    @Nullable Double getSelectivity(RelNode r, RelMetadataQuery mq, @Nullable RexNode predicate);
  }


  /**
   * Metadata about which combinations of columns are unique identifiers.
   *
   * @deprecated use {@link UniqueKeysHandler}
   */
  @Deprecated // to be removed before 2.0
  public interface UniqueKeys extends Metadata {
    MetadataDef<UniqueKeys> DEF = MetadataDef.of(UniqueKeys.class,
        UniqueKeys.Handler.class, BuiltInMethod.UNIQUE_KEYS.method);

    /**
     * Determines the set of unique minimal keys for this expression. A key is
     * represented as an {@link org.apache.calcite.util.ImmutableBitSet}, where
     * each bit position represents a 0-based output column ordinal.
     *
     * <p>Nulls can be ignored if the relational expression has filtered out
     * null values.
     *
     * @param ignoreNulls if true, ignore null values when determining
     *                    whether the keys are unique
     * @return set of keys, or null if this information cannot be determined
     * (whereas empty set indicates definitely no keys at all)
     */
    @Nullable Set<ImmutableBitSet> getUniqueKeys(boolean ignoreNulls);

    /** Handler API. */
    @FunctionalInterface
    interface Handler extends UniqueKeysHandler {
      @Deprecated // to be removed before 2.0
      @Override default MetadataDef<UniqueKeys> getDef() {
        return DEF;
      }
    }
  }

  /** MetadataHandler about which combinations of columns are unique identifiers. */
  @FunctionalInterface
  public interface UniqueKeysHandler extends MetadataHandler {
    /**
     * Determines the set of unique minimal keys for this expression. A key is
     * represented as an {@link org.apache.calcite.util.ImmutableBitSet}, where
     * each bit position represents a 0-based output column ordinal.
     *
     * <p>Nulls can be ignored if the relational expression has filtered out
     * null values.
     *
     * @param r to find metadata about
     * @param mq context
     * @param ignoreNulls if true, ignore null values when determining
     *                    whether the keys are unique
     * @return set of keys, or null if this information cannot be determined
     * (whereas empty set indicates definitely no keys at all)
     */
    @Nullable Set<ImmutableBitSet> getUniqueKeys(RelNode r, RelMetadataQuery mq,
        boolean ignoreNulls);
  }

  /**
   * Metadata about whether a set of columns uniquely identifies a row.
   *
   * @deprecated use {@link ColumnUniquenessHandler}
   */
  @Deprecated // to be removed before 2.0
  public interface ColumnUniqueness extends Metadata {
    MetadataDef<ColumnUniqueness> DEF = MetadataDef.of(ColumnUniqueness.class,
        ColumnUniqueness.Handler.class, BuiltInMethod.COLUMN_UNIQUENESS.method);

    /**
     * Determines whether a specified set of columns from a specified relational
     * expression are unique.
     *
     * <p>For example, if the relational expression is a {@code TableScan} to
     * T(A, B, C, D) whose key is (A, B), then:
     * <ul>
     * <li>{@code areColumnsUnique([0, 1])} yields true,
     * <li>{@code areColumnsUnique([0])} yields false,
     * <li>{@code areColumnsUnique([0, 2])} yields false.
     * </ul>
     *
     * <p>Nulls can be ignored if the relational expression has filtered out
     * null values.
     *
     * @param columns column mask representing the subset of columns for which
     *                uniqueness will be determined
     * @param ignoreNulls if true, ignore null values when determining column
     *                    uniqueness
     * @return whether the columns are unique, or
     * null if not enough information is available to make that determination
     */
    Boolean areColumnsUnique(ImmutableBitSet columns, boolean ignoreNulls);

    /** Handler API. */
    @FunctionalInterface
    interface Handler extends ColumnUniquenessHandler {
      @Deprecated // to be removed before 2.0
      @Override default MetadataDef<ColumnUniqueness> getDef() {
        return DEF;
      }
    }
  }

  /** MetadataHandler about whether a set of columns uniquely identifies a row. */
  @FunctionalInterface
  public interface ColumnUniquenessHandler extends MetadataHandler {
    /**
     * Determines whether a specified set of columns from a specified relational
     * expression are unique.
     *
     * <p>For example, if the relational expression is a {@code TableScan} to
     * T(A, B, C, D) whose key is (A, B), then:
     * <ul>
     * <li>{@code areColumnsUnique([0, 1])} yields true,
     * <li>{@code areColumnsUnique([0])} yields false,
     * <li>{@code areColumnsUnique([0, 2])} yields false.
     * </ul>
     *
     * <p>Nulls can be ignored if the relational expression has filtered out
     * null values.
     *
     * @param r to find metadata about
     * @param mq context
     * @param columns column mask representing the subset of columns for which
     *                uniqueness will be determined
     * @param ignoreNulls if true, ignore null values when determining column
     *                    uniqueness
     * @return whether the columns are unique, or
     * null if not enough information is available to make that determination
     */
    Boolean areColumnsUnique(RelNode r, RelMetadataQuery mq,
        ImmutableBitSet columns, boolean ignoreNulls);
  }

  /**
   * Metadata about which columns are sorted.
   *
   * @deprecated use {@link CollationHandler}
   */
  @Deprecated // to be removed before 2.0
  public interface Collation extends Metadata {
    MetadataDef<Collation> DEF = MetadataDef.of(Collation.class,
        Collation.Handler.class, BuiltInMethod.COLLATIONS.method);

    /** Determines which columns are sorted. */
    ImmutableList<RelCollation> collations();

    /** Handler API. */
    @FunctionalInterface
    interface Handler extends CollationHandler {
      ImmutableList<RelCollation> collations(RelNode r, RelMetadataQuery mq);
      @Deprecated // to be removed before 2.0
      @Override default MetadataDef<Collation> getDef() {
        return DEF;
      }
    }
  }

  /** MetadataHandler about which columns are sorted. */
  @FunctionalInterface
  public interface CollationHandler extends MetadataHandler {
    /** Determines which columns are sorted. */
    ImmutableList<RelCollation> collations(RelNode r, RelMetadataQuery mq);
  }

  /** Metadata about how a relational expression is distributed.
   *
   * <p>If you are an operator consuming a relational expression, which subset
   * of the rows are you seeing? You might be seeing all of them (BROADCAST
   * or SINGLETON), only those whose key column values have a particular hash
   * code (HASH) or only those whose column values have particular values or
   * ranges of values (RANGE).
   *
   * <p>When a relational expression is partitioned, it is often partitioned
   * among nodes, but it may be partitioned among threads running on the same
   * node. */
  @Deprecated // to be removed before 2.0
  public interface Distribution extends Metadata {
    MetadataDef<Distribution> DEF = MetadataDef.of(Distribution.class,
        Distribution.Handler.class, BuiltInMethod.DISTRIBUTION.method);

    /** Determines how the rows are distributed. */
    RelDistribution distribution();

    /** Handler API. */
    @FunctionalInterface
    interface Handler extends DistributionHandler {
      @Deprecated // to be removed before 2.0
      @Override default MetadataDef<Distribution> getDef() {
        return DEF;
      }
    }
  }

  /** MetadataHandler about how a relational expression is distributed.
   *
   * <p>If you are an operator consuming a relational expression, which subset
   * of the rows are you seeing? You might be seeing all of them (BROADCAST
   * or SINGLETON), only those whose key column values have a particular hash
   * code (HASH) or only those whose column values have particular values or
   * ranges of values (RANGE).
   *
   * <p>When a relational expression is partitioned, it is often partitioned
   * among nodes, but it may be partitioned among threads running on the same
   * node. */
  @FunctionalInterface
  public interface DistributionHandler extends MetadataHandler {
    /** Determines how the rows are distributed. */
    RelDistribution distribution(RelNode r, RelMetadataQuery mq);
  }

  /**
   * Metadata about the node types in a relational expression.
   *
   * <p>For each relational expression, it returns a multimap from the class
   * to the nodes instantiating that class. Each node will appear in the
   * multimap only once.
   */
  @Deprecated // to be removed before 2.0
  public interface NodeTypes extends Metadata {
    MetadataDef<NodeTypes> DEF = MetadataDef.of(NodeTypes.class,
        NodeTypes.Handler.class, BuiltInMethod.NODE_TYPES.method);

    /**
     * Returns a multimap from the class to the nodes instantiating that
     * class. The default implementation for a node classifies it as a
     * {@link RelNode}.
     */
    @Nullable Multimap<Class<? extends RelNode>, RelNode> getNodeTypes();

    /** Handler API. */
    @FunctionalInterface
    interface Handler extends NodeTypesHandler {
      @Deprecated // to be removed before 2.0
      @Override default MetadataDef<NodeTypes> getDef() {
        return DEF;
      }
    }
  }

  /**
   * MetadataHandler about the node types in a relational expression.
   *
   * <p>For each relational expression, it returns a multimap from the class
   * to the nodes instantiating that class. Each node will appear in the
   * multimap only once.
   */
  @FunctionalInterface
  public interface NodeTypesHandler extends MetadataHandler {
    /**
     * Returns a multimap from the class to the nodes instantiating that
     * class. The default implementation for a node classifies it as a
     * {@link RelNode}.
     */
    @Nullable Multimap<Class<? extends RelNode>, RelNode> getNodeTypes(RelNode r,
        RelMetadataQuery mq);
  }

  /** Metadata about the number of rows returned by a relational expression. */
  @Deprecated // to be removed before 2.0
  public interface RowCount extends Metadata {
    MetadataDef<RowCount> DEF = MetadataDef.of(RowCount.class,
        RowCount.Handler.class, BuiltInMethod.ROW_COUNT.method);

    /**
     * Estimates the number of rows which will be returned by a relational
     * expression. The default implementation for this query asks the rel itself
     * via {@link RelNode#estimateRowCount}, but metadata providers can override this
     * with their own cost models.
     *
     * @return estimated row count, or null if no reliable estimate can be
     * determined
     */
    @Nullable Double getRowCount();

    /** Handler API. */
    @FunctionalInterface
    interface Handler extends RowCountHandler {
      @Deprecated // to be removed before 2.0
      @Override default MetadataDef<RowCount> getDef() {
        return DEF;
      }
    }
  }

  /** MetadataHandler about the number of rows returned by a relational expression. */
  @FunctionalInterface
  public interface RowCountHandler extends MetadataHandler {

    /**
     * Estimates the number of rows which will be returned by a relational
     * expression. The default implementation for this query asks the rel itself
     * via {@link RelNode#estimateRowCount}, but metadata providers can override this
     * with their own cost models.
     *
     * @return estimated row count, or null if no reliable estimate can be
     * determined
     */
    @Nullable Double getRowCount(RelNode r, RelMetadataQuery mq);

  }

  /** Metadata about the maximum number of rows returned by a relational
   * expression. */
  @Deprecated // to be removed before 2.0
  public interface MaxRowCount extends Metadata {
    MetadataDef<MaxRowCount> DEF = MetadataDef.of(MaxRowCount.class,
        MaxRowCount.Handler.class, BuiltInMethod.MAX_ROW_COUNT.method);

    /**
     * Estimates the max number of rows which will be returned by a relational
     * expression.
     *
     * <p>The default implementation for this query returns
     * {@link Double#POSITIVE_INFINITY},
     * but metadata providers can override this with their own cost models.
     *
     * @return upper bound on the number of rows returned
     */
    @Nullable Double getMaxRowCount();

    /** Handler API. */
    @FunctionalInterface
    interface Handler extends MaxRowCountHandler {
      @Deprecated // to be removed before 2.0
      @Override default MetadataDef<MaxRowCount> getDef() {
        return DEF;
      }
    }
  }

  /** MetadataHandler about the maximum number of rows returned by a
   * relational expression. */
  @FunctionalInterface
  public interface MaxRowCountHandler extends MetadataHandler {
    /**
     * Estimates the max number of rows which will be returned by a relational
     * expression.
     *
     * <p>The default implementation for this query returns
     * {@link Double#POSITIVE_INFINITY},
     * but metadata providers can override this with their own cost models.
     *
     * @return upper bound on the number of rows returned
     */
    @Nullable Double getMaxRowCount(RelNode r, RelMetadataQuery mq);
  }

  /** Metadata about the minimum number of rows returned by a relational
   * expression. */
  @Deprecated // to be removed before 2.0
  public interface MinRowCount extends Metadata {
    MetadataDef<MinRowCount> DEF = MetadataDef.of(MinRowCount.class,
        MinRowCount.Handler.class, BuiltInMethod.MIN_ROW_COUNT.method);

    /**
     * Estimates the minimum number of rows which will be returned by a
     * relational expression.
     *
     * <p>The default implementation for this query returns 0,
     * but metadata providers can override this with their own cost models.
     *
     * @return lower bound on the number of rows returned
     */
    @Nullable Double getMinRowCount();

    /** Handler API. */
    @FunctionalInterface
    interface Handler extends MinRowCountHandler {
      @Deprecated // to be removed before 2.0
      @Override default MetadataDef<MinRowCount> getDef() {
        return DEF;
      }
    }
  }

  /** MetadataHandler about the minimum number of rows returned by a relational
   * expression. */
  @FunctionalInterface
  public interface MinRowCountHandler extends MetadataHandler {
    /**
     * Estimates the minimum number of rows which will be returned by a
     * relational expression.
     *
     * <p>The default implementation for this query returns 0,
     * but metadata providers can override this with their own cost models.
     *
     * @return lower bound on the number of rows returned
     */
    @Nullable Double getMinRowCount(RelNode r, RelMetadataQuery mq);
  }

  /** Metadata about the number of distinct rows returned by a set of columns
   * in a relational expression. */
  @Deprecated // to be removed before 2.0
  public interface DistinctRowCount extends Metadata {
    MetadataDef<DistinctRowCount> DEF = MetadataDef.of(DistinctRowCount.class,
        DistinctRowCount.Handler.class, BuiltInMethod.DISTINCT_ROW_COUNT.method);

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
    @Nullable Double getDistinctRowCount(ImmutableBitSet groupKey, @Nullable RexNode predicate);

    /** Handler API. */
    @FunctionalInterface
    interface Handler extends DistinctRowCountHandler {
      @Deprecated // to be removed before 2.0
      @Override default MetadataDef<DistinctRowCount> getDef() {
        return DEF;
      }
    }
  }

  /** MetadataHandler about the number of distinct rows returned by a set of
   * columns in a relational expression. */
  @FunctionalInterface
  public interface DistinctRowCountHandler extends MetadataHandler {
    /**
     * Estimates the number of rows which would be produced by a GROUP BY on the
     * set of columns indicated by groupKey, where the input to the GROUP BY has
     * been pre-filtered by predicate. This quantity (leaving out predicate) is
     * often referred to as cardinality (as in gender being a "low-cardinality
     * column").
     *
     * @param r to find metadata about
     * @param mq context
     * @param groupKey  column mask representing group by columns
     * @param predicate pre-filtered predicates
     * @return distinct row count for groupKey, filtered by predicate, or null
     * if no reliable estimate can be determined
     */
    @Nullable Double getDistinctRowCount(RelNode r, RelMetadataQuery mq,
        ImmutableBitSet groupKey, @Nullable RexNode predicate);
  }

  /** Metadata about the proportion of original rows that remain in a relational
   * expression. */
  @Deprecated // to be removed before 2.0
  public interface PercentageOriginalRows extends Metadata {
    MetadataDef<PercentageOriginalRows> DEF =
        MetadataDef.of(PercentageOriginalRows.class,
            PercentageOriginalRows.Handler.class,
            BuiltInMethod.PERCENTAGE_ORIGINAL_ROWS.method);

    /**
     * Estimates the percentage of the number of rows actually produced by a
     * relational expression out of the number of rows it would produce if all
     * single-table filter conditions were removed.
     *
     * @return estimated percentage (between 0.0 and 1.0), or null if no
     * reliable estimate can be determined
     */
    @Nullable Double getPercentageOriginalRows();

    /** Handler API. */
    @FunctionalInterface
    interface Handler extends PercentageOriginalRowsHandler {
      @Deprecated // to be removed before 2.0
      @Override default MetadataDef<PercentageOriginalRows> getDef() {
        return DEF;
      }
    }
  }

  /** MetadataHandler about the proportion of original rows that remain in a
   * relational expression. */
  @FunctionalInterface
  public interface PercentageOriginalRowsHandler extends MetadataHandler {

    /**
     * Estimates the percentage of the number of rows actually produced by a
     * relational expression out of the number of rows it would produce if all
     * single-table filter conditions were removed.
     *
     * @return estimated percentage (between 0.0 and 1.0), or null if no
     * reliable estimate can be determined
     */
    @Nullable Double getPercentageOriginalRows(RelNode r, RelMetadataQuery mq);
  }

  /** Metadata about the number of distinct values in the original source of a
   * column or set of columns. */
  @Deprecated // to be removed before 2.0
  public interface PopulationSize extends Metadata {
    MetadataDef<PopulationSize> DEF = MetadataDef.of(PopulationSize.class,
        PopulationSize.Handler.class, BuiltInMethod.POPULATION_SIZE.method);

    /**
     * Estimates the distinct row count in the original source for the given
     * {@code groupKey}, ignoring any filtering being applied by the expression.
     * Typically, "original source" means base table, but for derived columns,
     * the estimate may come from a non-leaf rel such as a LogicalProject.
     *
     * @param groupKey column mask representing the subset of columns for which
     *                 the row count will be determined
     * @return distinct row count for the given groupKey, or null if no reliable
     * estimate can be determined
     */
    @Nullable Double getPopulationSize(ImmutableBitSet groupKey);

    /** Handler API. */
    @FunctionalInterface
    interface Handler extends PopulationSizeHandler {
      @Deprecated // to be removed before 2.0
      @Override default MetadataDef<PopulationSize> getDef() {
        return DEF;
      }
    }
  }

  /** Metadata about the number of distinct values in the original source of a
   * column or set of columns. */
  @FunctionalInterface
  public interface PopulationSizeHandler extends MetadataHandler {
    /**
     * Estimates the distinct row count in the original source for the given
     * {@code groupKey}, ignoring any filtering being applied by the expression.
     * Typically, "original source" means base table, but for derived columns,
     * the estimate may come from a non-leaf rel such as a LogicalProject.
     *
     * @param r to find metadata about
     * @param mq context
     * @param groupKey column mask representing the subset of columns for which
     *                 the row count will be determined
     * @return distinct row count for the given groupKey, or null if no reliable
     * estimate can be determined
     */
    @Nullable Double getPopulationSize(RelNode r, RelMetadataQuery mq,
        ImmutableBitSet groupKey);
  }

  /** Metadata about the size of rows and columns. */
  @Deprecated // to be removed before 2.0
  public interface Size extends Metadata {
    MetadataDef<Size> DEF = MetadataDef.of(Size.class, Size.Handler.class,
        BuiltInMethod.AVERAGE_ROW_SIZE.method,
        BuiltInMethod.AVERAGE_COLUMN_SIZES.method);

    /**
     * Determines the average size (in bytes) of a row from this relational
     * expression.
     *
     * @return average size of a row, in bytes, or null if not known
     */
    @Nullable Double averageRowSize();

    /**
     * Determines the average size (in bytes) of a value of a column in this
     * relational expression.
     *
     * <p>Null values are included (presumably they occupy close to 0 bytes).
     *
     * <p>It is left to the caller to decide whether the size is the compressed
     * size, the uncompressed size, or memory allocation when the value is
     * wrapped in an object in the Java heap. The uncompressed size is probably
     * a good compromise.
     *
     * @return an immutable list containing, for each column, the average size
     * of a column value, in bytes. Each value or the entire list may be null if
     * the metadata is not available
     */
    List<@Nullable Double> averageColumnSizes();

    /** Handler API. */
    @Deprecated // to be removed before 2.0
    interface Handler extends SizeHandler {
      @Deprecated // to be removed before 2.0
      @Override default MetadataDef<Size> getDef() {
        return DEF;
      }
    }
  }

  /** MetadataHandler about the size of rows and columns. */
  public interface SizeHandler extends MetadataHandler {

    /**
     * Determines the average size (in bytes) of a row from this relational
     * expression.
     *
     * @return average size of a row, in bytes, or null if not known
     */
    @Nullable Double averageRowSize(RelNode r, RelMetadataQuery mq);

    /**
     * Determines the average size (in bytes) of a value of a column in this
     * relational expression.
     *
     * <p>Null values are included (presumably they occupy close to 0 bytes).
     *
     * <p>It is left to the caller to decide whether the size is the compressed
     * size, the uncompressed size, or memory allocation when the value is
     * wrapped in an object in the Java heap. The uncompressed size is probably
     * a good compromise.
     *
     * @return an immutable list containing, for each column, the average size
     * of a column value, in bytes. Each value or the entire list may be null if
     * the metadata is not available
     */
    @Nullable List<@Nullable Double> averageColumnSizes(RelNode r, RelMetadataQuery mq);
  }

  /** Metadata about the origins of columns. */
  @Deprecated // to be removed before 2.0
  public interface ColumnOrigin extends Metadata {
    MetadataDef<ColumnOrigin> DEF = MetadataDef.of(ColumnOrigin.class,
        ColumnOrigin.Handler.class, BuiltInMethod.COLUMN_ORIGIN.method);

    /**
     * For a given output column of an expression, determines all columns of
     * underlying tables which contribute to result values. An output column may
     * have more than one origin due to expressions such as Union and
     * LogicalProject. The optimizer may use this information for catalog access
     * (e.g. index availability).
     *
     * @param outputColumn 0-based ordinal for output column of interest
     * @return set of origin columns, or null if this information cannot be
     * determined (whereas empty set indicates definitely no origin columns at
     * all)
     */
    @Nullable Set<RelColumnOrigin> getColumnOrigins(int outputColumn);

    /** Handler API. */
    @FunctionalInterface
    interface Handler extends ColumnOriginHandler {
      @Deprecated // to be removed before 2.0
      @Override default MetadataDef<ColumnOrigin> getDef() {
        return DEF;
      }
    }
  }

  /** MetadataHandler about the origins of columns. */
  public interface ColumnOriginHandler extends MetadataHandler {
    /**
     * For a given output column of an expression, determines all columns of
     * underlying tables which contribute to result values. An output column may
     * have more than one origin due to expressions such as Union and
     * LogicalProject. The optimizer may use this information for catalog access
     * (e.g. index availability).
     *
     * @param r to find metadata about
     * @param mq context
     * @param outputColumn 0-based ordinal for output column of interest
     * @return set of origin columns, or null if this information cannot be
     * determined (whereas empty set indicates definitely no origin columns at
     * all)
     */
    @Nullable Set<RelColumnOrigin> getColumnOrigins(RelNode r, RelMetadataQuery mq,
        int outputColumn);
  }

  /** Metadata about the origins of expressions. */
  @Deprecated // to be removed before 2.0
  public interface ExpressionLineage extends Metadata {
    MetadataDef<ExpressionLineage> DEF = MetadataDef.of(ExpressionLineage.class,
        ExpressionLineage.Handler.class, BuiltInMethod.EXPRESSION_LINEAGE.method);

    /**
     * Given the input expression applied on the given {@link RelNode}, this
     * provider returns the expression with its lineage resolved.
     *
     * <p>In particular, the result will be a set of nodes which might contain
     * references to columns in TableScan operators ({@link RexTableInputRef}).
     * An expression can have more than one lineage expression due to Union
     * operators. However, we do not check column equality in Filter predicates.
     * Each TableScan operator below the node is identified uniquely by its
     * qualified name and its entity number.
     *
     * <p>For example, if the expression is {@code $0 + 2} and {@code $0} originated
     * from column {@code $3} in the {@code 0} occurrence of table {@code A} in the
     * plan, result will be: {@code A.#0.$3 + 2}. Occurrences are generated in no
     * particular order, but it is guaranteed that if two expressions referred to the
     * same table, the qualified name + occurrence will be the same.
     *
     * @param expression expression whose lineage we want to resolve
     *
     * @return set of expressions with lineage resolved, or null if this information
     * cannot be determined (e.g. origin of an expression is an aggregation
     * in an {@link org.apache.calcite.rel.core.Aggregate} operator)
     */
    @Nullable Set<RexNode> getExpressionLineage(RexNode expression);

    /** Handler API. */
    @FunctionalInterface
    interface Handler extends ExpressionLineageHandler {
      @Deprecated // to be removed before 2.0
      @Override default MetadataDef<ExpressionLineage> getDef() {
        return DEF;
      }
    }
  }

  /** MetadataHandler about the origins of expressions. */
  @FunctionalInterface
  public interface ExpressionLineageHandler extends MetadataHandler {
    /**
     * Given the input expression applied on the given {@link RelNode}, this
     * provider returns the expression with its lineage resolved.
     *
     * <p>In particular, the result will be a set of nodes which might contain
     * references to columns in TableScan operators ({@link RexTableInputRef}).
     * An expression can have more than one lineage expression due to Union
     * operators. However, we do not check column equality in Filter predicates.
     * Each TableScan operator below the node is identified uniquely by its
     * qualified name and its entity number.
     *
     * <p>For example, if the expression is {@code $0 + 2} and {@code $0} originated
     * from column {@code $3} in the {@code 0} occurrence of table {@code A} in the
     * plan, result will be: {@code A.#0.$3 + 2}. Occurrences are generated in no
     * particular order, but it is guaranteed that if two expressions referred to the
     * same table, the qualified name + occurrence will be the same.
     *
     * @param r to find metadata about
     * @param mq context
     * @param expression expression whose lineage we want to resolve
     *
     * @return set of expressions with lineage resolved, or null if this information
     * cannot be determined (e.g. origin of an expression is an aggregation
     * in an {@link org.apache.calcite.rel.core.Aggregate} operator)
     */
    @Nullable Set<RexNode> getExpressionLineage(RelNode r, RelMetadataQuery mq,
        RexNode expression);
  }

  /** Metadata to obtain references to tables used by a given expression. */
  @Deprecated // to be removed before 2.0
  public interface TableReferences extends Metadata {
    MetadataDef<TableReferences> DEF = MetadataDef.of(TableReferences.class,
        TableReferences.Handler.class, BuiltInMethod.TABLE_REFERENCES.method);

    /**
     * This provider returns the tables used by a given plan.
     *
     * <p>In particular, the result will be a set of unique table references
     * ({@link RelTableRef}) corresponding to each TableScan operator in the
     * plan. These table references are composed by the table qualified name
     * and an entity number.
     *
     * <p>Importantly, the table identifiers returned by this metadata provider
     * will be consistent with the unique identifiers used by the {@link ExpressionLineage}
     * provider, meaning that it is guaranteed that same table will use same unique
     * identifiers in both.
     *
     * @return set of unique table identifiers, or null if this information
     * cannot be determined
     */
    Set<RelTableRef> getTableReferences();

    /** Handler API. */
    @FunctionalInterface
    interface Handler extends TableReferencesHandler {
      @Deprecated // to be removed before 2.0
      @Override default MetadataDef<TableReferences> getDef() {
        return DEF;
      }
    }
  }

  /** MetadataHandler to obtain references to tables used by a given expression. */
  public interface TableReferencesHandler extends MetadataHandler {

    /**
     * This provider returns the tables used by a given plan.
     *
     * <p>In particular, the result will be a set of unique table references
     * ({@link RelTableRef}) corresponding to each TableScan operator in the
     * plan. These table references are composed by the table qualified name
     * and an entity number.
     *
     * <p>Importantly, the table identifiers returned by this metadata provider
     * will be consistent with the unique identifiers used by the {@link ExpressionLineage}
     * provider, meaning that it is guaranteed that same table will use same unique
     * identifiers in both.
     *
     * @return set of unique table identifiers, or null if this information
     * cannot be determined
     */
    Set<RelTableRef> getTableReferences(RelNode r, RelMetadataQuery mq);
  }

  /** Metadata about the cost of evaluating a relational expression, including
   * all of its inputs. */
  @Deprecated // to be removed before 2.0
  public interface CumulativeCost extends Metadata {
    MetadataDef<CumulativeCost> DEF = MetadataDef.of(CumulativeCost.class,
        CumulativeCost.Handler.class, BuiltInMethod.CUMULATIVE_COST.method);

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

    /** Handler API. */
    @FunctionalInterface
    interface Handler extends CumulativeCostHandler {
      @Deprecated // to be removed before 2.0
      @Override default MetadataDef<CumulativeCost> getDef() {
        return DEF;
      }

    }
  }

  /** MetadataHandler about the cost of evaluating a relational expression,
   * including all of its inputs. */
  @FunctionalInterface
  public interface CumulativeCostHandler extends MetadataHandler {
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
    RelOptCost getCumulativeCost(RelNode r, RelMetadataQuery mq);
  }

  /** Metadata about the cost of evaluating a relational expression, not
   * including its inputs. */
  @Deprecated // to be removed before 2.0
  public interface NonCumulativeCost extends Metadata {
    MetadataDef<NonCumulativeCost> DEF = MetadataDef.of(NonCumulativeCost.class,
        NonCumulativeCost.Handler.class,
        BuiltInMethod.NON_CUMULATIVE_COST.method);

    /**
     * Estimates the cost of executing a relational expression, not counting the
     * cost of its inputs. (However, the non-cumulative cost is still usually
     * dependent on the row counts of the inputs.)
     *
     * <p>The default implementation for this query asks the rel itself via
     * {@link RelNode#computeSelfCost(RelOptPlanner, RelMetadataQuery)},
     * but metadata providers can override this with their own cost models.
     *
     * @return estimated cost, or null if no reliable estimate can be
     * determined
     */
    RelOptCost getNonCumulativeCost();

    /** Handler API. */
    @FunctionalInterface
    interface Handler extends NonCumulativeCostHandler {
      @Deprecated // to be removed before 2.0
      @Override default MetadataDef<NonCumulativeCost> getDef() {
        return DEF;
      }

    }
  }

  /** MetadataHandler about the cost of evaluating a relational
   * expression, not including its inputs. */
  public interface NonCumulativeCostHandler extends MetadataHandler {

    /**
     * Estimates the cost of executing a relational expression, not counting the
     * cost of its inputs. (However, the non-cumulative cost is still usually
     * dependent on the row counts of the inputs.)
     *
     * <p>The default implementation for this query asks the rel itself via
     * {@link RelNode#computeSelfCost(RelOptPlanner, RelMetadataQuery)},
     * but metadata providers can override this with their own cost models.
     *
     * @return estimated cost, or null if no reliable estimate can be
     * determined
     */
    RelOptCost getNonCumulativeCost(RelNode r, RelMetadataQuery mq);

  }


  /** Metadata about whether a relational expression should appear in a plan. */
  @Deprecated // to be removed before 2.0
  public interface ExplainVisibility extends Metadata {
    MetadataDef<ExplainVisibility> DEF = MetadataDef.of(ExplainVisibility.class,
        ExplainVisibility.Handler.class,
        BuiltInMethod.EXPLAIN_VISIBILITY.method);

    /**
     * Determines whether a relational expression should be visible in EXPLAIN
     * PLAN output at a particular level of detail.
     *
     * @param explainLevel level of detail
     * @return true for visible, false for invisible
     */
    Boolean isVisibleInExplain(SqlExplainLevel explainLevel);

    /** Handler API. */
    @FunctionalInterface
    interface Handler extends ExplainVisibilityHandler {
      Boolean isVisibleInExplain(RelNode r, RelMetadataQuery mq,
          SqlExplainLevel explainLevel);
      @Deprecated // to be removed before 2.0
      @Override default MetadataDef<ExplainVisibility> getDef() {
        return DEF;
      }

    }
  }

  /** Metadata about whether a relational expression should appear in a plan. */
  public interface ExplainVisibilityHandler extends MetadataHandler {
    /**
     * Determines whether a relational expression should be visible in EXPLAIN
     * PLAN output at a particular level of detail.
     *
     * @param explainLevel level of detail
     * @return true for visible, false for invisible
     */
    Boolean isVisibleInExplain(RelNode r, RelMetadataQuery mq,
        SqlExplainLevel explainLevel);
  }

  /** Metadata about the predicates that hold in the rows emitted from a
   * relational expression. */
  @Deprecated // to be removed before 2.0
  public interface Predicates extends Metadata {
    MetadataDef<Predicates> DEF = MetadataDef.of(Predicates.class,
        Predicates.Handler.class, BuiltInMethod.PREDICATES.method);

    /**
     * Derives the predicates that hold on rows emitted from a relational
     * expression.
     *
     * @return Predicate list
     */
    RelOptPredicateList getPredicates();

    /** Handler API. */
    @FunctionalInterface
    interface Handler extends PredicatesHandler {
      RelOptPredicateList getPredicates(RelNode r, RelMetadataQuery mq);
      @Deprecated // to be removed before 2.0
      @Override default MetadataDef<Predicates> getDef() {
        return DEF;
      }

    }
  }

  /** MetadataHandler about the predicates that hold in the rows emitted
   * from a relational expression. */
  @FunctionalInterface
  public interface PredicatesHandler extends MetadataHandler {
    /**
     * Derives the predicates that hold on rows emitted from a relational
     * expression.
     *
     * @return Predicate list
     */
    RelOptPredicateList getPredicates(RelNode r, RelMetadataQuery mq);
  }

  /** Metadata about the predicates that hold in the rows emitted from a
   * relational expression.
   *
   * <p>The difference with respect to {@link Predicates} provider is that
   * this provider tries to extract ALL predicates even if they are not
   * applied on the output expressions of the relational expression; we rely
   * on {@link RexTableInputRef} to reference origin columns in
   * {@link org.apache.calcite.rel.core.TableScan} for the result predicates.
   */
  @Deprecated // to be removed before 2.0
  public interface AllPredicates extends Metadata {
    MetadataDef<AllPredicates> DEF = MetadataDef.of(AllPredicates.class,
            AllPredicates.Handler.class, BuiltInMethod.ALL_PREDICATES.method);

    /**
     * Derives the predicates that hold on rows emitted from a relational
     * expression.
     *
     * @return predicate list, or null if the provider cannot infer the
     * lineage for any of the expressions contained in any of the predicates
     */
    @Nullable RelOptPredicateList getAllPredicates();

    /** Handler API. */
    @FunctionalInterface
    interface Handler extends AllPredicatesHandler {
      @Deprecated // to be removed before 2.0
      @Override default MetadataDef<AllPredicates> getDef() {
        return DEF;
      }

    }
  }

  /** Metadata about the predicates that hold in the rows emitted from a
   * relational expression.
   *
   * <p>The difference with respect to {@link Predicates} provider is that
   * this provider tries to extract ALL predicates even if they are not
   * applied on the output expressions of the relational expression; we rely
   * on {@link RexTableInputRef} to reference origin columns in
   * {@link org.apache.calcite.rel.core.TableScan} for the result predicates.
   */
  public interface AllPredicatesHandler extends MetadataHandler {
    /**
     * Derives the predicates that hold on rows emitted from a relational
     * expression.
     *
     * @return predicate list, or null if the provider cannot infer the
     * lineage for any of the expressions contained in any of the predicates
     */
    @Nullable RelOptPredicateList getAllPredicates(RelNode r, RelMetadataQuery mq);
  }

  /** Metadata about the degree of parallelism of a relational expression, and
   * how its operators are assigned to processes with independent resource
   * pools. */
  @Deprecated // to be removed before 2.0
  public interface Parallelism extends Metadata {
    MetadataDef<Parallelism> DEF = MetadataDef.of(Parallelism.class,
        Parallelism.Handler.class, BuiltInMethod.IS_PHASE_TRANSITION.method,
        BuiltInMethod.SPLIT_COUNT.method);

    /** Returns whether each physical operator implementing this relational
     * expression belongs to a different process than its inputs.
     *
     * <p>A collection of operators processing all of the splits of a particular
     * stage in the query pipeline is called a "phase". A phase starts with
     * a leaf node such as a {@link org.apache.calcite.rel.core.TableScan},
     * or with a phase-change node such as an
     * {@link org.apache.calcite.rel.core.Exchange}. Hadoop's shuffle operator
     * (a form of sort-exchange) causes data to be sent across the network. */
    Boolean isPhaseTransition();

    /** Returns the number of distinct splits of the data.
     *
     * <p>Note that splits must be distinct. For broadcast, where each copy is
     * the same, returns 1.
     *
     * <p>Thus the split count is the <em>proportion</em> of the data seen by
     * each operator instance.
     */
    Integer splitCount();

    /** Handler API. */
    interface Handler extends ParallelismHandler {
      @Deprecated // to be removed before 2.0
      @Override default MetadataDef<Parallelism> getDef() {
        return DEF;
      }
    }
  }

  /** MetadataHandler about the degree of parallelism of a relational expression, and
   * how its operators are assigned to processes with independent resource
   * pools. */
  public interface ParallelismHandler extends MetadataHandler {
    /** Returns whether each physical operator implementing this relational
     * expression belongs to a different process than its inputs.
     *
     * <p>A collection of operators processing all of the splits of a particular
     * stage in the query pipeline is called a "phase". A phase starts with
     * a leaf node such as a {@link org.apache.calcite.rel.core.TableScan},
     * or with a phase-change node such as an
     * {@link org.apache.calcite.rel.core.Exchange}. Hadoop's shuffle operator
     * (a form of sort-exchange) causes data to be sent across the network. */
    Boolean isPhaseTransition(RelNode r, RelMetadataQuery mq);

    /** Returns the number of distinct splits of the data.
     *
     * <p>Note that splits must be distinct. For broadcast, where each copy is
     * the same, returns 1.
     *
     * <p>Thus the split count is the <em>proportion</em> of the data seen by
     * each operator instance.
     */
    Integer splitCount(RelNode r, RelMetadataQuery mq);
  }

  /** Metadata to get the lower bound cost of a RelNode. */
  @Deprecated // to be removed before 2.0
  public interface LowerBoundCost extends Metadata {
    MetadataDef<LowerBoundCost> DEF = MetadataDef.of(LowerBoundCost.class,
        LowerBoundCost.Handler.class, BuiltInMethod.LOWER_BOUND_COST.method);

    /** Returns the lower bound cost of a RelNode. */
    RelOptCost getLowerBoundCost(VolcanoPlanner planner);

    /** Handler API. */
    @FunctionalInterface
    interface Handler extends LowerBoundCostHandler {
      RelOptCost getLowerBoundCost(
          RelNode r, RelMetadataQuery mq, VolcanoPlanner planner);
      @Deprecated // to be removed before 2.0
      @Override default MetadataDef<LowerBoundCost> getDef() {
        return DEF;
      }

    }
  }

  /** MetadataHandler to get the lower bound cost of a RelNode. */
  @FunctionalInterface
  public interface LowerBoundCostHandler extends MetadataHandler {
    /** Returns the lower bound cost of a RelNode. */
    RelOptCost getLowerBoundCost(
        RelNode r, RelMetadataQuery mq, VolcanoPlanner planner);
  }

  /** Metadata about the memory use of an operator. */
  @Deprecated // to be removed before 2.0
  public interface Memory extends Metadata {
    MetadataDef<Memory> DEF = MetadataDef.of(Memory.class,
        Memory.Handler.class, BuiltInMethod.MEMORY.method,
        BuiltInMethod.CUMULATIVE_MEMORY_WITHIN_PHASE.method,
        BuiltInMethod.CUMULATIVE_MEMORY_WITHIN_PHASE_SPLIT.method);

    /** Returns the expected amount of memory, in bytes, required by a physical
     * operator implementing this relational expression, across all splits.
     *
     * <p>How much memory is used depends very much on the algorithm; for
     * example, an implementation of
     * {@link org.apache.calcite.rel.core.Aggregate} that loads all data into a
     * hash table requires approximately {@code rowCount * averageRowSize}
     * bytes, whereas an implementation that assumes that the input is sorted
     * requires only {@code averageRowSize} bytes to maintain a single
     * accumulator for each aggregate function.
     */
    @Nullable Double memory();

    /** Returns the cumulative amount of memory, in bytes, required by the
     * physical operator implementing this relational expression, and all other
     * operators within the same phase, across all splits.
     *
     * @see Parallelism#splitCount()
     */
    @Nullable Double cumulativeMemoryWithinPhase();

    /** Returns the expected cumulative amount of memory, in bytes, required by
     * the physical operator implementing this relational expression, and all
     * operators within the same phase, within each split.
     *
     * <p>Basic formula:
     *
     * <blockquote>cumulativeMemoryWithinPhaseSplit
     *     = cumulativeMemoryWithinPhase / Parallelism.splitCount</blockquote>
     */
    @Nullable Double cumulativeMemoryWithinPhaseSplit();

    /** Handler API. */
    interface Handler extends MemoryHandler {
      @Deprecated // to be removed before 2.0
      @Override default MetadataDef<Memory> getDef() {
        return DEF;
      }

    }
  }

  /** MetadataHandler about the memory use of an operator. */
  public interface MemoryHandler extends MetadataHandler {

    /** Returns the expected amount of memory, in bytes, required by a physical
     * operator implementing this relational expression, across all splits.
     *
     * <p>How much memory is used depends very much on the algorithm; for
     * example, an implementation of
     * {@link org.apache.calcite.rel.core.Aggregate} that loads all data into a
     * hash table requires approximately {@code rowCount * averageRowSize}
     * bytes, whereas an implementation that assumes that the input is sorted
     * requires only {@code averageRowSize} bytes to maintain a single
     * accumulator for each aggregate function.
     */
    @Nullable Double memory(RelNode r, RelMetadataQuery mq);

    /** Returns the cumulative amount of memory, in bytes, required by the
     * physical operator implementing this relational expression, and all other
     * operators within the same phase, across all splits.
     *
     * @see Parallelism#splitCount()
     */
    @Nullable Double cumulativeMemoryWithinPhase(RelNode r, RelMetadataQuery mq);

    /** Returns the expected cumulative amount of memory, in bytes, required by
     * the physical operator implementing this relational expression, and all
     * operators within the same phase, within each split.
     *
     * <p>Basic formula:
     *
     * <blockquote>cumulativeMemoryWithinPhaseSplit
     *     = cumulativeMemoryWithinPhase / Parallelism.splitCount</blockquote>
     */
    @Nullable Double cumulativeMemoryWithinPhaseSplit(RelNode r, RelMetadataQuery mq);
  }

  /** The built-in forms of metadata. */
  @Deprecated // to be removed before 2.0
  interface All extends Selectivity, UniqueKeys, RowCount, DistinctRowCount,
      PercentageOriginalRows, ColumnUniqueness, ColumnOrigin, Predicates,
      Collation, Distribution, Size, Parallelism, Memory, AllPredicates,
      ExpressionLineage, TableReferences, NodeTypes {
  }
}
