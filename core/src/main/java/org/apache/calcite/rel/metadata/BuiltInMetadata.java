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
import org.apache.calcite.rel.type.RelDataTypeFactory;
import org.apache.calcite.rex.RexBuilder;
import org.apache.calcite.rex.RexNode;
import org.apache.calcite.rex.RexTableInputRef;
import org.apache.calcite.rex.RexTableInputRef.RelTableRef;
import org.apache.calcite.sql.SqlExplainLevel;
import org.apache.calcite.tools.RelBuilder;
import org.apache.calcite.util.ArrowSet;
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
  public interface Selectivity extends Metadata {
    MetadataDef<Selectivity> DEF =
        MetadataDef.of(Selectivity.class, Selectivity.Handler.class,
            BuiltInMethod.SELECTIVITY.method);

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
    @Nullable Double getSelectivity(@Nullable RexNode predicate);

    /** Handler API. */
    @FunctionalInterface
    interface Handler extends MetadataHandler<Selectivity> {
      @Nullable Double getSelectivity(RelNode r, RelMetadataQuery mq, @Nullable RexNode predicate);

      @Override default MetadataDef<Selectivity> getDef() {
        return DEF;
      }
    }
  }

  /** Metadata about which combinations of columns are unique identifiers. */
  public interface UniqueKeys extends Metadata {
    MetadataDef<UniqueKeys> DEF =
        MetadataDef.of(UniqueKeys.class, UniqueKeys.Handler.class,
            BuiltInMethod.UNIQUE_KEYS.method);

    /**
     * Determines the set of unique minimal keys for this expression. A key is
     * represented as an {@link org.apache.calcite.util.ImmutableBitSet}, where
     * each bit position represents a 0-based output column ordinal.
     *
     * <p>Note that a unique key plus other columns is still unique.
     * Therefore, all columns are unique in a table with a unique key
     * consisting of the empty set, as is the case for zero-row and
     * single-row tables. The converse is not true: a table with all
     * columns unique does necessary have the empty set as a key -
     * that is never true with multi-row tables.
     *
     * <p>Nulls can be ignored if the relational expression has filtered out
     * null values.
     *
     * @param ignoreNulls if true, ignore null values when determining
     *                    whether the keys are unique
     * @return set of keys, or null if this information cannot be determined
     * (whereas empty set indicates definitely no keys at all, and a set
     * containing the empty set implies every column is unique)
     */
    @Nullable Set<ImmutableBitSet> getUniqueKeys(boolean ignoreNulls);

    /** Handler API. */
    @FunctionalInterface
    interface Handler extends MetadataHandler<UniqueKeys> {
      @Nullable Set<ImmutableBitSet> getUniqueKeys(RelNode r, RelMetadataQuery mq,
          boolean ignoreNulls);

      @Override default MetadataDef<UniqueKeys> getDef() {
        return DEF;
      }
    }
  }

  /** Metadata about whether a set of columns uniquely identifies a row. */
  public interface ColumnUniqueness extends Metadata {
    MetadataDef<ColumnUniqueness> DEF =
        MetadataDef.of(ColumnUniqueness.class, ColumnUniqueness.Handler.class,
            BuiltInMethod.COLUMN_UNIQUENESS.method);

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
    interface Handler extends MetadataHandler<ColumnUniqueness> {
      Boolean areColumnsUnique(RelNode r, RelMetadataQuery mq,
          ImmutableBitSet columns, boolean ignoreNulls);

      @Override default MetadataDef<ColumnUniqueness> getDef() {
        return DEF;
      }
    }
  }

  /** Metadata about which columns are sorted. */
  public interface Collation extends Metadata {
    MetadataDef<Collation> DEF =
        MetadataDef.of(Collation.class, Collation.Handler.class,
            BuiltInMethod.COLLATIONS.method);

    /** Determines which columns are sorted. */
    ImmutableList<RelCollation> collations();

    /** Handler API. */
    @FunctionalInterface
    interface Handler extends MetadataHandler<Collation> {
      ImmutableList<RelCollation> collations(RelNode r, RelMetadataQuery mq);

      @Override default MetadataDef<Collation> getDef() {
        return DEF;
      }
    }
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
  public interface Distribution extends Metadata {
    MetadataDef<Distribution> DEF =
        MetadataDef.of(Distribution.class, Distribution.Handler.class,
            BuiltInMethod.DISTRIBUTION.method);

    /** Determines how the rows are distributed. */
    RelDistribution distribution();

    /** Handler API. */
    @FunctionalInterface
    interface Handler extends MetadataHandler<Distribution> {
      RelDistribution distribution(RelNode r, RelMetadataQuery mq);

      @Override default MetadataDef<Distribution> getDef() {
        return DEF;
      }
    }
  }

  /**
   * Metadata about the node types in a relational expression.
   *
   * <p>For each relational expression, it returns a multimap from the class
   * to the nodes instantiating that class. Each node will appear in the
   * multimap only once.
   */
  public interface NodeTypes extends Metadata {
    MetadataDef<NodeTypes> DEF =
        MetadataDef.of(NodeTypes.class, NodeTypes.Handler.class,
            BuiltInMethod.NODE_TYPES.method);

    /**
     * Returns a multimap from the class to the nodes instantiating that
     * class. The default implementation for a node classifies it as a
     * {@link RelNode}.
     */
    @Nullable Multimap<Class<? extends RelNode>, RelNode> getNodeTypes();

    /** Handler API. */
    @FunctionalInterface
    interface Handler extends MetadataHandler<NodeTypes> {
      @Nullable Multimap<Class<? extends RelNode>, RelNode> getNodeTypes(RelNode r,
          RelMetadataQuery mq);

      @Override default MetadataDef<NodeTypes> getDef() {
        return DEF;
      }
    }
  }

  /** Metadata about the number of rows returned by a relational expression. */
  public interface RowCount extends Metadata {
    MetadataDef<RowCount> DEF =
        MetadataDef.of(RowCount.class, RowCount.Handler.class,
            BuiltInMethod.ROW_COUNT.method);

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
    interface Handler extends MetadataHandler<RowCount> {
      @Nullable Double getRowCount(RelNode r, RelMetadataQuery mq);

      @Override default MetadataDef<RowCount> getDef() {
        return DEF;
      }
    }
  }

  /** Metadata about the maximum number of rows returned by a relational
   * expression. */
  public interface MaxRowCount extends Metadata {
    MetadataDef<MaxRowCount> DEF =
        MetadataDef.of(MaxRowCount.class, MaxRowCount.Handler.class,
            BuiltInMethod.MAX_ROW_COUNT.method);

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
    interface Handler extends MetadataHandler<MaxRowCount> {
      @Nullable Double getMaxRowCount(RelNode r, RelMetadataQuery mq);

      @Override default MetadataDef<MaxRowCount> getDef() {
        return DEF;
      }
    }
  }

  /** Metadata about the minimum number of rows returned by a relational
   * expression. */
  public interface MinRowCount extends Metadata {
    MetadataDef<MinRowCount> DEF =
        MetadataDef.of(MinRowCount.class, MinRowCount.Handler.class,
            BuiltInMethod.MIN_ROW_COUNT.method);

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
    interface Handler extends MetadataHandler<MinRowCount> {
      @Nullable Double getMinRowCount(RelNode r, RelMetadataQuery mq);

      @Override default MetadataDef<MinRowCount> getDef() {
        return DEF;
      }
    }
  }

  /** Metadata about the number of distinct rows returned by a set of columns
   * in a relational expression. */
  public interface DistinctRowCount extends Metadata {
    MetadataDef<DistinctRowCount> DEF =
        MetadataDef.of(DistinctRowCount.class, DistinctRowCount.Handler.class,
            BuiltInMethod.DISTINCT_ROW_COUNT.method);

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
    interface Handler extends MetadataHandler<DistinctRowCount> {
      @Nullable Double getDistinctRowCount(RelNode r, RelMetadataQuery mq,
          ImmutableBitSet groupKey, @Nullable RexNode predicate);

      @Override default MetadataDef<DistinctRowCount> getDef() {
        return DEF;
      }
    }
  }

  /** Metadata about the proportion of original rows that remain in a relational
   * expression. */
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
    interface Handler extends MetadataHandler<PercentageOriginalRows> {
      @Nullable Double getPercentageOriginalRows(RelNode r, RelMetadataQuery mq);

      @Override default MetadataDef<PercentageOriginalRows> getDef() {
        return DEF;
      }
    }
  }

  /** Metadata about the number of distinct values in the original source of a
   * column or set of columns. */
  public interface PopulationSize extends Metadata {
    MetadataDef<PopulationSize> DEF =
        MetadataDef.of(PopulationSize.class, PopulationSize.Handler.class,
            BuiltInMethod.POPULATION_SIZE.method);

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
    interface Handler extends MetadataHandler<PopulationSize> {
      @Nullable Double getPopulationSize(RelNode r, RelMetadataQuery mq,
          ImmutableBitSet groupKey);

      @Override default MetadataDef<PopulationSize> getDef() {
        return DEF;
      }
    }
  }

  /** Metadata about the size of rows and columns. */
  public interface Size extends Metadata {
    MetadataDef<Size> DEF =
        MetadataDef.of(Size.class, Size.Handler.class,
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
    interface Handler extends MetadataHandler<Size> {
      @Nullable Double averageRowSize(RelNode r, RelMetadataQuery mq);
      @Nullable List<@Nullable Double> averageColumnSizes(RelNode r, RelMetadataQuery mq);

      @Override default MetadataDef<Size> getDef() {
        return DEF;
      }
    }
  }

  /** Metadata about the origins of columns. */
  public interface ColumnOrigin extends Metadata {
    MetadataDef<ColumnOrigin> DEF =
        MetadataDef.of(ColumnOrigin.class, ColumnOrigin.Handler.class,
            BuiltInMethod.COLUMN_ORIGIN.method);

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
    interface Handler extends MetadataHandler<ColumnOrigin> {
      @Nullable Set<RelColumnOrigin> getColumnOrigins(RelNode r, RelMetadataQuery mq,
          int outputColumn);

      @Override default MetadataDef<ColumnOrigin> getDef() {
        return DEF;
      }
    }
  }

  /** Metadata about the origins of expressions. */
  public interface ExpressionLineage extends Metadata {
    MetadataDef<ExpressionLineage> DEF =
        MetadataDef.of(ExpressionLineage.class, ExpressionLineage.Handler.class,
            BuiltInMethod.EXPRESSION_LINEAGE.method);

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
    interface Handler extends MetadataHandler<ExpressionLineage> {
      @Nullable Set<RexNode> getExpressionLineage(RelNode r, RelMetadataQuery mq,
          RexNode expression);

      @Override default MetadataDef<ExpressionLineage> getDef() {
        return DEF;
      }
    }
  }

  /** Metadata to obtain references to tables used by a given expression. */
  public interface TableReferences extends Metadata {
    MetadataDef<TableReferences> DEF =
        MetadataDef.of(TableReferences.class, TableReferences.Handler.class,
            BuiltInMethod.TABLE_REFERENCES.method);

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
    interface Handler extends MetadataHandler<TableReferences> {
      Set<RelTableRef> getTableReferences(RelNode r, RelMetadataQuery mq);

      @Override default MetadataDef<TableReferences> getDef() {
        return DEF;
      }
    }
  }

  /** Metadata about the cost of evaluating a relational expression, including
   * all of its inputs. */
  public interface CumulativeCost extends Metadata {
    MetadataDef<CumulativeCost> DEF =
        MetadataDef.of(CumulativeCost.class, CumulativeCost.Handler.class,
            BuiltInMethod.CUMULATIVE_COST.method);

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
    interface Handler extends MetadataHandler<CumulativeCost> {
      RelOptCost getCumulativeCost(RelNode r, RelMetadataQuery mq);

      @Override default MetadataDef<CumulativeCost> getDef() {
        return DEF;
      }

    }
  }

  /** Metadata about the cost of evaluating a relational expression, not
   * including its inputs. */
  public interface NonCumulativeCost extends Metadata {
    MetadataDef<NonCumulativeCost> DEF =
        MetadataDef.of(NonCumulativeCost.class, NonCumulativeCost.Handler.class,
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
    interface Handler extends MetadataHandler<NonCumulativeCost> {
      RelOptCost getNonCumulativeCost(RelNode r, RelMetadataQuery mq);

      @Override default MetadataDef<NonCumulativeCost> getDef() {
        return DEF;
      }

    }
  }

  /** Metadata about whether a relational expression should appear in a plan. */
  public interface ExplainVisibility extends Metadata {
    MetadataDef<ExplainVisibility> DEF =
        MetadataDef.of(ExplainVisibility.class, ExplainVisibility.Handler.class,
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
    interface Handler extends MetadataHandler<ExplainVisibility> {
      Boolean isVisibleInExplain(RelNode r, RelMetadataQuery mq,
          SqlExplainLevel explainLevel);

      @Override default MetadataDef<ExplainVisibility> getDef() {
        return DEF;
      }

    }
  }

  /** Metadata about the predicates that hold in the rows emitted from a
   * relational expression. */
  public interface Predicates extends Metadata {
    MetadataDef<Predicates> DEF =
        MetadataDef.of(Predicates.class, Predicates.Handler.class,
            BuiltInMethod.PREDICATES.method);

    /**
     * Derives the predicates that hold on rows emitted from a relational
     * expression.
     *
     * @return Predicate list
     */
    RelOptPredicateList getPredicates();

    /** Handler API. */
    @FunctionalInterface
    interface Handler extends MetadataHandler<Predicates> {
      RelOptPredicateList getPredicates(RelNode r, RelMetadataQuery mq);

      @Override default MetadataDef<Predicates> getDef() {
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
  public interface AllPredicates extends Metadata {
    MetadataDef<AllPredicates> DEF =
            MetadataDef.of(AllPredicates.class, AllPredicates.Handler.class,
                BuiltInMethod.ALL_PREDICATES.method);

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
    interface Handler extends MetadataHandler<AllPredicates> {
      @Nullable RelOptPredicateList getAllPredicates(RelNode r, RelMetadataQuery mq);

      @Override default MetadataDef<AllPredicates> getDef() {
        return DEF;
      }

    }
  }

  /** Metadata about the degree of parallelism of a relational expression, and
   * how its operators are assigned to processes with independent resource
   * pools. */
  public interface Parallelism extends Metadata {
    MetadataDef<Parallelism> DEF =
        MetadataDef.of(Parallelism.class, Parallelism.Handler.class,
            BuiltInMethod.IS_PHASE_TRANSITION.method,
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
    interface Handler extends MetadataHandler<Parallelism> {
      Boolean isPhaseTransition(RelNode r, RelMetadataQuery mq);
      Integer splitCount(RelNode r, RelMetadataQuery mq);

      @Override default MetadataDef<Parallelism> getDef() {
        return DEF;
      }

    }
  }

  /** Metadata to get the lower bound cost of a RelNode. */
  public interface LowerBoundCost extends Metadata {
    MetadataDef<LowerBoundCost> DEF =
        MetadataDef.of(LowerBoundCost.class, LowerBoundCost.Handler.class,
            BuiltInMethod.LOWER_BOUND_COST.method);

    /** Returns the lower bound cost of a RelNode. */
    RelOptCost getLowerBoundCost(VolcanoPlanner planner);

    /** Handler API. */
    @FunctionalInterface
    interface Handler extends MetadataHandler<LowerBoundCost> {
      RelOptCost getLowerBoundCost(
          RelNode r, RelMetadataQuery mq, VolcanoPlanner planner);

      @Override default MetadataDef<LowerBoundCost> getDef() {
        return DEF;
      }

    }
  }

  /** Metadata about the memory use of an operator. */
  public interface Memory extends Metadata {
    MetadataDef<Memory> DEF =
        MetadataDef.of(Memory.class, Memory.Handler.class,
            BuiltInMethod.MEMORY.method,
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
    interface Handler extends MetadataHandler<Memory> {
      @Nullable Double memory(RelNode r, RelMetadataQuery mq);
      @Nullable Double cumulativeMemoryWithinPhase(RelNode r, RelMetadataQuery mq);
      @Nullable Double cumulativeMemoryWithinPhaseSplit(RelNode r, RelMetadataQuery mq);

      @Override default MetadataDef<Memory> getDef() {
        return DEF;
      }
    }
  }

  /** Metadata about whether a column is a measure and, if so, what is the
   * expression to evaluate that measure in the current context. */
  public interface Measure extends Metadata {
    MetadataDef<Measure> DEF =
        MetadataDef.of(Measure.class, Measure.Handler.class,
            BuiltInMethod.MEASURE_EXPAND.method,
            BuiltInMethod.IS_MEASURE.method);

    /** Returns whether a given column is a measure.
     *
     * @param column Column ordinal (0-based) */
    Boolean isMeasure(int column);

    /** Expands a measure to an expression.
     *
     * @param column Column ordinal (0-based)
     * @param context Evaluation context */
    RexNode expand(int column, Context context);

    /** Handler API. */
    interface Handler extends MetadataHandler<Measure> {
      Boolean isMeasure(RelNode r, RelMetadataQuery mq, int column);

      RexNode expand(RelNode r, RelMetadataQuery mq, int column,
          Context context);

      @Override default MetadataDef<Measure> getDef() {
        return DEF;
      }
    }

    /** Context for a use of a measure at a call site. */
    interface Context {
      RelBuilder getRelBuilder();

      default RexBuilder getRexBuilder() {
        return getRelBuilder().getRexBuilder();
      }

      default RelDataTypeFactory getTypeFactory() {
        return getRelBuilder().getTypeFactory();
      }

      /** Returns a (conjunctive) list of filters.
       *
       * <p>The filters represent the "filter context"
       * and will become the {@code WHERE} clause of the subquery.
       *
       * <p>If the relation defining the measure has {@code N} dimensions then
       * the dimensions can be referenced using
       * {@link org.apache.calcite.rex.RexInputRef} 0 through N-1. */
      List<RexNode> getFilters(RelBuilder b);

      /** Returns the number of dimension columns. */
      int getDimensionCount();
    }
  }

  /** Metadata about the functional dependencies among columns. */
  public interface FunctionalDependency extends Metadata {
    MetadataDef<FunctionalDependency> DEF =
        MetadataDef.of(FunctionalDependency.class, FunctionalDependency.Handler.class,
            BuiltInMethod.FUNCTIONAL_DEPENDENCY.method,
            BuiltInMethod.FUNCTIONAL_DEPENDENCY_SET.method,
            BuiltInMethod.FUNCTIONAL_DEPENDENCY_DEPENDENTS.method,
            BuiltInMethod.FUNCTIONAL_DEPENDENCY_DETERMINANTS.method,
            BuiltInMethod.FUNCTIONAL_DEPENDENCY_GET_FDS.method);

    /**
     * Returns whether one column functionally determines another.
     *
     * <p>For example,
     * {@code empno} functionally determines {@code sal},
     * because {@code empno} is the primary key.
     *
     * @param determinant 0-based ordinal of determinant column
     * @param dependent 0-based ordinal of dependent column
     * @return {@code true} if determinant uniquely determines dependent;
     *         {@code false} if not;
     *         {@code null} if unknown
     */
    @Nullable Boolean determines(int determinant, int dependent);

    /**
     * Returns whether a set of columns functionally determines another set of columns.
     *
     * <p>For example,
     * {{@code empno}, {@code deptno}} functionally determines {{@code sal}, {@code job}},
     * because the determinant ({@code empno}, {@code deptno}) is a superset of a key.
     * If we know that {{@code deptno}, {@code job}} functionally determines {{@code sal}}
     * then we cannot deduce that {@code deptno} alone determines {@code sal}.
     *
     * @param determinants 0-based ordinals of determinant columns
     * @param dependents 0-based ordinals of dependent columns
     * @return true if determinants uniquely determine dependents
     */
    Boolean determinesSet(ImmutableBitSet determinants, ImmutableBitSet dependents);

    /**
     * Returns the closure of {@code ordinals} under the functional dependency set.
     * The closure is the set of column ordinals uniquely determined by {@code ordinals}.
     *
     * <p>For example,
     * if input is {{@code empno}, {@code deptno}},
     * and functional dependencies contains
     * {{@code empno}, {@code deptno}} determines {{@code sal}, {@code job}},
     * then returns {{@code empno}, {@code deptno}, {@code sal}, {@code job}}.
     *
     * @param ordinals 0-based ordinals of determinant columns
     * @return closure: columns functionally determined by {@code ordinals}
     */
    ImmutableBitSet dependents(ImmutableBitSet ordinals);

    /**
     * Finds minimal determinant sets for the given dependent columns
     * under the functional dependency set.
     *
     * <p>For example,
     * if input is {{@code empno}, {@code deptno}, {@code sal}, {@code job}},
     * and functional dependencies contains
     * {{@code empno} determines {@code deptno}},
     * {{@code empno} determines {{@code sal},
     * {{@code empno} determines {@code job}},
     * then returns {{@code empno}}.
     *
     * @param ordinals 0-based ordinals of dependent columns
     * @return sets of minimal determinant columns
     */
    Set<ImmutableBitSet> determinants(ImmutableBitSet ordinals);

    /** Returns the full set of functional dependencies. */
    ArrowSet getFDs();

    /** Handler API. */
    interface Handler extends MetadataHandler<FunctionalDependency> {
      @Nullable Boolean determines(RelNode r, RelMetadataQuery mq, int key, int column);

      Boolean determinesSet(RelNode r, RelMetadataQuery mq,
          ImmutableBitSet determinants, ImmutableBitSet dependents);

      ImmutableBitSet dependents(RelNode r, RelMetadataQuery mq, ImmutableBitSet ordinals);

      Set<ImmutableBitSet> determinants(RelNode r, RelMetadataQuery mq, ImmutableBitSet ordinals);

      ArrowSet getFDs(RelNode r, RelMetadataQuery mq);

      @Override default MetadataDef<FunctionalDependency> getDef() {
        return DEF;
      }
    }
  }

  /** The built-in forms of metadata. */
  interface All extends Selectivity, UniqueKeys, RowCount, DistinctRowCount,
      PercentageOriginalRows, ColumnUniqueness, ColumnOrigin, Predicates,
      Collation, Distribution, Size, Parallelism, Memory, AllPredicates,
      ExpressionLineage, TableReferences, NodeTypes, FunctionalDependency {
  }
}
