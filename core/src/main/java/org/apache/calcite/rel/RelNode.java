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
package org.apache.calcite.rel;

import org.apache.calcite.plan.Convention;
import org.apache.calcite.plan.RelOptCost;
import org.apache.calcite.plan.RelOptNode;
import org.apache.calcite.plan.RelOptPlanner;
import org.apache.calcite.plan.RelOptQuery;
import org.apache.calcite.plan.RelOptTable;
import org.apache.calcite.plan.RelTraitSet;
import org.apache.calcite.rel.core.CorrelationId;
import org.apache.calcite.rel.metadata.Metadata;
import org.apache.calcite.rel.metadata.RelMetadataQuery;
import org.apache.calcite.rel.type.RelDataType;
import org.apache.calcite.rex.RexNode;
import org.apache.calcite.rex.RexShuttle;
import org.apache.calcite.util.ImmutableBitSet;
import org.apache.calcite.util.Litmus;

import java.util.List;
import java.util.Set;

/**
 * A <code>RelNode</code> is a relational expression.
 *
 * <p>Relational expressions process data, so their names are typically verbs:
 * Sort, Join, Project, Filter, Scan, Sample.
 *
 * <p>A relational expression is not a scalar expression; see
 * {@link org.apache.calcite.sql.SqlNode} and {@link RexNode}.</p>
 *
 * <p>If this type of relational expression has some particular planner rules,
 * it should implement the <em>public static</em> method
 * {@link AbstractRelNode#register}.</p>
 *
 * <p>When a relational expression comes to be implemented, the system allocates
 * a {@link org.apache.calcite.plan.RelImplementor} to manage the process. Every
 * implementable relational expression has a {@link RelTraitSet} describing its
 * physical attributes. The RelTraitSet always contains a {@link Convention}
 * describing how the expression passes data to its consuming
 * relational expression, but may contain other traits, including some applied
 * externally. Because traits can be applied externally, implementations of
 * RelNode should never assume the size or contents of their trait set (beyond
 * those traits configured by the RelNode itself).</p>
 *
 * <p>For each calling-convention, there is a corresponding sub-interface of
 * RelNode. For example,
 * {@code org.apache.calcite.adapter.enumerable.EnumerableRel}
 * has operations to manage the conversion to a graph of
 * {@code org.apache.calcite.adapter.enumerable.EnumerableConvention}
 * calling-convention, and it interacts with a
 * {@code EnumerableRelImplementor}.</p>
 *
 * <p>A relational expression is only required to implement its
 * calling-convention's interface when it is actually implemented, that is,
 * converted into a plan/program. This means that relational expressions which
 * cannot be implemented, such as converters, are not required to implement
 * their convention's interface.</p>
 *
 * <p>Every relational expression must derive from {@link AbstractRelNode}. (Why
 * have the <code>RelNode</code> interface, then? We need a root interface,
 * because an interface can only derive from an interface.)</p>
 */
public interface RelNode extends RelOptNode, Cloneable {
  //~ Methods ----------------------------------------------------------------

  /**
   * Returns a list of this relational expression's child expressions.
   * (These are scalar expressions, and so do not include the relational
   * inputs that are returned by {@link #getInputs}.
   *
   * <p>The caller should treat the list as unmodifiable; typical
   * implementations will return an immutable list. If there are no
   * child expressions, returns an empty list, not <code>null</code>.
   *
   * @deprecated use #accept(org.apache.calcite.rex.RexShuttle)
   * @return List of this relational expression's child expressions
   * @see #accept(org.apache.calcite.rex.RexShuttle)
   */
  @Deprecated // to be removed before 2.0
  List<RexNode> getChildExps();

  /**
   * Return the CallingConvention trait from this RelNode's
   * {@link #getTraitSet() trait set}.
   *
   * @return this RelNode's CallingConvention
   */
  Convention getConvention();

  /**
   * Returns the name of the variable which is to be implicitly set at runtime
   * each time a row is returned from the first input of this relational
   * expression; or null if there is no variable.
   *
   * @return Name of correlating variable, or null
   */
  String getCorrelVariable();

  /**
   * Returns whether the same value will not come out twice. Default value is
   * <code>false</code>, derived classes should override.
   *
   * @return Whether the same value will not come out twice
   *
   * @deprecated Use {@link RelMetadataQuery#areRowsUnique(RelNode)}
   */
  @Deprecated // to be removed before 2.0
  boolean isDistinct();

  /**
   * Returns the <code>i</code><sup>th</sup> input relational expression.
   *
   * @param i Ordinal of input
   * @return <code>i</code><sup>th</sup> input
   */
  RelNode getInput(int i);

  /**
   * Returns the sub-query this relational expression belongs to.
   *
   * @return Sub-query
   */
  @Deprecated // to be removed before 2.0
  RelOptQuery getQuery();

  /**
   * Returns the type of the rows returned by this relational expression.
   */
  RelDataType getRowType();

  /**
   * Returns the type of the rows expected for an input. Defaults to
   * {@link #getRowType}.
   *
   * @param ordinalInParent input's 0-based ordinal with respect to this
   *                        parent rel
   * @return expected row type
   */
  RelDataType getExpectedInputRowType(int ordinalInParent);

  /**
   * Returns an array of this relational expression's inputs. If there are no
   * inputs, returns an empty list, not {@code null}.
   *
   * @return Array of this relational expression's inputs
   */
  List<RelNode> getInputs();

  /**
   * Returns an estimate of the number of rows this relational expression will
   * return.
   *
   * <p>NOTE jvs 29-Mar-2006: Don't call this method directly. Instead, use
   * {@link RelMetadataQuery#getRowCount}, which gives plugins a chance to
   * override the rel's default ideas about row count.
   *
   * @param mq Metadata query
   * @return Estimate of the number of rows this relational expression will
   *   return
   */
  double estimateRowCount(RelMetadataQuery mq);

  /**
   * @deprecated Call {@link RelMetadataQuery#getRowCount(RelNode)};
   * if you wish to override the default row count formula, override the
   * {@link #estimateRowCount(RelMetadataQuery)} method.
   */
  @Deprecated // to be removed before 2.0
  double getRows();

  /**
   * Returns the names of variables that are set in this relational
   * expression but also used and therefore not available to parents of this
   * relational expression.
   *
   * <p>Note: only {@link org.apache.calcite.rel.core.Correlate} should set
   * variables.
   *
   * <p>Note: {@link #getVariablesSet()} is equivalent but returns
   * {@link CorrelationId} rather than their names. It is preferable except for
   * calling old methods that require a set of strings.
   *
   * @return Names of variables which are set in this relational
   *   expression
   *
   * @deprecated Use {@link #getVariablesSet()}
   * and {@link CorrelationId#names(Set)}
   */
  @Deprecated // to be removed before 2.0
  Set<String> getVariablesStopped();

  /**
   * Returns the variables that are set in this relational
   * expression but also used and therefore not available to parents of this
   * relational expression.
   *
   * <p>Note: only {@link org.apache.calcite.rel.core.Correlate} should set
   * variables.
   *
   * @return Names of variables which are set in this relational
   *   expression
   */
  Set<CorrelationId> getVariablesSet();

  /**
   * Collects variables known to be used by this expression or its
   * descendants. By default, no such information is available and must be
   * derived by analyzing sub-expressions, but some optimizer implementations
   * may insert special expressions which remember such information.
   *
   * @param variableSet receives variables used
   */
  void collectVariablesUsed(Set<CorrelationId> variableSet);

  /**
   * Collects variables set by this expression.
   * TODO: is this required?
   *
   * @param variableSet receives variables known to be set by
   */
  void collectVariablesSet(Set<CorrelationId> variableSet);

  /**
   * Interacts with the {@link RelVisitor} in a
   * {@link org.apache.calcite.util.Glossary#VISITOR_PATTERN visitor pattern} to
   * traverse the tree of relational expressions.
   *
   * @param visitor Visitor that will traverse the tree of relational
   *                expressions
   */
  void childrenAccept(RelVisitor visitor);

  /**
   * Returns the cost of this plan (not including children). The base
   * implementation throws an error; derived classes should override.
   *
   * <p>NOTE jvs 29-Mar-2006: Don't call this method directly. Instead, use
   * {@link RelMetadataQuery#getNonCumulativeCost}, which gives plugins a
   * chance to override the rel's default ideas about cost.
   *
   * @param planner Planner for cost calculation
   * @param mq Metadata query
   * @return Cost of this plan (not including children)
   */
  RelOptCost computeSelfCost(RelOptPlanner planner, RelMetadataQuery mq);

  /**
   * @deprecated Call {@link RelMetadataQuery#getNonCumulativeCost(RelNode)};
   * if you wish to override the default cost formula, override the
   * {@link #computeSelfCost(RelOptPlanner, RelMetadataQuery)} method.
   */
  @Deprecated // to be removed before 2.0
  RelOptCost computeSelfCost(RelOptPlanner planner);

  /**
   * Returns a metadata interface.
   *
   * @param <M> Type of metadata being requested
   * @param metadataClass Metadata interface
   * @param mq Metadata query
   *
   * @return Metadata object that supplies the desired metadata (never null,
   *     although if the information is not present the metadata object may
   *     return null from all methods)
   */
  <M extends Metadata> M metadata(Class<M> metadataClass, RelMetadataQuery mq);

  /**
   * Describes the inputs and attributes of this relational expression.
   * Each node should call {@code super.explain}, then call the
   * {@link org.apache.calcite.rel.externalize.RelWriterImpl#input(String, RelNode)}
   * and
   * {@link org.apache.calcite.rel.externalize.RelWriterImpl#item(String, Object)}
   * methods for each input and attribute.
   *
   * @param pw Plan writer
   */
  void explain(RelWriter pw);

  /**
   * Receives notification that this expression is about to be registered. The
   * implementation of this method must at least register all child
   * expressions.
   *
   * @param planner Planner that plans this relational node
   * @return Relational expression that should be used by the planner
   */
  RelNode onRegister(RelOptPlanner planner);

  /**
   * Computes the digest, assigns it, and returns it. For planner use only.
   *
   * @return Digest of this relational expression
   */
  String recomputeDigest();

  /**
   * Replaces the <code>ordinalInParent</code><sup>th</sup> input. You must
   * override this method if you override {@link #getInputs}.
   *
   * @param ordinalInParent Position of the child input, 0 is the first
   * @param p New node that should be put at position {@code ordinalInParent}
   */
  void replaceInput(
      int ordinalInParent,
      RelNode p);

  /**
   * If this relational expression represents an access to a table, returns
   * that table, otherwise returns null.
   *
   * @return If this relational expression represents an access to a table,
   *   returns that table, otherwise returns null
   */
  RelOptTable getTable();

  /**
   * Returns the name of this relational expression's class, sans package
   * name, for use in explain. For example, for a <code>
   * org.apache.calcite.rel.ArrayRel.ArrayReader</code>, this method returns
   * "ArrayReader".
   *
   * @return Name of this relational expression's class, sans package name,
   *   for use in explain
   */
  String getRelTypeName();

  /**
   * Returns whether this relational expression is valid.
   *
   * <p>If assertions are enabled, this method is typically called with <code>
   * litmus</code> = <code>THROW</code>, as follows:
   *
   * <blockquote>
   * <pre>assert rel.isValid(Litmus.THROW)</pre>
   * </blockquote>
   *
   * <p>This signals that the method can throw an {@link AssertionError} if it
   * is not valid.
   *
   * @param litmus What to do if invalid
   * @param context Context for validity checking
   * @return Whether relational expression is valid
   * @throws AssertionError if this relational expression is invalid and
   *                        litmus is THROW
   */
  boolean isValid(Litmus litmus, Context context);

  @Deprecated // to be removed before 2.0
  boolean isValid(boolean fail);

  /**
   * Returns a description of the physical ordering (or orderings) of this
   * relational expression. Never null.
   *
   * @return Description of the physical ordering (or orderings) of this
   *   relational expression. Never null
   *
   * @deprecated Use {@link RelMetadataQuery#distribution(RelNode)}
   */
  @Deprecated // to be removed before 2.0
  List<RelCollation> getCollationList();

  /**
   * Creates a copy of this relational expression, perhaps changing traits and
   * inputs.
   *
   * <p>Sub-classes with other important attributes are encouraged to create
   * variants of this method with more parameters.</p>
   *
   * @param traitSet Trait set
   * @param inputs   Inputs
   * @return Copy of this relational expression, substituting traits and
   * inputs
   */
  RelNode copy(
      RelTraitSet traitSet,
      List<RelNode> inputs);

  /**
   * Registers any special rules specific to this kind of relational
   * expression.
   *
   * <p>The planner calls this method this first time that it sees a
   * relational expression of this class. The derived class should call
   * {@link org.apache.calcite.plan.RelOptPlanner#addRule} for each rule, and
   * then call {@code super.register}.</p>
   *
   * @param planner Planner to be used to register additional relational
   *                expressions
   */
  void register(RelOptPlanner planner);

  /**
   * Returns whether the result of this relational expression is uniquely
   * identified by this columns with the given ordinals.
   *
   * <p>For example, if this relational expression is a LogicalTableScan to
   * T(A, B, C, D) whose key is (A, B), then isKey([0, 1]) yields true,
   * and isKey([0]) and isKey([0, 2]) yields false.</p>
   *
   * @param columns Ordinals of key columns
   * @return Whether the given columns are a key or a superset of a key
   *
   * @deprecated Use {@link RelMetadataQuery#areColumnsUnique(RelNode, ImmutableBitSet)}
   */
  @Deprecated // to be removed before 2.0
  boolean isKey(ImmutableBitSet columns);

  /**
   * Accepts a visit from a shuttle.
   *
   * @param shuttle Shuttle
   * @return A copy of this node incorporating changes made by the shuttle to
   * this node's children
   */
  RelNode accept(RelShuttle shuttle);

  /**
   * Accepts a visit from a shuttle. If the shuttle updates expression, then
   * a copy of the relation should be created. This new relation might have
   * a different row-type.
   *
   * @param shuttle Shuttle
   * @return A copy of this node incorporating changes made by the shuttle to
   * this node's children
   */
  RelNode accept(RexShuttle shuttle);

  /** Context of a relational expression, for purposes of checking validity. */
  interface Context {
    Set<CorrelationId> correlationIds();
  }
}

// End RelNode.java
