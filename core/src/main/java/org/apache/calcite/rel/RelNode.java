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
import org.apache.calcite.plan.RelDigest;
import org.apache.calcite.plan.RelOptCost;
import org.apache.calcite.plan.RelOptNode;
import org.apache.calcite.plan.RelOptPlanner;
import org.apache.calcite.plan.RelOptTable;
import org.apache.calcite.plan.RelOptUtil;
import org.apache.calcite.plan.RelTraitSet;
import org.apache.calcite.rel.core.CorrelationId;
import org.apache.calcite.rel.metadata.Metadata;
import org.apache.calcite.rel.metadata.RelMetadataQuery;
import org.apache.calcite.rel.type.RelDataType;
import org.apache.calcite.rex.RexNode;
import org.apache.calcite.rex.RexShuttle;
import org.apache.calcite.util.Litmus;

import org.apiguardian.api.API;
import org.checkerframework.checker.nullness.qual.EnsuresNonNullIf;
import org.checkerframework.checker.nullness.qual.Nullable;
import org.checkerframework.dataflow.qual.Pure;

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
   * Return the CallingConvention trait from this RelNode's
   * {@link #getTraitSet() trait set}.
   *
   * @return this RelNode's CallingConvention
   */
  @Pure
  @Nullable Convention getConvention();

  /**
   * Returns the name of the variable which is to be implicitly set at runtime
   * each time a row is returned from the first input of this relational
   * expression; or null if there is no variable.
   *
   * @return Name of correlating variable, or null
   */
  @Nullable String getCorrelVariable();

  /**
   * Returns the <code>i</code><sup>th</sup> input relational expression.
   *
   * @param i Ordinal of input
   * @return <code>i</code><sup>th</sup> input
   */
  RelNode getInput(int i);

  /**
   * Returns the type of the rows returned by this relational expression.
   */
  @Override RelDataType getRowType();

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
  @Override List<RelNode> getInputs();

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
   * Returns the variables that are set in this relational
   * expression but also used and therefore not available to parents of this
   * relational expression.
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
  @Nullable RelOptCost computeSelfCost(RelOptPlanner planner, RelMetadataQuery mq);

  /**
   * Returns a metadata interface.
   *
   * @deprecated Use {@link RelMetadataQuery} via {@link #getCluster()}.
   *
   * @param <M> Type of metadata being requested
   * @param metadataClass Metadata interface
   * @param mq Metadata query
   *
   * @return Metadata object that supplies the desired metadata (never null,
   *     although if the information is not present the metadata object may
   *     return null from all methods)
   */
  @Deprecated // to be removed before 2.0
  <@Nullable M extends @Nullable Metadata> M metadata(Class<M> metadataClass, RelMetadataQuery mq);

  /**
   * Describes the inputs and attributes of this relational expression.
   * Each node should call {@code super.explain}, then call the
   * {@link org.apache.calcite.rel.externalize.RelWriterImpl#input(String, RelNode)}
   * and
   * {@link RelWriter#item(String, Object)}
   * methods for each input and attribute.
   *
   * @param pw Plan writer
   */
  void explain(RelWriter pw);

  /**
   * Returns a relational expression string of this {@code RelNode}.
   * The string returned is the same as
   * {@link RelOptUtil#toString(org.apache.calcite.rel.RelNode)}.
   *
   * This method is intended mainly for use while debugging in an IDE,
   * as a convenient short-hand for RelOptUtil.toString.
   * We recommend that classes implementing this interface
   * do not override this method.
   *
   * @return Relational expression string of this {@code RelNode}
   */
  default String explain() {
    return RelOptUtil.toString(this);
  }

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
   * Returns a digest string of this {@code RelNode}.
   *
   * <p>Each call creates a new digest string,
   * so don't forget to cache the result if necessary.
   *
   * @return Digest string of this {@code RelNode}
   *
   * @see #getRelDigest()
   */
  @Override default String getDigest() {
    return getRelDigest().toString();
  }

  /**
   * Returns a digest of this {@code RelNode}.
   *
   * <p>INTERNAL USE ONLY. For use by the planner.
   *
   * @return Digest of this {@code RelNode}
   * @see #getDigest()
   */
  @API(since = "1.24", status = API.Status.INTERNAL)
  RelDigest getRelDigest();

  /**
   * Recomputes the digest.
   *
   * <p>INTERNAL USE ONLY. For use by the planner.
   *
   * @see #getDigest()
   */
  @API(since = "1.24", status = API.Status.INTERNAL)
  void recomputeDigest();

  /**
   * Deep equality check for RelNode digest.
   *
   * <p>By default this method collects digest attributes from
   * explain terms, then compares each attribute pair.</p>
   *
   * @return Whether the 2 RelNodes are equivalent or have the same digest.
   * @see #deepHashCode()
   */
  @EnsuresNonNullIf(expression = "#1", result = true)
  boolean deepEquals(@Nullable Object obj);

  /**
   * Compute deep hash code for RelNode digest.
   *
   * @see #deepEquals(Object)
   */
  int deepHashCode();

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
  @Nullable RelOptTable getTable();

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
  boolean isValid(Litmus litmus, @Nullable Context context);

  /**
   * Creates a copy of this relational expression, perhaps changing traits and
   * inputs.
   *
   * <p>Sub-classes with other important attributes are encouraged to create
   * variants of this method with more parameters.
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
   * Indicates whether it is an enforcer operator, e.g. PhysicalSort,
   * PhysicalHashDistribute, etc. As an enforcer, the operator must be
   * created only when required traitSet is not satisfied by its input.
   *
   * @return Whether it is an enforcer operator
   */
  default boolean isEnforcer() {
    return false;
  }

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

  /** Returns whether a field is nullable. */
  default boolean fieldIsNullable(int i) {
    return getRowType().getFieldList().get(i).getType().isNullable();
  }

  /** Context of a relational expression, for purposes of checking validity. */
  interface Context {
    Set<CorrelationId> correlationIds();
  }
}
