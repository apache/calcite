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
package org.eigenbase.rel;

import java.util.*;

import org.eigenbase.rel.metadata.*;
import org.eigenbase.relopt.*;
import org.eigenbase.reltype.*;
import org.eigenbase.rex.*;

/**
 * A <code>RelNode</code> is a relational expression.
 *
 * <p>A relational expression is not a scalar expression; see
 * {@link org.eigenbase.sql.SqlNode} and {@link RexNode}.</p>
 *
 * <p>If this type of relational expression has some particular planner rules,
 * it should implement the <em>public static</em> method
 * {@link AbstractRelNode#register}.</p>
 *
 * <p>When a relational expression comes to be implemented, the system allocates
 * a {@link org.eigenbase.relopt.RelImplementor} to manage the process. Every
 * implementable relational expression has a {@link RelTraitSet} describing its
 * physical attributes. The RelTraitSet always contains a {@link Convention}
 * describing how the expression passes data to its consuming
 * relational expression, but may contain other traits, including some applied
 * externally. Because traits can be applied externally, implementations of
 * RelNode should never assume the size or contents of their trait set (beyond
 * those traits configured by the RelNode itself).</p>
 *
 * <p>For each calling-convention, there is a corresponding sub-interface of
 * RelNode. For example, {@code net.hydromatic.optiq.rules.java.EnumerableRel}
 * has operations to manage the conversion to a graph of
 * {@code net.hydromatic.optiq.rules.java.EnumerableConvention}
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
   */
  List<RexNode> getChildExps();

  /**
   * Return the CallingConvention trait from this RelNode's {@link
   * #getTraitSet() trait set}.
   *
   * @return this RelNode's CallingConvention
   */
  Convention getConvention();

  /**
   * Sets the name of the variable which is to be implicitly set at runtime
   * each time a row is returned from this relational expression
   *
   * @param correlVariable Name of correlating variable
   */
  void setCorrelVariable(String correlVariable);

  /**
   * Returns the name of the variable which is to be implicitly set at runtime
   * each time a row is returned from this relational expression; or null if
   * there is no variable.
   *
   * @return Name of correlating variable, or null
   */
  String getCorrelVariable();

  /**
   * Returns whether the same value will not come out twice. Default value is
   * <code>false</code>, derived classes should override.
   */
  boolean isDistinct();

  /**
   * Returns the <code>i</code><sup>th</sup> input relational expression.
   *
   * @param i Ordinal of input
   * @return <code>i</code><sup>th</sup> input
   */
  RelNode getInput(int i);

  /**
   * Returns a variable with which to reference the current row of this
   * relational expression as a correlating variable. Creates a variable if
   * none exists.
   */
  String getOrCreateCorrelVariable();

  /**
   * Returns the sub-query this relational expression belongs to. A sub-query
   * determines the scope for correlating variables (see {@link
   * #setCorrelVariable(String)}).
   *
   * @return Sub-query
   */
  RelOptQuery getQuery();

  /**
   * Returns the type of the rows returned by this relational expression.
   */
  RelDataType getRowType();

  /**
   * Returns the type of the rows expected for an input. Defaults to {@link
   * #getRowType}.
   *
   * @param ordinalInParent input's 0-based ordinal with respect to this
   *                        parent rel
   * @return expected row type
   */
  RelDataType getExpectedInputRowType(int ordinalInParent);

  /**
   * Returns an array of this relational expression's inputs. If there are no
   * inputs, returns an empty array, not <code>null</code>.
   */
  List<RelNode> getInputs();

  /**
   * Returns an estimate of the number of rows this relational expression will
   * return.
   *
   * <p>NOTE jvs 29-Mar-2006: Don't call this method directly. Instead, use
   * {@link RelMetadataQuery#getRowCount}, which gives plugins a chance to
   * override the rel's default ideas about row count.
   */
  double getRows();

  /**
   * Returns the names of variables which are set in this relational
   * expression but also used and therefore not available to parents of this
   * relational expression.
   *
   * <p>By default, returns the empty set. Derived classes may override this
   * method.</p>
   */
  Set<String> getVariablesStopped();

  /**
   * Collects variables known to be used by this expression or its
   * descendants. By default, no such information is available and must be
   * derived by analyzing sub-expressions, but some optimizer implementations
   * may insert special expressions which remember such information.
   *
   * @param variableSet receives variables used
   */
  void collectVariablesUsed(Set<String> variableSet);

  /**
   * Collects variables set by this expression.
   *
   * @param variableSet receives variables known to be set by
   */
  void collectVariablesSet(Set<String> variableSet);

  /**
   * Interacts with the {@link RelVisitor} in a {@link
   * org.eigenbase.util.Glossary#VISITOR_PATTERN visitor pattern} to traverse
   * the tree of relational expressions.
   */
  void childrenAccept(RelVisitor visitor);

  /**
   * Returns the cost of this plan (not including children). The base
   * implementation throws an error; derived classes should override.
   *
   * <p>NOTE jvs 29-Mar-2006: Don't call this method directly. Instead, use
   * {@link RelMetadataQuery#getNonCumulativeCost}, which gives plugins a
   * chance to override the rel's default ideas about cost.
   */
  RelOptCost computeSelfCost(RelOptPlanner planner);

  /**
   * Returns a metadata interface.
   *
   * @param metadataClass Metadata interface
   * @param <M> Type of metadata being requested
   * @return Metadata object that supplies the desired metadata (never null,
   *     although if the information is not present the metadata object may
   *     return null from all methods)
   */
  <M extends Metadata> M metadata(Class<M> metadataClass);

  /**
   * Describes the inputs and attributes of this relational expression.
   * Each node should call {@code super.explain}, then call the
   * {@link RelWriterImpl#input(String, RelNode)}
   * and {@link RelWriterImpl#item(String, Object)} methods for each input
   * and attribute.
   *
   * @param pw Plan writer
   */
  void explain(RelWriter pw);

  /**
   * Receives notification that this expression is about to be registered. The
   * implementation of this method must at least register all child
   * expressions.
   */
  RelNode onRegister(RelOptPlanner planner);

  /**
   * Computes the digest, assigns it, and returns it. For planner use only.
   */
  String recomputeDigest();

  /**
   * Registers a correlation variable.
   *
   * @see #getVariablesStopped
   */
  void registerCorrelVariable(String correlVariable);

  /**
   * Replaces the <code>ordinalInParent</code><sup>th</sup> input. You must
   * override this method if you override {@link #getInputs}.
   */
  void replaceInput(
      int ordinalInParent,
      RelNode p);

  /**
   * If this relational expression represents an access to a table, returns
   * that table, otherwise returns null.
   */
  RelOptTable getTable();

  /**
   * Returns the name of this relational expression's class, sans package
   * name, for use in explain. For example, for a <code>
   * org.eigenbase.rel.ArrayRel.ArrayReader</code>, this method returns
   * "ArrayReader".
   */
  String getRelTypeName();

  /**
   * Returns whether this relational expression is valid.
   *
   * <p>If assertions are enabled, this method is typically called with <code>
   * fail</code> = <code>true</code>, as follows:
   *
   * <blockquote>
   * <pre>assert rel.isValid(true)</pre>
   * </blockquote>
   *
   * This signals that the method can throw an {@link AssertionError} if it is
   * not valid.
   *
   * @param fail Whether to fail if invalid
   * @return Whether relational expression is valid
   * @throws AssertionError if this relational expression is invalid and
   *                        fail=true and assertions are enabled
   */
  boolean isValid(boolean fail);

  /**
   * Returns a description of the physical ordering (or orderings) of this
   * relational expression. Never null.
   */
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
   * relational expression of this class. The derived class should call {@link
   * org.eigenbase.relopt.RelOptPlanner#addRule} for each rule, and then call
   * {@code super.register}.</p>
   */
  void register(RelOptPlanner planner);

  /**
   * Returns whether the result of this relational expression is uniquely
   * identified by this columns with the given ordinals.
   *
   * <p>For example, if this relational expression is a TableAccessRel to
   * T(A, B, C, D) whose key is (A, B), then isKey([0, 1]) yields true,
   * and isKey([0]) and isKey([0, 2]) yields false.</p>
   *
   * @param columns Ordinals of key columns
   * @return Whether the given columns are a key or a superset of a key
   */
  boolean isKey(BitSet columns);

  /**
   * Accepts a visit from a shuttle.
   *
   * @param shuttle Shuttle
   * @return A copy of this node incorporating changes made by the shuttle to
   * this node's children
   */
  RelNode accept(RelShuttle shuttle);
}

// End RelNode.java
