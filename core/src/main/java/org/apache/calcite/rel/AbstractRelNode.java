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
import org.apache.calcite.plan.ConventionTraitDef;
import org.apache.calcite.plan.RelDigest;
import org.apache.calcite.plan.RelOptCluster;
import org.apache.calcite.plan.RelOptCost;
import org.apache.calcite.plan.RelOptPlanner;
import org.apache.calcite.plan.RelOptTable;
import org.apache.calcite.plan.RelOptUtil;
import org.apache.calcite.plan.RelTraitSet;
import org.apache.calcite.rel.core.CorrelationId;
import org.apache.calcite.rel.hint.Hintable;
import org.apache.calcite.rel.hint.RelHint;
import org.apache.calcite.rel.metadata.Metadata;
import org.apache.calcite.rel.metadata.MetadataFactory;
import org.apache.calcite.rel.metadata.RelMetadataQuery;
import org.apache.calcite.rel.type.RelDataType;
import org.apache.calcite.rex.RexShuttle;
import org.apache.calcite.runtime.PairList;
import org.apache.calcite.sql.SqlExplainLevel;
import org.apache.calcite.util.Litmus;
import org.apache.calcite.util.Pair;
import org.apache.calcite.util.Util;

import com.google.common.collect.ImmutableSet;

import org.apiguardian.api.API;
import org.checkerframework.checker.initialization.qual.UnknownInitialization;
import org.checkerframework.checker.nullness.qual.MonotonicNonNull;
import org.checkerframework.checker.nullness.qual.Nullable;
import org.checkerframework.dataflow.qual.Pure;

import java.util.ArrayList;
import java.util.Collections;
import java.util.List;
import java.util.Map;
import java.util.Set;
import java.util.concurrent.atomic.AtomicInteger;

import static com.google.common.base.Preconditions.checkNotNull;

import static java.util.Objects.requireNonNull;

/**
 * Base class for every relational expression ({@link RelNode}).
 */
public abstract class AbstractRelNode implements RelNode {
  //~ Static fields/initializers ---------------------------------------------

  /** Generator for {@link #id} values. */
  private static final AtomicInteger NEXT_ID = new AtomicInteger(0);

  //~ Instance fields --------------------------------------------------------

  /**
   * Cached type of this relational expression.
   */
  protected @MonotonicNonNull RelDataType rowType;

  /**
   * The digest that uniquely identifies the node.
   */
  @API(since = "1.24", status = API.Status.INTERNAL)
  protected final RelDigest digest;

  private final RelOptCluster cluster;

  /** Unique id of this object, for debugging. */
  protected final int id;

  /** RelTraitSet that describes the traits of this RelNode. */
  protected final RelTraitSet traitSet;

  //~ Constructors -----------------------------------------------------------

  /**
   * Creates an <code>AbstractRelNode</code>.
   */
  protected AbstractRelNode(RelOptCluster cluster, RelTraitSet traitSet) {
    super();
    this.cluster = requireNonNull(cluster, "cluster");
    this.traitSet = requireNonNull(traitSet, "traitSet");
    this.id = NEXT_ID.getAndIncrement();
    this.digest = new InnerRelDigest();
  }

  //~ Methods ----------------------------------------------------------------

  @Override public RelNode copy(RelTraitSet traitSet, List<RelNode> inputs) {
    // Note that empty set equals empty set, so relational expressions
    // with zero inputs do not generally need to implement their own copy
    // method.
    if (getInputs().equals(inputs)
        && traitSet == getTraitSet()) {
      return this;
    }
    throw new AssertionError("Relational expression should override copy. "
        + "Class=[" + getClass()
        + "]; traits=[" + getTraitSet()
        + "]; desired traits=[" + traitSet
        + "]");
  }

  protected static <T> T sole(List<T> collection) {
    assert collection.size() == 1;
    return collection.get(0);
  }

  @Override public final RelOptCluster getCluster() {
    return cluster;
  }

  @Pure
  @Override public final @Nullable Convention getConvention(
      @UnknownInitialization AbstractRelNode this) {
    return traitSet == null ? null : traitSet.getTrait(ConventionTraitDef.INSTANCE);
  }

  @Override public RelTraitSet getTraitSet() {
    return traitSet;
  }

  @Override public @Nullable String getCorrelVariable() {
    return null;
  }

  @Override public int getId() {
    return id;
  }

  @Override public RelNode getInput(int i) {
    List<RelNode> inputs = getInputs();
    return inputs.get(i);
  }

  @Override public void register(RelOptPlanner planner) {
    Util.discard(planner);
  }

  // It is not recommended to override this method, but sub-classes can do it at their own risk.
  @Override public String getRelTypeName() {
    String cn = getClass().getName();
    int i = cn.length();
    while (--i >= 0) {
      if (cn.charAt(i) == '$' || cn.charAt(i) == '.') {
        return cn.substring(i + 1);
      }
    }
    return cn;
  }

  @Override public boolean isValid(Litmus litmus, @Nullable Context context) {
    return litmus.succeed();
  }

  @Override public final RelDataType getRowType() {
    if (rowType == null) {
      rowType = checkNotNull(deriveRowType(), "null row type for %s", this);
    }
    return rowType;
  }

  protected RelDataType deriveRowType() {
    // This method is only called if rowType is null, so you don't NEED to
    // implement it if rowType is always set.
    throw new UnsupportedOperationException();
  }

  @Override public RelDataType getExpectedInputRowType(int ordinalInParent) {
    return getRowType();
  }

  @Override public List<RelNode> getInputs() {
    return Collections.emptyList();
  }

  @Override public double estimateRowCount(RelMetadataQuery mq) {
    return 1.0;
  }

  @Override public Set<CorrelationId> getVariablesSet() {
    return ImmutableSet.of();
  }

  @Override public void collectVariablesUsed(Set<CorrelationId> variableSet) {
    // for default case, nothing to do
  }

  @Override public boolean isEnforcer() {
    return false;
  }

  @Override public void collectVariablesSet(Set<CorrelationId> variableSet) {
  }

  @Override public void childrenAccept(RelVisitor visitor) {
    List<RelNode> inputs = getInputs();
    for (int i = 0; i < inputs.size(); i++) {
      visitor.visit(inputs.get(i), i, this);
    }
  }

  @Override public RelNode accept(RelShuttle shuttle) {
    // Call fall-back method. Specific logical types (such as LogicalProject
    // and LogicalJoin) have their own RelShuttle.visit methods.
    return shuttle.visit(this);
  }

  @Override public RelNode accept(RexShuttle shuttle) {
    return this;
  }

  @Override public @Nullable RelOptCost computeSelfCost(RelOptPlanner planner,
      RelMetadataQuery mq) {
    // by default, assume cost is proportional to number of rows
    double rowCount = mq.getRowCount(this);
    return planner.getCostFactory().makeCost(rowCount, rowCount, 0);
  }

  @Deprecated // to be removed before 2.0
  @Override public final <@Nullable M extends @Nullable Metadata> M metadata(Class<M> metadataClass,
      RelMetadataQuery mq) {
    final MetadataFactory factory = cluster.getMetadataFactory();
    final M metadata = factory.query(this, mq, metadataClass);
    checkNotNull(metadata, "no provider found (rel=%s, m=%s); "
        + "a backstop provider is recommended", this, metadataClass);
    // Usually the metadata belongs to the rel that created it. RelSubset and
    // HepRelVertex are notable exceptions, so disable the assertion. It's not
    // worth the performance hit to override this method for them.
    //   assert metadata.rel() == this : "someone else's metadata";
    return metadata;
  }

  @Override public void explain(RelWriter pw) {
    explainTerms(pw).done(this);
  }

  /**
   * Describes the inputs and attributes of this relational expression.
   * Each node should call {@code super.explainTerms}, then call the
   * {@link org.apache.calcite.rel.externalize.RelWriterImpl#input(String, RelNode)}
   * and
   * {@link RelWriter#item(String, Object)}
   * methods for each input and attribute.
   *
   * @param pw Plan writer
   * @return Plan writer for fluent-explain pattern
   */
  public RelWriter explainTerms(RelWriter pw) {
    return pw;
  }

  @Override public RelNode onRegister(RelOptPlanner planner) {
    List<RelNode> oldInputs = getInputs();
    List<RelNode> inputs = new ArrayList<>(oldInputs.size());
    for (final RelNode input : oldInputs) {
      RelNode e = planner.ensureRegistered(input, null);
      assert e == input || RelOptUtil.equal("rowtype of rel before registration",
          input.getRowType(),
          "rowtype of rel after registration",
          e.getRowType(),
          Litmus.THROW);
      inputs.add(e);
    }
    RelNode r = this;
    if (!Util.equalShallow(oldInputs, inputs)) {
      r = copy(getTraitSet(), inputs);
    }
    r.recomputeDigest();
    assert r.isValid(Litmus.THROW, null);
    return r;
  }

  @Override public void recomputeDigest() {
    digest.clear();
  }

  @Override public void replaceInput(
      int ordinalInParent,
      RelNode p) {
    throw new UnsupportedOperationException("replaceInput called on " + this);
  }

  /** Description; consists of id plus digest. */
  @Override public String toString() {
    return "rel#" + id + ':' + getDigest();
  }

  @Deprecated // to be removed before 2.0
  @Override public final String getDescription() {
    return this.toString();
  }

  @Override public String getDigest() {
    return digest.toString();
  }

  @Override public final RelDigest getRelDigest() {
    return digest;
  }

  @Override public @Nullable RelOptTable getTable() {
    return null;
  }

  /**
   * {@inheritDoc}
   *
   * <p>This method (and {@link #hashCode} is intentionally final. We do not want
   * sub-classes of {@link RelNode} to redefine identity. Various algorithms
   * (e.g. visitors, planner) can define the identity as meets their needs.
   */
  @Override public final boolean equals(@Nullable Object obj) {
    return super.equals(obj);
  }

  /**
   * {@inheritDoc}
   *
   * <p>This method (and {@link #equals} is intentionally final. We do not want
   * sub-classes of {@link RelNode} to redefine identity. Various algorithms
   * (e.g. visitors, planner) can define the identity as meets their needs.
   */
  @Override public final int hashCode() {
    return super.hashCode();
  }

  /**
   * Equality check for RelNode digest.
   *
   * <p>By default this method collects digest attributes from
   * {@link #explainTerms(RelWriter)}, then compares each attribute pair.
   * This should work well for most cases. If this method is a performance
   * bottleneck for your project, or the default behavior can't handle
   * your scenario properly, you can choose to override this method and
   * {@link #deepHashCode()}. See {@code LogicalJoin} as an example.
   *
   * @return Whether the 2 RelNodes are equivalent or have the same digest.
   * @see #deepHashCode()
   */
  @API(since = "1.25", status = API.Status.MAINTAINED)
  @Override public boolean deepEquals(@Nullable Object obj) {
    if (this == obj) {
      return true;
    }
    if (obj == null || this.getClass() != obj.getClass()) {
      return false;
    }
    AbstractRelNode that = (AbstractRelNode) obj;
    boolean result = this.getTraitSet().equals(that.getTraitSet())
        && this.getRowType().equalsSansFieldNames(that.getRowType());
    if (!result) {
      return false;
    }
    PairList<String, @Nullable Object> items1 = this.getDigestItems();
    PairList<String, @Nullable Object> items2 = that.getDigestItems();
    if (items1.size() != items2.size()) {
      return false;
    }
    for (int i = 0; result && i < items1.size(); i++) {
      Map.Entry<String, @Nullable Object> attr1 = items1.get(i);
      Map.Entry<String, @Nullable Object> attr2 = items2.get(i);
      if (attr1.getValue() instanceof RelNode) {
        result = ((RelNode) attr1.getValue()).deepEquals(attr2.getValue());
      } else {
        result = attr1.equals(attr2);
      }
    }
    return result;
  }

  /**
   * Compute hash code for RelNode digest.
   *
   * @see RelNode#deepEquals(Object)
   */
  @API(since = "1.25", status = API.Status.MAINTAINED)
  @Override public int deepHashCode() {
    int result = 31 + getTraitSet().hashCode();
    PairList<String, @Nullable Object> items = this.getDigestItems();
    for (@Nullable Object value : items.rightList()) {
      final int h;
      if (value == null) {
        h = 0;
      } else if (value instanceof RelNode) {
        h = ((RelNode) value).deepHashCode();
      } else {
        h = value.hashCode();
      }
      result = result * 31 + h;
    }
    return result;
  }

  private PairList<String, @Nullable Object> getDigestItems() {
    RelDigestWriter rdw = new RelDigestWriter();
    explainTerms(rdw);
    if (this instanceof Hintable) {
      List<RelHint> hints = ((Hintable) this).getHints();
      rdw.itemIf("hints", hints, !hints.isEmpty());
    }
    return rdw.attrs;
  }

  /** Implementation of {@link RelDigest}. */
  private class InnerRelDigest implements RelDigest {
    /** Cached hash code. */
    private int hash = 0;

    @Override public RelNode getRel() {
      return AbstractRelNode.this;
    }

    @Override public void clear() {
      hash = 0;
    }

    @Override public boolean equals(final @Nullable Object o) {
      if (this == o) {
        return true;
      }
      if (o == null || getClass() != o.getClass()) {
        return false;
      }
      final InnerRelDigest relDigest = (InnerRelDigest) o;
      return deepEquals(relDigest.getRel());
    }

    @Override public int hashCode() {
      if (hash == 0) {
        hash = deepHashCode();
      }
      return hash;
    }

    @Override public String toString() {
      RelDigestWriter rdw = new RelDigestWriter();
      explain(rdw);
      return requireNonNull(rdw.digest, "digest");
    }
  }

  /**
   * A writer object used exclusively for computing the digest of a RelNode.
   *
   * <p>The writer is meant to be used only for computing a single digest and
   * then thrown away.  After calling {@link #done(RelNode)} the writer should
   * be used only to obtain the computed {@link #digest}. Any other action is
   * prohibited.
   */
  private static final class RelDigestWriter implements RelWriter {
    private final PairList<String, @Nullable Object> attrs = PairList.of();

    @Nullable String digest = null;

    @Override public void explain(final RelNode rel,
        final List<Pair<String, @Nullable Object>> valueList) {
      throw new IllegalStateException("Should not be called for computing digest");
    }

    @Override public SqlExplainLevel getDetailLevel() {
      return SqlExplainLevel.DIGEST_ATTRIBUTES;
    }

    @Override public RelWriter item(String term, @Nullable Object value) {
      if (value != null && value.getClass().isArray()) {
        // We can't call hashCode and equals on Array, so
        // convert it to String to keep the same behaviour.
        value = "" + value;
      }
      attrs.add(term, value);
      return this;
    }

    @Override public RelWriter done(RelNode node) {
      StringBuilder sb = new StringBuilder();
      sb.append(node.getRelTypeName());
      sb.append('.');
      sb.append(node.getTraitSet());
      sb.append('(');
      attrs.forEachIndexed((j, left, right) -> {
        if (j > 0) {
          sb.append(',');
        }
        sb.append(left);
        sb.append('=');
        if (right instanceof RelNode) {
          RelNode input = (RelNode) right;
          sb.append(input.getRelTypeName());
          sb.append('#');
          sb.append(input.getId());
        } else {
          sb.append(right);
        }
      });
      sb.append(')');
      digest = sb.toString();
      return this;
    }
  }
}
