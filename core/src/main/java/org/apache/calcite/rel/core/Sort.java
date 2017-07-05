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
package org.apache.calcite.rel.core;

import org.apache.calcite.linq4j.Ord;
import org.apache.calcite.plan.RelOptCluster;
import org.apache.calcite.plan.RelOptCost;
import org.apache.calcite.plan.RelOptPlanner;
import org.apache.calcite.plan.RelTraitSet;
import org.apache.calcite.rel.RelCollation;
import org.apache.calcite.rel.RelCollationTraitDef;
import org.apache.calcite.rel.RelFieldCollation;
import org.apache.calcite.rel.RelInput;
import org.apache.calcite.rel.RelNode;
import org.apache.calcite.rel.RelWriter;
import org.apache.calcite.rel.SingleRel;
import org.apache.calcite.rel.metadata.RelMetadataQuery;
import org.apache.calcite.rex.RexNode;
import org.apache.calcite.rex.RexShuttle;
import org.apache.calcite.util.Util;

import com.google.common.collect.ImmutableList;

import java.util.Collections;
import java.util.List;

/**
 * Relational expression that imposes a particular sort order on its input
 * without otherwise changing its content.
 */
public abstract class Sort extends SingleRel {
  //~ Instance fields --------------------------------------------------------

  public final RelCollation collation;
  protected final ImmutableList<RexNode> fieldExps;
  public final RexNode offset;
  public final RexNode fetch;

  //~ Constructors -----------------------------------------------------------

  /**
   * Creates a Sort.
   *
   * @param cluster   Cluster this relational expression belongs to
   * @param traits    Traits
   * @param child     input relational expression
   * @param collation array of sort specifications
   */
  public Sort(
      RelOptCluster cluster,
      RelTraitSet traits,
      RelNode child,
      RelCollation collation) {
    this(cluster, traits, child, collation, null, null);
  }

  /**
   * Creates a Sort.
   *
   * @param cluster   Cluster this relational expression belongs to
   * @param traits    Traits
   * @param child     input relational expression
   * @param collation array of sort specifications
   * @param offset    Expression for number of rows to discard before returning
   *                  first row
   * @param fetch     Expression for number of rows to fetch
   */
  public Sort(
      RelOptCluster cluster,
      RelTraitSet traits,
      RelNode child,
      RelCollation collation,
      RexNode offset,
      RexNode fetch) {
    super(cluster, traits, child);
    this.collation = collation;
    this.offset = offset;
    this.fetch = fetch;

    assert traits.containsIfApplicable(collation)
        : "traits=" + traits + ", collation=" + collation;
    assert !(fetch == null
        && offset == null
        && collation.getFieldCollations().isEmpty())
        : "trivial sort";
    ImmutableList.Builder<RexNode> builder = ImmutableList.builder();
    for (RelFieldCollation field : collation.getFieldCollations()) {
      int index = field.getFieldIndex();
      builder.add(cluster.getRexBuilder().makeInputRef(child, index));
    }
    fieldExps = builder.build();
  }

  /**
   * Creates a Sort by parsing serialized output.
   */
  public Sort(RelInput input) {
    this(input.getCluster(), input.getTraitSet().plus(input.getCollation()),
        input.getInput(),
        RelCollationTraitDef.INSTANCE.canonize(input.getCollation()),
        input.getExpression("offset"), input.getExpression("fetch"));
  }

  //~ Methods ----------------------------------------------------------------

  @Override public final Sort copy(RelTraitSet traitSet, List<RelNode> inputs) {
    return copy(traitSet, sole(inputs), collation, offset, fetch);
  }

  public final Sort copy(RelTraitSet traitSet, RelNode newInput,
      RelCollation newCollation) {
    return copy(traitSet, newInput, newCollation, offset, fetch);
  }

  public abstract Sort copy(RelTraitSet traitSet, RelNode newInput,
      RelCollation newCollation, RexNode offset, RexNode fetch);

  @Override public RelOptCost computeSelfCost(RelOptPlanner planner,
      RelMetadataQuery mq) {
    // Higher cost if rows are wider discourages pushing a project through a
    // sort.
    final double rowCount = mq.getRowCount(this);
    final double bytesPerRow = getRowType().getFieldCount() * 4;
    final double cpu = Util.nLogN(rowCount) * bytesPerRow;
    return planner.getCostFactory().makeCost(rowCount, cpu, 0);
  }

  @Override public List<RexNode> getChildExps() {
    return fieldExps;
  }

  public RelNode accept(RexShuttle shuttle) {
    RexNode offset = shuttle.apply(this.offset);
    RexNode fetch = shuttle.apply(this.fetch);
    List<RexNode> fieldExps = shuttle.apply(this.fieldExps);
    assert fieldExps == this.fieldExps
        : "Sort node does not support modification of input field expressions."
          + " Old expressions: " + this.fieldExps + ", new ones: " + fieldExps;
    if (offset == this.offset
        && fetch == this.fetch) {
      return this;
    }
    return copy(traitSet, getInput(), collation, offset, fetch);
  }

  /**
   * Returns the array of {@link RelFieldCollation}s asked for by the sort
   * specification, from most significant to least significant.
   *
   * <p>See also {@link RelMetadataQuery#collations(RelNode)},
   * which lists all known collations. For example,
   * <code>ORDER BY time_id</code> might also be sorted by
   * <code>the_year, the_month</code> because of a known monotonicity
   * constraint among the columns. {@code getCollation} would return
   * <code>[time_id]</code> and {@code collations} would return
   * <code>[ [time_id], [the_year, the_month] ]</code>.</p>
   */
  public RelCollation getCollation() {
    return collation;
  }

  @SuppressWarnings("deprecation")
  @Override public List<RelCollation> getCollationList() {
    return Collections.singletonList(getCollation());
  }

  public RelWriter explainTerms(RelWriter pw) {
    super.explainTerms(pw);
    assert fieldExps.size() == collation.getFieldCollations().size();
    if (pw.nest()) {
      pw.item("collation", collation);
    } else {
      for (Ord<RexNode> ord : Ord.zip(fieldExps)) {
        pw.item("sort" + ord.i, ord.e);
      }
      for (Ord<RelFieldCollation> ord
          : Ord.zip(collation.getFieldCollations())) {
        pw.item("dir" + ord.i, ord.e.shortString());
      }
    }
    pw.itemIf("offset", offset, offset != null);
    pw.itemIf("fetch", fetch, fetch != null);
    return pw;
  }
}

// End Sort.java
