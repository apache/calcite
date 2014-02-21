/*
// Licensed to Julian Hyde under one or more contributor license
// agreements. See the NOTICE file distributed with this work for
// additional information regarding copyright ownership.
//
// Julian Hyde licenses this file to you under the Apache License,
// Version 2.0 (the "License"); you may not use this file except in
// compliance with the License. You may obtain a copy of the License at:
//
// http://www.apache.org/licenses/LICENSE-2.0
//
// Unless required by applicable law or agreed to in writing, software
// distributed under the License is distributed on an "AS IS" BASIS,
// WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
// See the License for the specific language governing permissions and
// limitations under the License.
*/
package org.eigenbase.rel;

import java.util.*;

import org.eigenbase.relopt.*;
import org.eigenbase.reltype.*;
import org.eigenbase.rex.*;
import org.eigenbase.util.*;
import org.eigenbase.util.mapping.MappingType;
import org.eigenbase.util.mapping.Mappings;

import net.hydromatic.linq4j.Ord;

/**
 * <code>ProjectRel</code> is a relational expression which computes a set of
 * 'select expressions' from its input relational expression.
 *
 * <p>The result is usually 'boxed' as a record with one named field for each
 * column; if there is precisely one expression, the result may be 'unboxed',
 * and consist of the raw value type.</p>
 */
public final class ProjectRel extends ProjectRelBase {
  //~ Constructors -----------------------------------------------------------

  /**
   * Creates a ProjectRel with no sort keys.
   *
   * @param cluster    Cluster this relational expression belongs to
   * @param child      input relational expression
   * @param exps       set of expressions for the input columns
   * @param fieldNames aliases of the expressions
   * @param flags      values as in {@link ProjectRelBase.Flags}
   */
  public ProjectRel(
      RelOptCluster cluster,
      RelNode child,
      List<RexNode> exps,
      List<String> fieldNames,
      int flags) {
    this(
        cluster,
        cluster.traitSetOf(RelCollationImpl.EMPTY),
        child,
        exps,
        RexUtil.createStructType(
            cluster.getTypeFactory(),
            exps,
            fieldNames),
        flags);
  }

  /**
   * Creates a ProjectRel.
   *
   * @param cluster  Cluster this relational expression belongs to
   * @param traitSet traits of this rel
   * @param child    input relational expression
   * @param exps     List of expressions for the input columns
   * @param rowType  output row type
   * @param flags    values as in {@link ProjectRelBase.Flags}
   */
  public ProjectRel(
      RelOptCluster cluster,
      RelTraitSet traitSet,
      RelNode child,
      List<RexNode> exps,
      RelDataType rowType,
      int flags) {
    super(cluster, traitSet, child, exps, rowType, flags);
  }

  /**
   * Creates a ProjectRel by parsing serialized output.
   */
  public ProjectRel(RelInput input) {
    super(input);
  }

  //~ Methods ----------------------------------------------------------------

  @Override
  public RelNode copy(RelTraitSet traitSet, List<RelNode> inputs) {
    assert traitSet.containsIfApplicable(Convention.NONE);
    return new ProjectRel(
        getCluster(),
        traitSet,
        sole(inputs),
        getProjects(),
        rowType,
        getFlags());
  }

  @Override
  public RelNode accept(RelShuttle shuttle) {
    return shuttle.visit(this);
  }

  /**
   * Returns a permutation, if this projection is merely a permutation of its
   * input fields, otherwise null.
   */
  public Permutation getPermutation() {
    final int fieldCount = rowType.getFieldList().size();
    if (fieldCount != getChild().getRowType().getFieldList().size()) {
      return null;
    }
    Permutation permutation = new Permutation(fieldCount);
    for (int i = 0; i < fieldCount; ++i) {
      final RexNode exp = exps.get(i);
      if (exp instanceof RexInputRef) {
        permutation.set(i, ((RexInputRef) exp).getIndex());
      } else {
        return null;
      }
    }
    return permutation;
  }

  /**
   * Checks whether this is a functional mapping.
   * Every output is a source field, but
   * a source field may appear as zero, one, or more output fields.
   */
  public boolean isMapping() {
    for (RexNode exp : exps) {
      if (!(exp instanceof RexInputRef)) {
        return false;
      }
    }
    return true;
  }

  /**
   * Returns a mapping, or null if this projection is not a mapping.
   *
   * <p>The mapping is an inverse surjection.
   * Every target has a source field, but
   * a source field may appear as zero, one, or more target fields.
   * Thus you can safely call
   * {@link org.eigenbase.util.mapping.Mappings.TargetMapping#getTarget(int)}
   */
  public Mappings.TargetMapping getMapping() {
    Mappings.TargetMapping mapping =
        Mappings.create(
            MappingType.INVERSE_SURJECTION,
            getChild().getRowType().getFieldCount(),
            exps.size());
    for (Ord<RexNode> exp : Ord.zip(exps)) {
      if (!(exp.e instanceof RexInputRef)) {
        return null;
      }
      mapping.set(((RexInputRef) exp.e).getIndex(), exp.i);
    }
    return mapping;
  }
}

// End ProjectRel.java
