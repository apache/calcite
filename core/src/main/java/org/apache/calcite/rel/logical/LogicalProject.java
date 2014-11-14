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

import org.eigenbase.relopt.*;
import org.eigenbase.reltype.*;
import org.eigenbase.rex.*;

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
      List<? extends RexNode> exps,
      RelDataType rowType,
      int flags) {
    super(cluster, traitSet, child, exps, rowType, flags);
    assert traitSet.containsIfApplicable(Convention.NONE);
  }

  /**
   * Creates a ProjectRel by parsing serialized output.
   */
  public ProjectRel(RelInput input) {
    super(input);
  }

  //~ Methods ----------------------------------------------------------------

  @Override public ProjectRel copy(RelTraitSet traitSet, RelNode input,
      List<RexNode> exps, RelDataType rowType) {
    return new ProjectRel(getCluster(), traitSet, input, exps, rowType, flags);
  }

  @Override
  public RelNode accept(RelShuttle shuttle) {
    return shuttle.visit(this);
  }
}

// End ProjectRel.java
