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
 * <code>ValuesRel</code> represents a sequence of zero or more literal row
 * values.
 */
public class ValuesRel extends ValuesRelBase {
  //~ Constructors -----------------------------------------------------------

  /**
   * Creates a new ValuesRel. Note that tuples passed in become owned by this
   * rel (without a deep copy), so caller must not modify them after this
   * call, otherwise bad things will happen.
   *
   * @param cluster .
   * @param rowType row type for tuples produced by this rel
   * @param tuples  2-dimensional array of tuple values to be produced; outer
   *                list contains tuples; each inner list is one tuple; all
   *                tuples must be of same length, conforming to rowType
   */
  public ValuesRel(
      RelOptCluster cluster,
      RelDataType rowType,
      List<List<RexLiteral>> tuples) {
    super(
        cluster,
        rowType,
        tuples,
        cluster.traitSetOf(Convention.NONE));
  }

  /**
   * Creates a ValuesRel by parsing serialized output.
   */
  public ValuesRel(RelInput input) {
    super(input);
  }

  public RelNode copy(RelTraitSet traitSet, List<RelNode> inputs) {
    assert traitSet.containsIfApplicable(Convention.NONE);
    assert inputs.isEmpty();
    return new ValuesRel(
        getCluster(),
        rowType,
        tuples);
  }

  @Override
  public RelNode accept(RelShuttle shuttle) {
    return shuttle.visit(this);
  }
}

// End ValuesRel.java
