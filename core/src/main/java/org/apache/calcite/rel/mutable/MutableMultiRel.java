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
package org.apache.calcite.rel.mutable;

import org.apache.calcite.linq4j.Ord;
import org.apache.calcite.plan.RelOptCluster;
import org.apache.calcite.rel.type.RelDataType;

import com.google.common.collect.Lists;

import java.util.ArrayList;
import java.util.List;

/** Base Class for relations with three or more inputs */
abstract class MutableMultiRel extends MutableRel {
  protected final List<MutableRel> inputs;

  protected MutableMultiRel(RelOptCluster cluster,
      RelDataType rowType, MutableRelType type, List<MutableRel> inputs) {
    super(cluster, rowType, type);
    this.inputs = new ArrayList<>(inputs);
    for (Ord<MutableRel> input : Ord.zip(inputs)) {
      input.e.parent = this;
      input.e.ordinalInParent = input.i;
    }
  }

  @Override public void setInput(int ordinalInParent, MutableRel input) {
    inputs.set(ordinalInParent, input);
    if (input != null) {
      input.parent = this;
      input.ordinalInParent = ordinalInParent;
    }
  }

  @Override public List<MutableRel> getInputs() {
    return inputs;
  }

  @Override public void childrenAccept(MutableRelVisitor visitor) {
    for (MutableRel input : inputs) {
      visitor.visit(input);
    }
  }

  protected List<MutableRel> cloneChildren() {
    return Lists.transform(inputs, MutableRel::clone);
  }
}

// End MutableMultiRel.java
