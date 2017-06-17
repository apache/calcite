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

import org.apache.calcite.plan.RelOptCluster;
import org.apache.calcite.rel.type.RelDataType;

import com.google.common.collect.ImmutableList;

import java.util.List;

/** Mutable equivalent of {@link org.apache.calcite.rel.BiRel}. */
abstract class MutableBiRel extends MutableRel {
  protected MutableRel left;
  protected MutableRel right;

  protected MutableBiRel(MutableRelType type, RelOptCluster cluster,
      RelDataType rowType, MutableRel left, MutableRel right) {
    super(cluster, rowType, type);
    this.left = left;
    left.parent = this;
    left.ordinalInParent = 0;

    this.right = right;
    right.parent = this;
    right.ordinalInParent = 1;
  }

  public void setInput(int ordinalInParent, MutableRel input) {
    if (ordinalInParent > 1) {
      throw new IllegalArgumentException();
    }
    if (ordinalInParent == 0) {
      this.left = input;
    } else {
      this.right = input;
    }
    if (input != null) {
      input.parent = this;
      input.ordinalInParent = ordinalInParent;
    }
  }

  public List<MutableRel> getInputs() {
    return ImmutableList.of(left, right);
  }

  public MutableRel getLeft() {
    return left;
  }

  public MutableRel getRight() {
    return right;
  }

  public void childrenAccept(MutableRelVisitor visitor) {

    visitor.visit(left);
    visitor.visit(right);
  }
}

// End MutableBiRel.java
