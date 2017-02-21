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

import org.apache.calcite.rel.type.RelDataType;

import com.google.common.collect.ImmutableList;

import java.util.List;

/** Mutable equivalent of {@link org.apache.calcite.rel.SingleRel}. */
abstract class MutableSingleRel extends MutableRel {
  protected MutableRel input;

  protected MutableSingleRel(MutableRelType type,
      RelDataType rowType, MutableRel input) {
    super(input.cluster, rowType, type);
    this.input = input;
    input.parent = this;
    input.ordinalInParent = 0;
  }

  public void setInput(int ordinalInParent, MutableRel input) {
    if (ordinalInParent > 0) {
      throw new IllegalArgumentException();
    }
    this.input = input;
    if (input != null) {
      input.parent = this;
      input.ordinalInParent = 0;
    }
  }

  public List<MutableRel> getInputs() {
    return ImmutableList.of(input);
  }

  public void childrenAccept(MutableRelVisitor visitor) {
    visitor.visit(input);
  }

  public MutableRel getInput() {
    return input;
  }
}

// End MutableSingleRel.java
