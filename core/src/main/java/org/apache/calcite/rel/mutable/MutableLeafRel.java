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

import org.apache.calcite.rel.RelNode;

import com.google.common.collect.ImmutableList;

import java.util.List;

/** Abstract base class for implementations of {@link MutableRel} that have
 * no inputs. */
abstract class MutableLeafRel extends MutableRel {
  protected final RelNode rel;

  protected MutableLeafRel(MutableRelType type, RelNode rel) {
    super(rel.getCluster(), rel.getRowType(), type);
    this.rel = rel;
  }

  public void setInput(int ordinalInParent, MutableRel input) {
    throw new IllegalArgumentException();
  }

  public List<MutableRel> getInputs() {
    return ImmutableList.of();
  }

  public void childrenAccept(MutableRelVisitor visitor) {
    // no children - nothing to do
  }
}

// End MutableLeafRel.java
