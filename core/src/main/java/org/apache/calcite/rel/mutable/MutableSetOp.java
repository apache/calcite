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

import java.util.List;
import java.util.Objects;

/** Mutable equivalent of {@link org.apache.calcite.rel.core.SetOp}. */
abstract class MutableSetOp extends MutableMultiRel {
  protected final boolean all;

  protected MutableSetOp(RelOptCluster cluster, RelDataType rowType,
      MutableRelType type, List<MutableRel> inputs, boolean all) {
    super(cluster, rowType, type, inputs);
    this.all = all;
  }

  public boolean isAll() {
    return all;
  }

  @Override public boolean equals(Object obj) {
    return obj == this
        || obj instanceof MutableSetOp
        && type == ((MutableSetOp) obj).type
        && all == ((MutableSetOp) obj).all
        && inputs.equals(((MutableSetOp) obj).getInputs());
  }

  @Override public int hashCode() {
    return Objects.hash(type, inputs, all);
  }
}

// End MutableSetOp.java
