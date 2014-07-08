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
package org.apache.optiq.rel;

import java.util.BitSet;
import java.util.List;

import org.apache.optiq.rel.metadata.RelMetadataQuery;
import org.apache.optiq.relopt.RelOptCluster;
import org.apache.optiq.relopt.RelTraitSet;

/**
 * Abstract base class for implementations of
 * {@link IntersectRel}.
 */
public abstract class IntersectRelBase extends SetOpRel {
  /**
   * Creates an IntersectRelBase.
   */
  public IntersectRelBase(
      RelOptCluster cluster,
      RelTraitSet traits,
      List<RelNode> inputs,
      boolean all) {
    super(cluster, traits, inputs, all);
  }

  /**
   * Creates an IntersectRelBase by parsing serialized output.
   */
  protected IntersectRelBase(RelInput input) {
    super(input);
  }

  @Override
  public double getRows() {
    // REVIEW jvs 30-May-2005:  I just pulled this out of a hat.
    double dRows = Double.MAX_VALUE;
    for (RelNode input : inputs) {
      dRows = Math.min(
          dRows, RelMetadataQuery.getRowCount(input));
    }
    dRows *= 0.25;
    return dRows;
  }

  @Override
  public boolean isKey(BitSet columns) {
    for (RelNode input : inputs) {
      if (input.isKey(columns)) {
        return true;
      }
    }
    return super.isKey(columns);
  }
}

// End IntersectRelBase.java
