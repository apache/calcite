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
package org.apache.calcite.rel.rules;

/**
 * A conflict rule (CR) is a pair of table sets denoted by T1 → T2, which means that if T1
 * intersects with the table under the join operator, T2 must be included in this join. With every
 * hyperedge in hypergraph, we associate a set of conflict rules.
 *
 * @see HyperEdge
 * @see ConflictDetectionHelper
 */
public class ConflictRule {

  // 'from' and 'to' are bitmaps where index refer to inputs of hypergraph
  final long from;

  final long to;

  public ConflictRule(long from, long to) {
    this.from = from;
    this.to = to;
  }

  @Override public String toString() {
    return "Table" + LongBitmap.printBitmap(from) + " -> Table"
        + LongBitmap.printBitmap(to);
  }

  public ConflictRule shift(int offset) {
    return new ConflictRule(from << offset, to << offset);
  }
}
