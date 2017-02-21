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

import org.apache.calcite.rel.RelCollation;
import org.apache.calcite.rex.RexNode;

import java.util.Objects;

/** Mutable equivalent of {@link org.apache.calcite.rel.core.Sort}. */
public class MutableSort extends MutableSingleRel {
  public final RelCollation collation;
  public final RexNode offset;
  public final RexNode fetch;

  private MutableSort(MutableRel input, RelCollation collation,
      RexNode offset, RexNode fetch) {
    super(MutableRelType.SORT, input.rowType, input);
    this.collation = collation;
    this.offset = offset;
    this.fetch = fetch;
  }

  /**
   * Creates a MutableSort.
   *
   * @param input     Input relational expression
   * @param collation Array of sort specifications
   * @param offset    Expression for number of rows to discard before returning
   *                  first row
   * @param fetch     Expression for number of rows to fetch
   */
  public static MutableSort of(MutableRel input, RelCollation collation,
      RexNode offset, RexNode fetch) {
    return new MutableSort(input, collation, offset, fetch);
  }

  @Override public boolean equals(Object obj) {
    return obj == this
        || obj instanceof MutableSort
        && collation.equals(((MutableSort) obj).collation)
        && Objects.equals(offset, ((MutableSort) obj).offset)
        && Objects.equals(fetch, ((MutableSort) obj).fetch)
        && input.equals(((MutableSort) obj).input);
  }

  @Override public int hashCode() {
    return Objects.hash(input, collation, offset, fetch);
  }

  @Override public StringBuilder digest(StringBuilder buf) {
    buf.append("Sort(collation: ").append(collation);
    if (offset != null) {
      buf.append(", offset: ").append(offset);
    }
    if (fetch != null) {
      buf.append(", fetch: ").append(fetch);
    }
    return buf.append(")");
  }

  @Override public MutableRel clone() {
    return MutableSort.of(input.clone(), collation, offset, fetch);
  }
}

// End MutableSort.java
