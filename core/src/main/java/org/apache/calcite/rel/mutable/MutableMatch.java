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
import org.apache.calcite.rel.type.RelDataType;
import org.apache.calcite.rex.RexNode;
import org.apache.calcite.util.ImmutableBitSet;

import java.util.Map;
import java.util.Objects;
import java.util.SortedSet;

/** Mutable equivalent of {@link org.apache.calcite.rel.core.Match}. */
public class MutableMatch extends MutableSingleRel {
  public final RexNode pattern;
  public final boolean strictStart;
  public final boolean strictEnd;
  public final Map<String, RexNode> patternDefinitions;
  public final Map<String, RexNode> measures;
  public final RexNode after;
  public final Map<String, ? extends SortedSet<String>> subsets;
  public final boolean allRows;
  public final ImmutableBitSet partitionKeys;
  public final RelCollation orderKeys;
  public final RexNode interval;

  private MutableMatch(RelDataType rowType, MutableRel input,
       RexNode pattern, boolean strictStart, boolean strictEnd,
       Map<String, RexNode> patternDefinitions, Map<String, RexNode> measures,
       RexNode after, Map<String, ? extends SortedSet<String>> subsets,
       boolean allRows, ImmutableBitSet partitionKeys, RelCollation orderKeys,
       RexNode interval) {
    super(MutableRelType.MATCH, rowType, input);
    this.pattern = pattern;
    this.strictStart = strictStart;
    this.strictEnd = strictEnd;
    this.patternDefinitions = patternDefinitions;
    this.measures = measures;
    this.after = after;
    this.subsets = subsets;
    this.allRows = allRows;
    this.partitionKeys = partitionKeys;
    this.orderKeys = orderKeys;
    this.interval = interval;
  }

  /**
   * Creates a MutableMatch.
   *
   */
  public static MutableMatch of(RelDataType rowType,
      MutableRel input, RexNode pattern, boolean strictStart, boolean strictEnd,
      Map<String, RexNode> patternDefinitions, Map<String, RexNode> measures,
      RexNode after, Map<String, ? extends SortedSet<String>> subsets,
      boolean allRows, ImmutableBitSet partitionKeys, RelCollation orderKeys,
      RexNode interval) {
    return new MutableMatch(rowType, input, pattern, strictStart, strictEnd,
        patternDefinitions, measures, after, subsets, allRows, partitionKeys,
        orderKeys, interval);
  }

  @Override public boolean equals(Object obj) {
    return obj == this
        || obj instanceof MutableMatch
        && pattern.equals(((MutableMatch) obj).pattern)
        && strictStart == (((MutableMatch) obj).strictStart)
        && strictEnd == (((MutableMatch) obj).strictEnd)
        && allRows == (((MutableMatch) obj).allRows)
        && patternDefinitions.equals(((MutableMatch) obj).patternDefinitions)
        && measures.equals(((MutableMatch) obj).measures)
        && after.equals(((MutableMatch) obj).after)
        && subsets.equals(((MutableMatch) obj).subsets)
        && partitionKeys.equals(((MutableMatch) obj).partitionKeys)
        && orderKeys.equals(((MutableMatch) obj).orderKeys)
        && interval.equals(((MutableMatch) obj).interval)
        && input.equals(((MutableMatch) obj).input);
  }

  @Override public int hashCode() {
    return Objects.hash(input, pattern, strictStart, strictEnd,
        patternDefinitions, measures, after, subsets, allRows,
        partitionKeys, orderKeys, interval);
  }

  @Override public StringBuilder digest(StringBuilder buf) {
    return buf.append("Match(pattern: ").append(pattern)
        .append(", strictStart: ").append(strictStart)
        .append(", strictEnd: ").append(strictEnd)
        .append(", patternDefinitions: ").append(patternDefinitions)
        .append(", measures: ").append(measures)
        .append(", after: ").append(after)
        .append(", subsets: ").append(subsets)
        .append(", allRows: ").append(allRows)
        .append(", partitionKeys: ").append(partitionKeys)
        .append(", orderKeys: ").append(orderKeys)
        .append(", interval: ").append(interval)
        .append(")");
  }

  @Override public MutableRel clone() {
    return MutableMatch.of(rowType, input.clone(), pattern, strictStart,
        strictEnd, patternDefinitions, measures, after, subsets, allRows,
        partitionKeys, orderKeys, interval);
  }
}

// End MutableMatch.java
