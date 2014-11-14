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
package org.eigenbase.relopt;

import java.util.BitSet;
import java.util.List;

import org.eigenbase.rex.RexCall;
import org.eigenbase.rex.RexInputRef;
import org.eigenbase.rex.RexLiteral;
import org.eigenbase.rex.RexNode;

/** Utilities for strong predicates.
 *
 * <p>A predicate is strong (or null-rejecting) if it is UNKNOWN if any of its
 * inputs is UNKNOWN.</p>
 *
 * <p>By the way, UNKNOWN is just the boolean form of NULL.</p>
 *
 * <p>Examples:</p>
 * <ul>
 *   <li>{@code UNKNOWN} is strong
 *   <li>{@code c = 1} is strong
 *   <li>{@code c IS NULL} is not strong. (It always returns TRUE or FALSE.)
 *   <li>{@code p1 AND p2} is strong if p1 or p2 are strong
 *   <li>{@code p1 OR p2} is strong if p1 and p2 are strong
 *   <li>{@code c1 = 1 OR c2 IS NULL} is strong on c1 but not c2
 * </ul>
 */
public class Strong {
  private final BitSet nullColumns;

  private Strong(BitSet nullColumns) {
    this.nullColumns = nullColumns;
  }

  public static Strong of(BitSet nullColumns) {
    return new Strong(nullColumns);
  }

  /** Returns whether the analyzed expression will return null if a given set
   * of input columns are null. */
  public static boolean is(RexNode node, BitSet nullColumns) {
    return of(nullColumns).strong(node);
  }

  private boolean strong(RexNode node) {
    switch (node.getKind()) {
    case LITERAL:
      return ((RexLiteral) node).getValue() == null;
    case IS_TRUE:
    case IS_NOT_NULL:
    case AND:
    case EQUALS:
    case NOT_EQUALS:
    case LESS_THAN:
    case LESS_THAN_OR_EQUAL:
    case GREATER_THAN:
    case GREATER_THAN_OR_EQUAL:
      return anyStrong(((RexCall) node).getOperands());
    case OR:
      return allStrong(((RexCall) node).getOperands());
    case INPUT_REF:
      return nullColumns.get(((RexInputRef) node).getIndex());
    default:
      return false;
    }
  }

  private boolean allStrong(List<RexNode> operands) {
    for (RexNode operand : operands) {
      if (!strong(operand)) {
        return false;
      }
    }
    return true;
  }

  private boolean anyStrong(List<RexNode> operands) {
    for (RexNode operand : operands) {
      if (strong(operand)) {
        return true;
      }
    }
    return false;
  }
}

// End Strong.java
