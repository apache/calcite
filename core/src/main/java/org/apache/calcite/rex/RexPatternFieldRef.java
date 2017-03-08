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
package org.apache.calcite.rex;

import org.apache.calcite.rel.type.RelDataType;
import org.apache.calcite.sql.SqlKind;

/**
 * Variable which references a field of an input relational expression
 */
public class RexPatternFieldRef extends RexInputRef {
  private final String alpha;

  public RexPatternFieldRef(String alpha, int index, RelDataType type) {
    super(index, type);
    this.alpha = alpha;
    digest = alpha + ".$" + index;
  }

  public String getAlpha() {
    return alpha;
  }

  public static RexPatternFieldRef of(String alpha, int index, RelDataType type) {
    return new RexPatternFieldRef(alpha, index, type);
  }

  public static RexPatternFieldRef of(String alpha, RexInputRef ref) {
    return new RexPatternFieldRef(alpha, ref.getIndex(), ref.getType());
  }

  @Override public <R> R accept(RexVisitor<R> visitor) {
    return visitor.visitPatternFieldRef(this);
  }

  @Override public <R, P> R accept(RexBiVisitor<R, P> visitor, P arg) {
    return visitor.visitPatternFieldRef(this, arg);
  }

  @Override public SqlKind getKind() {
    return SqlKind.PATTERN_INPUT_REF;
  }
}

// End RexPatternFieldRef.java
