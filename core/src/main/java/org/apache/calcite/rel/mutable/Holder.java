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

/** Implementation of {@link MutableRel} whose only purpose is to have a
 * child. Used as the root of a tree. */
public class Holder extends MutableSingleRel {
  private Holder(RelDataType rowType, MutableRel input) {
    super(MutableRelType.HOLDER, rowType, input);
  }

  /**
   * Creates a Holder.
   *
   * @param input Input relational expression
   */
  public static Holder of(MutableRel input) {
    return new Holder(input.rowType, input);
  }

  @Override public StringBuilder digest(StringBuilder buf) {
    return buf.append("Holder");
  }

  @Override public MutableRel clone() {
    return Holder.of(input.clone());
  }
}

// End Holder.java
