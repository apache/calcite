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

import org.apache.calcite.rel.core.Values;

/** Mutable equivalent of {@link org.apache.calcite.rel.core.Values}. */
public class MutableValues extends MutableLeafRel {
  private MutableValues(Values rel) {
    super(MutableRelType.VALUES, rel);
  }

  /**
   * Creates a MutableValue.
   *
   * @param values  The underlying Values object
   */
  public static MutableValues of(Values values) {
    return new MutableValues(values);
  }

  @Override public boolean equals(Object obj) {
    return obj == this
        || obj instanceof MutableValues
        && rel == ((MutableValues) obj).rel;
  }

  @Override public int hashCode() {
    return rel.hashCode();
  }

  @Override public StringBuilder digest(StringBuilder buf) {
    return buf.append("Values(tuples: ")
        .append(((Values) rel).getTuples()).append(")");
  }

  @Override public MutableRel clone() {
    return MutableValues.of((Values) rel);
  }
}

// End MutableValues.java
