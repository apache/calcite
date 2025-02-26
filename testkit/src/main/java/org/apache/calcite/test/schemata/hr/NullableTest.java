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
package org.apache.calcite.test.schemata.hr;

import org.checkerframework.checker.nullness.qual.Nullable;

import java.util.Objects;

/**
 * A table model that contains nullable columns.
 */
public class NullableTest {
  public final @Nullable Integer col1;
  public final @Nullable Integer col2;
  public final int col3;

  public NullableTest(@Nullable Integer col1, @Nullable Integer col2,
      int col3) {
    this.col1 = col1;
    this.col2 = col2;
    this.col3 = col3;
  }

  @Override public String toString() {
    return "DependentNullable [col1: " + col1 + ", col2: " + col2 + ", col3: " + col3 + "]";
  }

  @Override public boolean equals(Object obj) {
    return obj == this
        || obj instanceof NullableTest
        && Objects.equals(col1, ((NullableTest) obj).col1)
        && Objects.equals(col2, ((NullableTest) obj).col2)
        && col3 == ((NullableTest) obj).col3;
  }

  @Override public int hashCode() {
    return Objects.hash(col1, col2, col3);
  }
}
