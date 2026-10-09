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
package org.apache.calcite.util;

import java.util.Comparator;

/** Compares constants using SQL semantics. */
public final class SqlConstantComparator implements Comparator<Comparable> {
  /** Singleton instance. */
  public static final SqlConstantComparator INSTANCE = new SqlConstantComparator();

  private SqlConstantComparator() {
  }

  @SuppressWarnings({"rawtypes", "unchecked"})
  @Override public int compare(Comparable v0, Comparable v1) {
    if (hasCustomSqlConstantComparison(v0)
        && v0.getClass() == v1.getClass()) {
      return ((ComparableSqlConstant) v0).compareSqlConstantTo(v1);
    }
    return v0.compareTo(v1);
  }

  /** Returns whether a constant should use custom SQL comparison. */
  public boolean hasCustomSqlConstantComparison(Comparable v) {
    return v instanceof ComparableSqlConstant;
  }
}
