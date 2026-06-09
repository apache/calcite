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

import org.apache.calcite.util.TimestampWithTimeZoneString;

/** Compares Rex values using SQL value semantics. */
final class RexSqlValueComparison {
  private RexSqlValueComparison() {
  }

  @SuppressWarnings({"rawtypes", "unchecked"})
  static int compare(Comparable v0, Comparable v1) {
    if (v0 instanceof TimestampWithTimeZoneString
        && v1 instanceof TimestampWithTimeZoneString) {
      return ((TimestampWithTimeZoneString) v0)
          .compareToInstant((TimestampWithTimeZoneString) v1);
    }
    return v0.compareTo(v1);
  }

  static boolean usesSpecialComparison(Comparable v0, Comparable v1) {
    return usesSpecialComparison(v0)
        && usesSpecialComparison(v1);
  }

  static boolean usesSpecialComparison(Comparable v) {
    return v instanceof TimestampWithTimeZoneString;
  }
}
