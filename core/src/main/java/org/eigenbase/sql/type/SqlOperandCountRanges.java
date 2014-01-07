/*
// Licensed to Julian Hyde under one or more contributor license
// agreements. See the NOTICE file distributed with this work for
// additional information regarding copyright ownership.
//
// Julian Hyde licenses this file to you under the Apache License,
// Version 2.0 (the "License"); you may not use this file except in
// compliance with the License. You may obtain a copy of the License at:
//
// http://www.apache.org/licenses/LICENSE-2.0
//
// Unless required by applicable law or agreed to in writing, software
// distributed under the License is distributed on an "AS IS" BASIS,
// WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
// See the License for the specific language governing permissions and
// limitations under the License.
*/
package org.eigenbase.sql.type;

import org.eigenbase.sql.SqlOperandCountRange;

/**
 * Helpers for {@link SqlOperandCountRange}.
 */
public abstract class SqlOperandCountRanges {
  public static SqlOperandCountRange of(int length) {
    return new RangeImpl(length, length);
  }

  public static SqlOperandCountRange between(int min, int max) {
    assert min < max;
    return new RangeImpl(min, max);
  }

  public static SqlOperandCountRange any() {
    return new VariadicImpl();
  }

  private static class VariadicImpl implements SqlOperandCountRange {
    public boolean isValidCount(int count) {
      return true;
    }

    public int getMin() {
      return 0;
    }

    public int getMax() {
      return -1;
    }
  }

  private static class RangeImpl implements SqlOperandCountRange {
    private final int min;
    private final int max;

    public RangeImpl(int min, int max) {
      this.min = min;
      this.max = max;
    }

    public boolean isValidCount(int count) {
      return count >= min && count <= max;
    }

    public int getMin() {
      return min;
    }

    public int getMax() {
      return max;
    }
  }
}

// End SqlOperandCountRanges.java
