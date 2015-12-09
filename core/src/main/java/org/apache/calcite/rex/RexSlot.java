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

import java.util.AbstractList;
import java.util.concurrent.CopyOnWriteArrayList;

/**
 * Abstract base class for {@link RexInputRef} and {@link RexLocalRef}.
 */
public abstract class RexSlot extends RexVariable {
  //~ Instance fields --------------------------------------------------------

  protected final int index;

  //~ Constructors -----------------------------------------------------------

  /**
   * Creates a slot.
   *
   * @param index Index of the field in the underlying rowtype
   * @param type  Type of the column
   */
  protected RexSlot(
      String name,
      int index,
      RelDataType type) {
    super(name, type);
    assert index >= 0;
    this.index = index;
  }

  //~ Methods ----------------------------------------------------------------

  public int getIndex() {
    return index;
  }

  /**
   * Thread-safe list that populates itself if you make a reference beyond
   * the end of the list. Useful if you are using the same entries repeatedly.
   * Once populated, accesses are very efficient.
   */
  protected static class SelfPopulatingList
      extends CopyOnWriteArrayList<String> {
    private final String prefix;

    SelfPopulatingList(final String prefix, final int initialSize) {
      super(fromTo(prefix, 0, initialSize));
      this.prefix = prefix;
    }

    private static AbstractList<String> fromTo(
        final String prefix,
        final int start,
        final int end) {
      return new AbstractList<String>() {
        public String get(int index) {
          return prefix + (index + start);
        }

        public int size() {
          return end - start;
        }
      };
    }

    @Override public String get(int index) {
      for (;;) {
        try {
          return super.get(index);
        } catch (IndexOutOfBoundsException e) {
          if (index < 0) {
            throw new IllegalArgumentException();
          }
          // Double-checked locking, but safe because CopyOnWriteArrayList.array
          // is marked volatile, and size() uses array.length.
          synchronized (this) {
            final int size = size();
            if (index >= size) {
              addAll(fromTo(prefix, size, Math.max(index + 1, size * 2)));
            }
          }
        }
      }
    }
  }
}

// End RexSlot.java
