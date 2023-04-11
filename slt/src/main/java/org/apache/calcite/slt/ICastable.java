/*
 * Copyright 2022 VMware, Inc.
 * SPDX-License-Identifier: MIT
 *
 * Permission is hereby granted, free of charge, to any person obtaining a copy
 * of this software and associated documentation files (the "Software"), to deal
 * in the Software without restriction, including without limitation the rights
 * to use, copy, modify, merge, publish, distribute, sublicense, and/or sell
 * copies of the Software, and to permit persons to whom the Software is
 * furnished to do so, subject to the following conditions:
 *
 * The above copyright notice and this permission notice shall be included in all
 * copies or substantial portions of the Software.
 *
 * THE SOFTWARE IS PROVIDED "AS IS", WITHOUT WARRANTY OF ANY KIND, EXPRESS OR
 * IMPLIED, INCLUDING BUT NOT LIMITED TO THE WARRANTIES OF MERCHANTABILITY,
 * FITNESS FOR A PARTICULAR PURPOSE AND NONINFRINGEMENT. IN NO EVENT SHALL THE
 * AUTHORS OR COPYRIGHT HOLDERS BE LIABLE FOR ANY CLAIM, DAMAGES OR OTHER
 * LIABILITY, WHETHER IN AN ACTION OF CONTRACT, TORT OR OTHERWISE, ARISING FROM,
 * OUT OF OR IN CONNECTION WITH THE SOFTWARE OR THE USE OR OTHER DEALINGS IN THE
 * SOFTWARE.
 *
 *
 */

package org.apache.calcite.slt;

import javax.annotation.Nullable;

/**
 * Utility interface providing some useful casting methods.
 */
public interface ICastable {
  @Nullable
  default <T> T as(Class<T> clazz) {
    return ICastable.as(this, clazz);
  }

  @Nullable
  static <T> T as(Object obj, Class<T> clazz) {
    try {
      return clazz.cast(obj);
    } catch (ClassCastException e) {
      return null;
    }
  }

  default void error(String message) {
    System.err.println(message);
  }

  default <T> T as(Class<T> clazz, @Nullable String failureMessage) {
    T result = this.as(clazz);
    if (result == null) {
      if (failureMessage == null)
        failureMessage = this + "(" + this.getClass().getName() + ") is not an instance of " + clazz;
      this.error(failureMessage);
    }
    assert result != null;
    return result;
  }

  default <T> T to(Class<T> clazz) {
    return this.as(clazz, (String) null);
  }

  default <T> boolean is(Class<T> clazz) {
    return this.as(clazz) != null;
  }
}
