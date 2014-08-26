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
package net.hydromatic.optiq.runtime;

/**
 * Efficiently writes strings of spaces.
 */
public class Spacer {
  private int n;

  /** Creates a Spacer with zero spaces. */
  public Spacer() {
    this(0);
  }

  /** Creates a Spacer with a given number of spaces. */
  public Spacer(int n) {
    set(n);
  }

  /** Sets the current number of spaces. */
  public Spacer set(int n) {
    this.n = n;
    return this;
  }

  /** Returns the current number of spaces. */
  public int get() {
    return n;
  }

  /** Increases the current number of spaces by {@code n}. */
  public Spacer add(int n) {
    return set(this.n + n);
  }

  /** Reduces the current number of spaces by {@code n}. */
  public Spacer subtract(int n) {
    return set(this.n - n);
  }

  /** Returns a string of the current number of spaces. */
  public String toString() {
    return Spaces.of(n);
  }

  /** Appends current number of spaces to a {@link StringBuilder}. */
  public StringBuilder spaces(StringBuilder buf) {
    return Spaces.append(buf, n);
  }

  /** Returns a string that is padded on the right with spaces to the current
   * length. */
  public String padRight(String string) {
    Spaces.padRight(string, n);
    final int x = n - string.length();
    if (x <= 0) {
      return string;
    }
    // Replacing StringBuffer with String would hurt performance.
    //noinspection StringBufferReplaceableByString
    return Spaces.append(new StringBuilder(string), x).toString();
  }
}

// End Spacer.java
