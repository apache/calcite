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
package net.hydromatic.optiq.runtime;

import java.io.IOException;
import java.io.PrintWriter;
import java.io.StringWriter;
import java.io.Writer;
import java.util.Arrays;
import java.util.concurrent.locks.ReentrantLock;

/**
 * Efficiently writes strings of spaces.
 */
public class Spacer {
  private static final ReentrantLock lock = new ReentrantLock();

  /** Array of spaces at least as long as any Spacer in existence. */
  private static char[] spaces = {' '};

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
    ensureSpaces(n);
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
    return new String(spaces, 0, n);
  }

  /** Appends current number of spaces to a {@link StringBuilder}. */
  public StringBuilder spaces(StringBuilder buf) {
    buf.append(spaces, 0, n);
    return buf;
  }

  /** Appends current number of spaces to a {@link Writer}. */
  public Writer spaces(Writer buf) throws IOException {
    buf.write(spaces, 0, n);
    return buf;
  }

  /** Appends current number of spaces to a {@link StringWriter}. */
  public StringWriter spaces(StringWriter buf) {
    buf.write(spaces, 0, n);
    return buf;
  }

  /** Appends current number of spaces to a {@link PrintWriter}. */
  public PrintWriter spaces(PrintWriter buf) {
    buf.write(spaces, 0, n);
    return buf;
  }

  private static void ensureSpaces(int n) {
    lock.lock();
    try {
      if (spaces.length < n) {
        char[] newSpaces = new char[n];
        Arrays.fill(newSpaces, ' ');
        // atomic assignment; other Spacer instances may be using this
        spaces = newSpaces;
      }
    } finally {
      lock.unlock();
    }
  }

  /** Returns a string that is padded on the right with spaces to the current
   * length. */
  public String padRight(String string) {
    final int x = n - string.length();
    if (x <= 0) {
      return string;
    }
    return new StringBuilder(string).append(spaces, 0, x).toString();
  }
}

// End Spacer.java
