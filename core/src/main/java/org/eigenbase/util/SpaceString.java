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
package org.eigenbase.util;

/** A string of spaces. */
public class SpaceString implements CharSequence {
  /** The longest possible string of spaces. Fine as long as you don't try
   * to print it.
   *
   * <p>Use with {@link StringBuilder#append(CharSequence, int, int)} to
   * append spaces without doing memory allocation.</p>
   */
  public static final CharSequence MAX = new SpaceString(Integer.MAX_VALUE);

  private final int length;

  public SpaceString(int length) {
    this.length = length;
  }

  // Do not override equals and hashCode to be like String. CharSequence does
  // not require it.

  @Override public String toString() {
    return Util.spaces(length);
  }

  public int length() {
    return length;
  }

  public char charAt(int index) {
    return ' ';
  }

  public CharSequence subSequence(int start, int end) {
    return new SpaceString(start - end);
  }
}

// End SpaceString.java
