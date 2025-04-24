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
package org.apache.calcite.schema.lookup;

import org.apache.calcite.linq4j.function.Predicate1;

import java.util.regex.Pattern;

/**
 * This class is used to hold a pattern, which is typically
 * used in SQL LIKE statements.
 *
 * <p>The pattern can contain wildcards (`%`) or character ranges (`[a-z]`).
 *
 * <p>The pattern can also be translated to a {@code Predicate1<String>} which
 * can be used to do filtering inside java.
 */
public class LikePattern {
  private static final String ANY = "%";
  public final String pattern;

  public LikePattern(String pattern) {
    if (pattern == null) {
      pattern = ANY;
    }
    this.pattern = pattern;
  }

  @Override public String toString() {
    return "LikePattern[" + this.pattern + "]";
  }

  public Predicate1<String> matcher() {
    return matcher(pattern);
  }

  public static LikePattern any() {
    return new LikePattern(ANY);
  }

  public static Predicate1<String> matcher(String likePattern) {
    if (likePattern == null || likePattern.equals(ANY)) {
      return v1 -> true;
    }
    final Pattern regex = likeToRegex(likePattern);
    return v1 -> regex.matcher(v1).matches();
  }

  /**
   * Converts a LIKE-style pattern (where '%' represents a wild-card, escaped
   * using '\') to a Java regex. It's always case sensitive.
   */
  public static Pattern likeToRegex(String pattern) {
    StringBuilder buf = new StringBuilder("^");
    char[] charArray = pattern.toCharArray();
    int slash = -2;
    for (int i = 0; i < charArray.length; i++) {
      char c = charArray[i];
      if (slash == i - 1) {
        buf.append('[').append(c).append(']');
      } else {
        switch (c) {
        case '\\':
          slash = i;
          break;
        case '%':
          buf.append(".*");
          break;
        case '[':
          buf.append("\\[");
          break;
        case ']':
          buf.append("\\]");
          break;
        default:
          buf.append('[').append(c).append(']');
        }
      }
    }
    buf.append("$");
    return Pattern.compile(buf.toString());
  }

}
