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

import java.util.ArrayList;
import java.util.List;

/**
 * This class is used to hold a pattern, which is typically
 * used in SQL LIKE statements.
 *
 * <p>The pattern can contain wildcards ({@code %}), each of which matches
 * any sequence of characters, including none. A wildcard can be escaped
 * using {@code \}. All other characters, including {@code _}, {@code [}
 * and {@code ]}, match only themselves.
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
    final List<String> segments = segments(likePattern);
    return v1 -> matches(segments, v1);
  }

  /** Splits a LIKE-style pattern into the literal segments between its
   * '%' wild-cards, resolving '\' escapes. A pattern with n wild-cards has
   * n + 1 segments, some of which may be empty. */
  private static List<String> segments(String pattern) {
    final List<String> segments = new ArrayList<>();
    final StringBuilder buf = new StringBuilder();
    for (int i = 0; i < pattern.length(); i++) {
      final char c = pattern.charAt(i);
      if (c == '\\') {
        if (++i < pattern.length()) {
          buf.append(pattern.charAt(i));
        }
      } else if (c == '%') {
        segments.add(buf.toString());
        buf.setLength(0);
      } else {
        buf.append(c);
      }
    }
    segments.add(buf.toString());
    return segments;
  }

  /** Returns whether a string matches a pattern split by
   * {@link #segments}. Each middle segment is matched at its leftmost
   * occurrence, which leaves the most room for the segments after it. */
  private static boolean matches(List<String> segments, String s) {
    final String first = segments.get(0);
    if (segments.size() == 1) {
      return s.equals(first);
    }
    final String last = segments.get(segments.size() - 1);
    if (s.length() < first.length() + last.length()
        || !s.startsWith(first) || !s.endsWith(last)) {
      return false;
    }
    final int end = s.length() - last.length();
    int pos = first.length();
    for (String segment : segments.subList(1, segments.size() - 1)) {
      final int i = s.indexOf(segment, pos);
      if (i < 0 || i + segment.length() > end) {
        return false;
      }
      pos = i + segment.length();
    }
    return true;
  }

}
