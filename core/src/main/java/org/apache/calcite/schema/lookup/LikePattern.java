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
import java.util.BitSet;
import java.util.List;

/**
 * This class is used to hold a pattern, which is typically
 * used in SQL LIKE statements.
 *
 * <p>The pattern can contain wildcards: {@code %}, which matches any sequence
 * of characters, including none; and {@code _}, which matches exactly one character.
 * A wildcard can be escaped using {@code \}. All other characters match only themselves.
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
    final List<Segment> segments = segments(likePattern);
    return v1 -> matches(segments, v1);
  }

  /** Splits a LIKE-style pattern into the segments between its '%'
   * wild-cards, resolving '\' escapes. Consecutive wild-cards act as
   * one, so only the first and last segments can be empty: the first if
   * the pattern starts with a wild-card, the last if it ends with one. */
  private static List<Segment> segments(String pattern) {
    final List<Segment> segments = new ArrayList<>();
    final StringBuilder buf = new StringBuilder();
    BitSet any = new BitSet();
    for (int i = 0; i < pattern.length(); i++) {
      final char c = pattern.charAt(i);
      if (c == '\\') {
        if (++i < pattern.length()) {
          buf.append(pattern.charAt(i));
        }
      } else if (c == '%') {
        if (buf.length() > 0 || segments.isEmpty()) {
          segments.add(new Segment(buf.toString(), any));
        }
        buf.setLength(0);
        any = new BitSet();
      } else {
        if (c == '_') {
          any.set(buf.length());
        }
        buf.append(c);
      }
    }
    segments.add(new Segment(buf.toString(), any));
    return segments;
  }

  /** Returns whether a string matches a pattern split by
   * {@link #segments}. Each middle segment is matched at its leftmost
   * position, which leaves the most room for the segments after it. */
  private static boolean matches(List<Segment> segments, String s) {
    final Segment first = segments.get(0);
    if (segments.size() == 1) {
      return s.length() == first.length() && first.matchesAt(s, 0);
    }
    final Segment last = segments.get(segments.size() - 1);
    final int end = s.length() - last.length();
    if (end < first.length() || !first.matchesAt(s, 0) || !last.matchesAt(s, end)) {
      return false;
    }
    int pos = first.length();
    for (Segment segment : segments.subList(1, segments.size() - 1)) {
      while (pos + segment.length() <= end && !segment.matchesAt(s, pos)) {
        pos++;
      }
      if (pos + segment.length() > end) {
        return false;
      }
      pos += segment.length();
    }
    return true;
  }

  /** Characters of a pattern between '%' wild-cards (or the start or end of the pattern).
   * A position in {@code any} holds an unescaped '_', which matches any one character. */
  private static class Segment {
    final String chars;
    final BitSet any;

    Segment(String chars, BitSet any) {
      this.chars = chars;
      this.any = any;
    }

    int length() {
      return chars.length();
    }

    /** Returns whether this segment matches {@code s} starting at
     * {@code pos}; the caller ensures there are enough characters. */
    boolean matchesAt(String s, int pos) {
      for (int i = 0; i < chars.length(); i++) {
        if (!any.get(i) && s.charAt(pos + i) != chars.charAt(i)) {
          return false;
        }
      }
      return true;
    }
  }

}
