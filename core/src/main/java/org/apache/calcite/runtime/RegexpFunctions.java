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
package org.apache.calcite.runtime;

import java.util.regex.Matcher;
import java.util.regex.Pattern;

import static org.apache.calcite.util.Static.RESOURCE;

/**
 * A collection of functions used in REGEXP processing.
 */
public class RegexpFunctions {
  private RegexpFunctions() {
  }

  /** SQL {@code REGEXP_REPLACE} function. */
  public static String regexpReplace(String s, String regex, String replacement) {
    return regexpReplace(s, regex, replacement, 1, 0, null);
  }

  /** SQL {@code REGEXP_REPLACE} function. */
  public static String regexpReplace(String s, String regex, String replacement,
      Integer pos) {
    return regexpReplace(s, regex, replacement, pos, 0, null);
  }

  /** SQL {@code REGEXP_REPLACE} function. */
  public static String regexpReplace(String s, String regex, String replacement,
      Integer pos, Integer occurrence) {
    return regexpReplace(s, regex, replacement, pos, occurrence, null);
  }

  /** SQL {@code REGEXP_REPLACE} function. */
  public static String regexpReplace(String s, String regex, String replacement,
      Integer pos, Integer occurrence, String matchType) {
    if (pos < 1 || pos > s.length()) {
      throw RESOURCE.invalidInputForRegexpReplace(pos.toString()).ex();
    }

    // TODO: Matcher#appendReplacement(StringBuilder sb, String replacement)
    // method is supported in JDK 1.9, and is not supported in JDK 1.8.
    StringBuffer sb = new StringBuffer();
    String input = s;
    if (pos != 1) {
      sb.append(s.substring(0, pos - 1));
      input = s.substring(pos - 1);
    }

    int count = 0;
    int flags = makeRegexpFlags(matchType);
    Matcher matcher = Pattern.compile(regex, flags).matcher(input);
    while (matcher.find()) {
      if (occurrence == 0) {
        matcher.appendReplacement(sb, replacement);
      } else if (++count == occurrence) {
        matcher.appendReplacement(sb, replacement);
        break;
      }
    }
    matcher.appendTail(sb);

    return sb.toString();
  }

  private static int makeRegexpFlags(String stringFlags) {
    int flags = 0;
    if (stringFlags != null) {
      for (int i = 0; i < stringFlags.length(); ++i) {
        switch (stringFlags.charAt(i)) {
        case 'i':
          flags |= Pattern.CASE_INSENSITIVE;
          break;
        case 'c':
          flags &= ~Pattern.CASE_INSENSITIVE;
          break;
        case 'n':
          flags |= Pattern.DOTALL;
          break;
        case 'm':
          flags |= Pattern.MULTILINE;
          break;
        default:
          throw RESOURCE.invalidInputForRegexpReplace(stringFlags).ex();
        }
      }
    }
    return flags;
  }
}

// End RegexpFunctions.java
