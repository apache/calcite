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
package org.apache.calcite.util.format;

import com.google.common.collect.ImmutableMap;

import java.util.List;
import java.util.Stack;

/**
 * Utility class used to convert format strings into {@link FormatModelElement}s.
 */
public class FormatModelUtil {

  private FormatModelUtil() {}

  /**
   * Parses the {@code fmtString} using element identifiers supplied by {@code fmtModel}.
   *
   * TODO(CALCITE-2980): make this configurable for multiple parse maps. Currently this only works
   * for BigQuery and MySQL style format strings where elements begin with '%'
   */
  public static List<FormatModelElement> parse(String fmtString,
      ImmutableMap<String, FormatModelElement> fmtModel) {
    char ch;
    int idx = 0;
    StringBuilder buf = new StringBuilder();
    Stack elements = new Stack();
    while (idx < fmtString.length()) {
      switch (ch = fmtString.charAt(idx)) {
      case '%':
        String key = buf.append(fmtString, idx, idx + 2).toString();
        FormatModelElement element = fmtModel.get(key);
        if (element == null) {
          elements.push(new FormatModelElementLiteral(buf.toString()));
        } else {
          elements.push(element);
        }
        buf.setLength(0);
        idx += 2;
        break;
      default:
        buf.append(ch);
        // consume literal elements up to end of string or next '%'
        while (idx < fmtString.length() - 1) {
          ch = fmtString.charAt(++idx);
          if (ch == '%') {
            --idx;
            break;
          }
          buf.append(ch);
        }
        elements.push(new FormatModelElementLiteral(buf.toString()));
        buf.setLength(0);
        ++idx;
      }
    }
    return elements;
  }
}
