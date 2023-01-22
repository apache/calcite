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

import java.util.ArrayList;
import java.util.List;
import java.util.regex.Matcher;
import java.util.regex.Pattern;

/**
 * Utility class used to convert format strings into {@link FormatModelElement}s.
 */
public class FormatModelUtil {

  private FormatModelUtil() {}

  /**
   * Parses the {@code fmtString} using element identifiers supplied by {@code fmtModel}.
   */
  public static List<FormatModelElement> parse(String format,
      ImmutableMap<String, FormatModelElement> fmtModelMap) {
    List<FormatModelElement> elements = new ArrayList<>();
    // TODO(CALCITE-2980): make these regex patterns static and tied to a library or dialect.
    StringBuilder regex = new StringBuilder();
    for (String key : fmtModelMap.keySet()) {
      regex.append("(").append(Pattern.quote(key)).append(")|");
    }
    // remove the last '|'
    regex.setLength(regex.length() - 1);
    Matcher matcher = Pattern.compile(regex.toString()).matcher(format);
    int i = 0;
    while (matcher.find()) {
      // Add any leading literal text before next element match
      String literal = format.substring(i, matcher.start());
      if (!literal.isEmpty()) {
        elements.add(new FormatModelElementLiteral(literal));
      }
      // add the element match
      String key = matcher.group();
      elements.add(fmtModelMap.get(key));
      i = matcher.end();
    }
    // add any remaining literal text after last element match
    String literal = format.substring(i);
    if (!literal.isEmpty()) {
      elements.add(new FormatModelElementLiteral(literal));
    }
    return elements;
  }
}
