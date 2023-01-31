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

import org.apache.calcite.util.format.FormatElementEnum.ParseMap;

import java.util.ArrayList;
import java.util.Date;
import java.util.List;
import java.util.Map;
import java.util.Objects;
import java.util.concurrent.ConcurrentHashMap;
import java.util.function.Consumer;
import java.util.regex.Matcher;
import java.util.regex.Pattern;

/**
 * Utility class used to convert format strings into {@link FormatElement}s.
 */
public class FormatModels {
  private final Pattern fmtRegex;
  private final Map<String, FormatElement> elementMap;
  private final Map<String, List<FormatElement>> memoizedElements = new ConcurrentHashMap<>();

  private static final FormatModels DEFAULT = FormatModels.create(ParseMap.getMap());

  private FormatModels(Pattern parseMapRegex, Map<String, FormatElement> elementMap) {
    this.fmtRegex = parseMapRegex;
    this.elementMap = elementMap;
  }

  /**
   * Generates a {@link Pattern} using the keys of a {@link FormatModels} element map. This pattern
   * is used in {@link #parse(String)} to help locate known format elements in a string.
   */
  private static Pattern regexFromMap(Map<String, FormatElement> elementMap) {
    StringBuilder regex = new StringBuilder();
    for (String key : elementMap.keySet()) {
      regex.append("(").append(Pattern.quote(key)).append(")|");
    }
    // remove the last '|'
    regex.setLength(regex.length() - 1);
    return Pattern.compile(regex.toString());
  }

  private FormatElement internalLiteralElement(String literal) {
    return new FormatModelElementLiteral(literal);
  }

  private FormatElement internalCompositeElement(List<FormatElement> fmtElements,
      String description) {
    return new CompositeFormatElement(fmtElements, description);
  }

  /**
   * Creates a {@link FormatModels} that uses the provided map to identify {@link FormatElement}s
   * while parsing a format string.
   */
  public static FormatModels create(Map<String, FormatElement> elementMap) {
    final Pattern regex = regexFromMap(elementMap);
    return new FormatModels(regex, elementMap);
  }

  /**
   * Creates a {@link  FormatModelElementLiteral} from the provided string.
   */
  public static FormatElement literalElement(String literal) {
    return DEFAULT.internalLiteralElement(literal);
  }

  /**
   * Creates a {@link  CompositeFormatElement} from the provided list of {@link FormatElement}s and
   * description.
   */
  public static FormatElement compositeElement(List<FormatElement> fmtElements,
      String description) {
    return DEFAULT.internalCompositeElement(fmtElements, description);
  }

  /**
   * Returns the keys of a {@link FormatModels} parse map as a {@link Pattern}.
   */
  public Pattern getElementRegex() {
    return this.fmtRegex;
  }

  /**
   * Returns the map used to create the {@link FormatModels} instance.
   */
  public Map<String, FormatElement> getElementMap() {
    return this.elementMap;
  }

  private List<FormatElement> internalParse(String format) {
    List<FormatElement> elements = new ArrayList<>();
    Matcher matcher = getElementRegex().matcher(format);
    int i = 0;
    String literal;
    while (matcher.find()) {
      // Add any leading literal text before next element match
      literal = format.substring(i, matcher.start());
      if (!literal.isEmpty()) {
        elements.add(literalElement(literal));
      }
      // add the element match - use literal as default to be safe.
      String key = matcher.group();
      elements.add(getElementMap().getOrDefault(key, literalElement(key)));
      i = matcher.end();
    }
    // add any remaining literal text after last element match
    literal = format.substring(i);
    if (!literal.isEmpty()) {
      elements.add(literalElement(literal));
    }
    return elements;
  }

  /**
   * Parses the {@code fmtString} using element identifiers supplied by {@code fmtModel}.
   */
  public List<FormatElement> parse(String format) {
    return memoizedElements.computeIfAbsent(format, f -> internalParse(f));
  }

  /**
   * Represents literal text in a format string.
   */
  private static class FormatModelElementLiteral implements FormatElement {

    private final String literal;

    private FormatModelElementLiteral(String literal) {
      this.literal = Objects.requireNonNull(literal, "null literal");
    }

    @Override public String format(Date date) {
      return toString();
    }

    @Override public String getDescription() {
      return "Represents literal text in a format string";
    }

    @Override public String toString() {
      return this.literal;
    }
  }

  /**
   * Represents a format element comprised of one or more {@link FormatElementEnum} entries.
   */
  private static class CompositeFormatElement implements FormatElement {

    private final String description;
    private final List<FormatElement> formatElements;

    private CompositeFormatElement(List<FormatElement> formatElements, String description) {
      this.formatElements = Objects.requireNonNull(formatElements, "format elements");
      this.description = Objects.requireNonNull(description, "no description provided");
    }

    @Override public String format(Date date) {
      StringBuilder buf = new StringBuilder();
      flatten(ele -> buf.append(ele.format(date)));
      return buf.toString();
    }

    /**
     * Applies a consumer to each format element that make up the composite element.
     *
     * <p>For example, {@code %R} in Google SQL represents the hour in 24-hour format (e.g.,
     * 00..23) followed by the minute as a decimal number.
     * {@code flatten(i -> println(i.toString())); } would print "HH24:MI"
     */
    @Override public void flatten(Consumer<FormatElement> consumer) {
      formatElements.forEach(consumer);
    }

    @Override public String getDescription() {
      return this.description;
    }
  }
}
