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

import com.google.common.collect.ImmutableList;
import com.google.common.collect.ImmutableMap;

import java.util.Date;
import java.util.LinkedHashMap;
import java.util.List;
import java.util.Map;
import java.util.concurrent.ConcurrentHashMap;
import java.util.function.Consumer;
import java.util.regex.Matcher;
import java.util.regex.Pattern;

import static org.apache.calcite.util.format.FormatElementEnum.CC;
import static org.apache.calcite.util.format.FormatElementEnum.D;
import static org.apache.calcite.util.format.FormatElementEnum.DAY;
import static org.apache.calcite.util.format.FormatElementEnum.DD;
import static org.apache.calcite.util.format.FormatElementEnum.DDD;
import static org.apache.calcite.util.format.FormatElementEnum.DY;
import static org.apache.calcite.util.format.FormatElementEnum.E;
import static org.apache.calcite.util.format.FormatElementEnum.FF1;
import static org.apache.calcite.util.format.FormatElementEnum.FF2;
import static org.apache.calcite.util.format.FormatElementEnum.FF3;
import static org.apache.calcite.util.format.FormatElementEnum.FF4;
import static org.apache.calcite.util.format.FormatElementEnum.FF5;
import static org.apache.calcite.util.format.FormatElementEnum.FF6;
import static org.apache.calcite.util.format.FormatElementEnum.HH12;
import static org.apache.calcite.util.format.FormatElementEnum.HH24;
import static org.apache.calcite.util.format.FormatElementEnum.IW;
import static org.apache.calcite.util.format.FormatElementEnum.MI;
import static org.apache.calcite.util.format.FormatElementEnum.MM;
import static org.apache.calcite.util.format.FormatElementEnum.MON;
import static org.apache.calcite.util.format.FormatElementEnum.MONTH;
import static org.apache.calcite.util.format.FormatElementEnum.MS;
import static org.apache.calcite.util.format.FormatElementEnum.PM;
import static org.apache.calcite.util.format.FormatElementEnum.Q;
import static org.apache.calcite.util.format.FormatElementEnum.SS;
import static org.apache.calcite.util.format.FormatElementEnum.TZR;
import static org.apache.calcite.util.format.FormatElementEnum.W;
import static org.apache.calcite.util.format.FormatElementEnum.WW;
import static org.apache.calcite.util.format.FormatElementEnum.YY;
import static org.apache.calcite.util.format.FormatElementEnum.YYYY;

import static java.util.Objects.requireNonNull;

/**
 * Utilities for {@link FormatModel}.
 */
public class FormatModels {
  private FormatModels() {
  }

  /** The format model consisting of built-in format elements.
   *
   * <p>Due to the design of {@link FormatElementEnum}, it is similar to
   * Oracle's format model.
   */
  public static final FormatModel DEFAULT;

  /** Format model for BigQuery.
   *
   * <p>BigQuery format element reference:
   * <a href="https://cloud.google.com/bigquery/docs/reference/standard-sql/format-elements">
   * BigQuery Standard SQL Format Elements</a>.
   */
  public static final FormatModel BIG_QUERY;

  /** Format model for PostgreSQL.
   *
   * <p>PostgreSQL format element reference:
   * <a href="https://www.postgresql.org/docs/current/functions-formatting.html">
   * PostgreSQL Standard SQL Format Elements</a>.
   */
  public static final FormatModel POSTGRESQL;

  static {
    final Map<String, FormatElement> map = new LinkedHashMap<>();
    for (FormatElementEnum fe : FormatElementEnum.values()) {
      map.put(fe.toString(), fe);
    }
    DEFAULT = create(map);

    map.clear();
    map.put("%A", DAY);
    map.put("%a", DY);
    map.put("%B", MONTH);
    map.put("%b", MON);
    map.put("%c",
        compositeElement("The date and time representation (English);",
            DY, literalElement(" "), MON, literalElement(" "),
            DD, literalElement(" "), HH24, literalElement(":"),
            MI, literalElement(":"), SS, literalElement(" "),
            YYYY));
    map.put("%d", DD);
    map.put("%E1S", FF1);
    map.put("%E2S", FF2);
    map.put("%E3S", FF3);
    map.put("%E4S", FF4);
    map.put("%E5S", FF5);
    map.put("%E*S", FF6);
    map.put("%e", E);
    map.put("%F",
        compositeElement("The date in the format %Y-%m-%d.", YYYY, literalElement("-"), MM,
            literalElement("-"), DD));
    map.put("%H", HH24);
    map.put("%I", HH12);
    map.put("%j", DDD);
    map.put("%M", MI);
    map.put("%m", MM);
    map.put("%p", PM);
    map.put("%Q", Q);
    map.put("%R",
        compositeElement("The time in the format %H:%M",
            HH24, literalElement(":"), MI));
    map.put("%S", SS);
    map.put("%T",
        compositeElement("The time in the format %H:%M:%S.",
            HH24, literalElement(":"), MI, literalElement(":"), SS));
    map.put("%u", D);
    map.put("%V", IW);
    map.put("%W", WW);
    map.put("%x",
        compositeElement("The date representation in MM/DD/YY format",
            MM, literalElement("/"), DD, literalElement("/"), YY));
    map.put("%Y", YYYY);
    map.put("%y", YY);
    map.put("%Z", TZR);

    map.put("HH12", HH12);
    map.put("HH24", HH24);
    map.put("MI", MI);
    map.put("SS", SS);
    map.put("MS", MS);
    map.put("FF1", FF1);
    map.put("FF2", FF2);
    map.put("FF3", FF3);
    map.put("FF4", FF4);
    map.put("FF5", FF5);
    map.put("FF6", FF6);
    map.put("YYYY", YYYY);
    map.put("YY", YY);
    map.put("Day", DAY);
    map.put("DAY", DAY);
    map.put("DY", DY);
    map.put("Month", MONTH);
    map.put("MONTH", MONTH);
    map.put("Mon", MON);
    map.put("MON", MON);
    map.put("MM", MM);
    map.put("CC", CC);
    map.put("DDD", DDD);
    map.put("DD", DD);
    map.put("D", D);
    map.put("WW", WW);
    map.put("W", W);
    map.put("IW", IW);
    map.put("Q", Q);
    // Our implementation of TO_CHAR does not support TIMESTAMPTZ
    // As PostgreSQL, we will skip the timezone when formatting TIMESTAMP values
    map.put("TZ", TZR);

    BIG_QUERY = create(map);
    POSTGRESQL = create(map);
  }

  /**
   * Generates a {@link Pattern} using the keys of a {@link FormatModel} element
   * map. This pattern is used in {@link FormatModel#parse(String)} to help
   * locate known format elements in a string.
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

  /**
   * Creates a {@link FormatModel} that uses the provided map to identify
   * {@link FormatElement}s while parsing a format string.
   */
  public static FormatModel create(Map<String, FormatElement> elementMap) {
    final Pattern pattern = regexFromMap(elementMap);
    return new FormatModelImpl(pattern, elementMap);
  }

  /**
   * Creates a literal format element.
   */
  public static FormatElement literalElement(String literal) {
    return new FormatModelElementLiteral(literal);
  }

  /**
   * Creates a composite format element from the provided list of elements
   * and description.
   */
  public static FormatElement compositeElement(String description,
      FormatElement... fmtElements) {
    return new CompositeFormatElement(ImmutableList.copyOf(fmtElements),
        description);
  }


  /** Implementation of {@link FormatModel} based on a list of format
   * elements. */
  private static class FormatModelImpl implements FormatModel {
    final Pattern pattern;
    final Map<String, FormatElement> elementMap;

    /** Cache of parsed format strings.
     *
     * <p>NOTE: The current implementation could grow without bounds.
     * A per-thread cache would be better, or a cache tied to a statement
     * execution (e.g. in DataContext). Also limit to a say 100 entries. */
    final Map<String, List<FormatElement>> memoizedElements =
        new ConcurrentHashMap<>();

    FormatModelImpl(Pattern pattern, Map<String, FormatElement> elementMap) {
      this.pattern = requireNonNull(pattern, "pattern");
      this.elementMap = ImmutableMap.copyOf(elementMap);
    }

    @Override public Map<String, FormatElement> getElementMap() {
      return elementMap;
    }

    @Override public List<FormatElement> parseNoCache(String format) {
      final ImmutableList.Builder<FormatElement> elements =
          ImmutableList.builder();
      final Matcher matcher = pattern.matcher(format);
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
      return elements.build();
    }

    @Override public List<FormatElement> parse(String format) {
      return memoizedElements.computeIfAbsent(format, this::parseNoCache);
    }
  }

  /**
   * A format element that is literal text.
   */
  private static class FormatModelElementLiteral implements FormatElement {
    private final String literal;

    FormatModelElementLiteral(String literal) {
      this.literal = requireNonNull(literal, "literal");
    }

    @Override public void format(StringBuilder sb, Date date) {
      sb.append(literal);
    }

    @Override public void toPattern(StringBuilder sb) {
      sb.append(literal);
    }

    @Override public String getDescription() {
      return "Represents literal text in a format string";
    }

    @Override public String toString() {
      return this.literal;
    }
  }

  /**
   * A format element comprised of one or more {@link FormatElement} entries.
   */
  private static class CompositeFormatElement implements FormatElement {
    private final String description;
    private final List<FormatElement> formatElements;

    CompositeFormatElement(List<FormatElement> formatElements,
        String description) {
      this.formatElements = ImmutableList.copyOf(formatElements);
      this.description = requireNonNull(description, "description");
    }

    @Override public void format(StringBuilder sb, Date date) {
      flatten(ele -> ele.format(sb, date));
    }

    @Override public void toPattern(StringBuilder sb) {
      flatten(ele -> ele.toPattern(sb));
    }

    /**
     * Applies a consumer to each format element that make up the composite
     * element.
     *
     * <p>For example, {@code %R} in Google SQL represents the hour in 24-hour
     * format (e.g., 00..23) followed by the minute as a decimal number.
     * {@code flatten(i -> println(i.toString())); } would print "HH24:MI".
     */
    @Override public void flatten(Consumer<FormatElement> consumer) {
      formatElements.forEach(consumer);
    }

    @Override public String getDescription() {
      return this.description;
    }
  }
}
