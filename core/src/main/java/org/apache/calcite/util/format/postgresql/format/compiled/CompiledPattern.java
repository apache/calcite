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
package org.apache.calcite.util.format.postgresql.format.compiled;

import org.apache.calcite.util.format.postgresql.ChronoUnitEnum;
import org.apache.calcite.util.format.postgresql.PatternModifier;

import java.text.ParseException;
import java.text.ParsePosition;
import java.util.Locale;
import java.util.Set;

/**
 * A single date/time format compiled component such as "YYYY" or "MI". A compiled component
 * is a format component along with any flags it had. Adds the ability to parse a string.
 */
public abstract class CompiledPattern implements CompiledItem {
  private final ChronoUnitEnum chronoUnit;
  protected final Set<PatternModifier> modifiers;

  protected CompiledPattern(ChronoUnitEnum chronoUnit, Set<PatternModifier> modifiers) {
    this.chronoUnit = chronoUnit;
    this.modifiers = modifiers;
  }

  /**
   * Get the ChronoUnitEnum value that this pattern is for.
   *
   * @return a ChronoUnitEnum value
   */
  public ChronoUnitEnum getChronoUnit() {
    return chronoUnit;
  }

  /**
   * Parse this date/time component from a String.
   *
   * @param inputPosition starting position for parsing
   * @param input full string that will be parsed
   * @param enforceLength whether to limit the length of characters read. Needed when one
   *                      sequence of digits is followed by another (such as YYYYDD).
   * @param locale Locale to use for parsing day and month names if the TM flag was present
   * @return the integer value of the parsed date/time component
   * @throws ParseException if unable to parse a value from input
   */
  public abstract int parseValue(ParsePosition inputPosition, String input, boolean enforceLength,
      Locale locale) throws ParseException;

  @Override public int getFormatPatternLength() {
    return 2 * modifiers.size() + getBaseFormatPatternLength();
  }

  /**
   * Returns the length of the format pattern without modifiers.
   *
   * @return length of the format pattern
   */
  protected abstract int getBaseFormatPatternLength();

  /**
   * Does this pattern match a sequence of digits.
   *
   * @return true if this pattern matches a sequence of digits
   */
  public boolean isNumeric() {
    return false;
  }
}
