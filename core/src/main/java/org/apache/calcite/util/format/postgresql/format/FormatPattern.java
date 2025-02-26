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
package org.apache.calcite.util.format.postgresql.format;

import org.apache.calcite.util.format.postgresql.PatternModifier;
import org.apache.calcite.util.format.postgresql.format.compiled.CompiledPattern;

import com.google.common.collect.ImmutableSet;

import java.text.ParseException;
import java.text.ParsePosition;
import java.util.Set;

/**
 * A single component of a date/time format pattern such as YYYY or MI. Each
 * component may have some flags applied. If the flags are not used by a component,
 * then they are ignored.
 */
public abstract class FormatPattern {
  protected final String pattern;

  /**
   * Base class constructor that stores the pattern.
   *
   * @param pattern the date/time component taht this FormatPattern represents
   */
  protected FormatPattern(String pattern) {
    this.pattern = pattern;
  }

  /**
   * Creates the compiled version of the parsed date/time component along with
   * any flags that it had. It is expected that the formatString has the date/time
   * component (with its flags) at the position indicated in formatParsePosition.
   *
   * @param formatString the full date/time format string
   * @param formatParsePosition starting position in formatString with this
   *                            pattern is located
   * @return the compiled version of the parsed date/time component
   * @throws ParseException If the date/time format component was not found
   */
  public CompiledPattern compilePattern(String formatString,
      ParsePosition formatParsePosition) throws ParseException {
    String formatTrimmed = formatString.substring(formatParsePosition.getIndex());
    int charsConsumed = 0;
    final ImmutableSet.Builder<PatternModifier> modifiers = ImmutableSet.builder();

    // Find the prefix modifiers
    boolean modifierFound = true;
    while (modifierFound) {
      modifierFound = false;

      for (PatternModifier modifier : PatternModifier.values()) {
        if (modifier.isPrefix() && formatTrimmed.startsWith(modifier.getModifierString())) {
          modifiers.add(modifier);
          formatTrimmed = formatTrimmed.substring(modifier.getModifierString().length());
          charsConsumed += modifier.getModifierString().length();
          modifierFound = true;
          break;
        }
      }
    }

    if (formatTrimmed.startsWith(pattern)) {
      charsConsumed += pattern.length();
      formatTrimmed = formatString.substring(formatParsePosition.getIndex() + charsConsumed);

      // Find the suffix modifiers
      modifierFound = true;
      while (modifierFound) {
        modifierFound = false;

        for (PatternModifier modifier : PatternModifier.values()) {
          if (!modifier.isPrefix() && formatTrimmed.startsWith(modifier.getModifierString())) {
            modifiers.add(modifier);
            formatTrimmed = formatTrimmed.substring(modifier.getModifierString().length());
            modifierFound = true;
            break;
          }
        }
      }

      return buildCompiledPattern(modifiers.build());
    }

    throw new ParseException("Pattern not found", formatParsePosition.getIndex());
  }

  /**
   * Creates a new instance of the compiled version of this date/time component.
   *
   * @param modifiers the set of flags that were parsed
   * @return a new instance of the compiled version of this date/time component
   */
  protected abstract CompiledPattern buildCompiledPattern(Set<PatternModifier> modifiers);

  public String getPattern() {
    return pattern;
  }
}
