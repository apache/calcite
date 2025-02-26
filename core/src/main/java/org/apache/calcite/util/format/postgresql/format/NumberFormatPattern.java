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

import org.apache.calcite.util.format.postgresql.ChronoUnitEnum;
import org.apache.calcite.util.format.postgresql.PatternModifier;
import org.apache.calcite.util.format.postgresql.format.compiled.CompiledPattern;
import org.apache.calcite.util.format.postgresql.format.compiled.NumberCompiledPattern;

import java.time.ZonedDateTime;
import java.util.Set;
import java.util.function.Function;

/**
 * A date/time format component that will parse a sequence of digits into a value
 * (such as "DD").
 */
public class NumberFormatPattern extends FormatPattern {
  private final ChronoUnitEnum chronoUnit;
  private final Function<ZonedDateTime, Integer> dateTimeToIntFunction;
  private final Function<Integer, Integer> valueAdjuster;
  private final int minCharacters;
  private final int maxCharacters;
  private final int minValue;
  private final int maxValue;

  public NumberFormatPattern(String pattern, ChronoUnitEnum chronoUnit,
      Function<ZonedDateTime, Integer> dateTimeToIntConverter,
      Function<Integer, Integer> valueAdjuster, int minCharacters, int maxCharacters, int minValue,
      int maxValue) {
    super(pattern);
    this.chronoUnit = chronoUnit;
    this.dateTimeToIntFunction = dateTimeToIntConverter;
    this.valueAdjuster = valueAdjuster;
    this.minCharacters = minCharacters;
    this.maxCharacters = maxCharacters;
    this.minValue = minValue;
    this.maxValue = maxValue;
  }

  public NumberFormatPattern(String pattern, ChronoUnitEnum chronoUnit,
      Function<ZonedDateTime, Integer> dateTimeToIntConverter, int minCharacters,
      int maxCharacters, int minValue, int maxValue) {
    this(pattern, chronoUnit, dateTimeToIntConverter, v -> v, minCharacters, maxCharacters,
        minValue, maxValue);
  }

  @Override protected CompiledPattern buildCompiledPattern(Set<PatternModifier> modifiers) {
    return new NumberCompiledPattern(
        chronoUnit,
        dateTimeToIntFunction,
        valueAdjuster,
        minCharacters,
        maxCharacters,
        minValue,
        maxValue,
        pattern,
        modifiers);
  }
}
