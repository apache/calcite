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

import org.apache.calcite.util.format.postgresql.CapitalizationEnum;
import org.apache.calcite.util.format.postgresql.PatternModifier;
import org.apache.calcite.util.format.postgresql.format.compiled.CompiledPattern;
import org.apache.calcite.util.format.postgresql.format.compiled.DayOfWeekCompiledPattern;

import java.time.format.TextStyle;
import java.util.Set;

/**
 * The date/time format component for a text representation of a day of the week.
 */
public class DayOfWeekFormatPattern extends FormatPattern {
  private final CapitalizationEnum capitalization;
  private final TextStyle textStyle;

  public DayOfWeekFormatPattern(String pattern) {
    super(pattern);
    switch (pattern) {
    case "DAY":
      capitalization = CapitalizationEnum.ALL_UPPER;
      textStyle = TextStyle.FULL;
      break;
    case "Day":
      capitalization = CapitalizationEnum.CAPITALIZED;
      textStyle = TextStyle.FULL;
      break;
    case "day":
      capitalization = CapitalizationEnum.ALL_LOWER;
      textStyle = TextStyle.FULL;
      break;
    case "DY":
      capitalization = CapitalizationEnum.ALL_UPPER;
      textStyle = TextStyle.SHORT;
      break;
    case "Dy":
      capitalization = CapitalizationEnum.CAPITALIZED;
      textStyle = TextStyle.SHORT;
      break;
    default:
      capitalization = CapitalizationEnum.ALL_LOWER;
      textStyle = TextStyle.SHORT;
      break;
    }
  }

  @Override protected CompiledPattern buildCompiledPattern(Set<PatternModifier> modifiers) {
    return new DayOfWeekCompiledPattern(modifiers, capitalization, textStyle);
  }
}
