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
import org.apache.calcite.util.format.postgresql.format.compiled.TimeZoneCompiledPattern;

import java.util.Set;

/**
 * The date/time format component for the 3 letter timezone code (such as UTC).
 * This is only supported when converting a date/time value to a string.
 */
public class TimeZoneFormatPattern extends FormatPattern {
  private final boolean upperCase;

  public TimeZoneFormatPattern(String pattern) {
    super(pattern);
    upperCase = "TZ".equals(pattern);
  }

  @Override protected CompiledPattern buildCompiledPattern(Set<PatternModifier> modifiers) {
    return new TimeZoneCompiledPattern(modifiers, upperCase);
  }
}
