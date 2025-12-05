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
import org.apache.calcite.util.format.postgresql.format.compiled.BcAdCompiledPattern;
import org.apache.calcite.util.format.postgresql.format.compiled.CompiledPattern;

import java.util.Set;

/**
 * The date/time format component for BC/AD (era, ie. BCE/CE).
 */
public class BcAdFormatPattern extends FormatPattern {
  private final boolean upperCase;
  private final boolean includePeriods;

  public BcAdFormatPattern(String pattern) {
    super(pattern);
    switch (pattern) {
    case "bc":
    case "ad":
      this.upperCase = false;
      this.includePeriods = false;
      break;
    case "BC":
    case "AD":
      this.upperCase = true;
      this.includePeriods = false;
      break;
    case "b.c.":
    case "a.d.":
      this.upperCase = false;
      this.includePeriods = true;
      break;
    default:
      this.upperCase = true;
      this.includePeriods = true;
      break;
    }
  }

  @Override protected CompiledPattern buildCompiledPattern(Set<PatternModifier> modifiers) {
    return new BcAdCompiledPattern(modifiers, upperCase, includePeriods);
  }
}
