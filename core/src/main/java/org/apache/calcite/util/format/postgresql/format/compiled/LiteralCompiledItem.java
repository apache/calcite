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

import java.time.ZonedDateTime;
import java.util.Locale;

/**
 * A literal string in a date/time format. When converting a date/time to a string,
 * the literal value is output exactly. When parsing a string into a date/time value,
 * just need to consume a string of equal length to the literal (can be a different
 * string).
 */
public class LiteralCompiledItem implements CompiledItem {
  private final String literalValue;

  public LiteralCompiledItem(String literalValue) {
    this.literalValue = literalValue;
  }

  @Override public String convertToString(final ZonedDateTime dateTime, final Locale locale) {
    return literalValue;
  }

  @Override public int getFormatPatternLength() {
    return literalValue.length();
  }
}
