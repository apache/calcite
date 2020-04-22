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
package org.apache.calcite.jdbc;

import org.apache.calcite.sql.SqlCollation;

import java.nio.charset.Charset;
import java.text.Collator;
import java.util.Locale;

/**
 * Collation that uses a specific {@link Collator} for comparison.
 */
public class JavaCollation extends SqlCollation {
  private final Collator collator;

  public JavaCollation(Coercibility coercibility, Locale locale, Charset charset, int strength) {
    super(coercibility, locale, charset, getStrengthString(strength));
    collator = Collator.getInstance(locale);
    collator.setStrength(strength);
  }

  // Strength values
  private static final String STRENGTH_PRIMARY = "primary";
  private static final String STRENGTH_SECONDARY = "secondary";
  private static final String STRENGTH_TERTIARY = "tertiary";
  private static final String STRENGTH_IDENTICAL = "identical";

  private static String getStrengthString(int strengthValue) {
    switch (strengthValue) {
    case Collator.PRIMARY:
      return STRENGTH_PRIMARY;
    case Collator.SECONDARY:
      return STRENGTH_SECONDARY;
    case Collator.TERTIARY:
      return STRENGTH_TERTIARY;
    case Collator.IDENTICAL:
      return STRENGTH_IDENTICAL;
    default:
      throw new IllegalArgumentException("Incorrect strength value.");
    }
  }

  @Override protected String generateCollationName(Charset charset) {
    return super.generateCollationName(charset) + "$JAVA_COLLATOR";
  }

  @Override public Collator getCollator() {
    return collator;
  }
}
