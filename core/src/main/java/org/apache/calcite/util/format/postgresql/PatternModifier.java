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
package org.apache.calcite.util.format.postgresql;

/**
 * Flags that can be added to a date/time format component.
 */
public enum PatternModifier {
  FM(true, "FM"),
  TH_UPPER(false, "TH"),
  TH_LOWER(false, "th"),
  TM(true, "TM");

  private final boolean prefix;
  private final String modifierString;

  PatternModifier(boolean prefix, String modifierString) {
    this.prefix = prefix;
    this.modifierString = modifierString;
  }

  /**
   * Is this modifier placed before or after the pattern.
   *
   * @return true if this modifier is placed before the pattern
   */
  public boolean isPrefix() {
    return prefix;
  }

  /**
   * Get the text for this modifier.
   *
   * @return text for this modifier
   */
  public String getModifierString() {
    return modifierString;
  }
}
