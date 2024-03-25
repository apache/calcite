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
package org.apache.calcite.access;

import org.apache.commons.lang.StringUtils;

import org.checkerframework.checker.nullness.qual.Nullable;

import java.security.Principal;
import java.util.Locale;

import static org.apache.calcite.runtime.Utilities.hash;

import static java.util.Objects.requireNonNull;

/**
 * CalcitePrincipal is a simple implementation of {@link Principal}
 * that represents a user. CalcitePrincipal ignore the case of the
 * name when comparing two principals.
 */
public class CalcitePrincipal implements Principal, Comparable<CalcitePrincipal> {

  private final String name;

  public CalcitePrincipal(String name) {
    this.name = requireNonNull(name, "name").toUpperCase(Locale.ROOT);
  }

  @Override public String getName() {
    return this.name;
  }

  @Override public String toString() {
    return this.name;
  }

  @Override public boolean equals(@Nullable Object o) {
    if (this == o) {
      return true;
    } else if (o != null && this.getClass() == o.getClass()) {
      CalcitePrincipal that = (CalcitePrincipal) o;
      return StringUtils.equalsIgnoreCase(this.name, that.name);
    } else {
      return false;
    }
  }

  @Override public int hashCode() {
    return hash(this.name);
  }

  @Override public int compareTo(CalcitePrincipal o) {
    return String.CASE_INSENSITIVE_ORDER.compare(this.name, o.name);
  }
}
