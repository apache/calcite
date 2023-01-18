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
package org.apache.calcite.util.format;

import org.checkerframework.checker.nullness.qual.Nullable;

import java.util.Arrays;
import java.util.List;
import java.util.Objects;
import java.util.StringJoiner;

/**
 * Represents a format element comprised of one or more {@link FormatElementEnum} entries.
 */
public class FormatModelElementAlias implements FormatModelElement {

  private String alias;
  private String description;
  private List<FormatModelElement> formatElements;

  private FormatModelElementAlias(String alias, List<FormatModelElement> formatElements,
      String description) {
    this.alias = Objects.requireNonNull(alias, "element alias");
    this.formatElements = Objects.requireNonNull(formatElements, "format elements");
    this.description = description;
  }

  public static FormatModelElementAlias create(String alias, FormatModelElement fmtElement) {
    return new FormatModelElementAlias(alias, Arrays.asList(fmtElement),
        fmtElement.getDescription());
  }

  public static FormatModelElementAlias create(String alias, List<FormatModelElement> fmtElements,
      @Nullable String description) {
    return new FormatModelElementAlias(alias, fmtElements, description);
  }

  @Override public List<FormatModelElement> getElements() {
    return this.formatElements;
  }

  @Override public String getLiteral() {
    return this.alias;
  }

  @Override public String getToken() {
    StringJoiner buf = new StringJoiner(", ");
    getElements().forEach(ele -> buf.add(ele.getToken()));
    return buf.toString();
  }

  @Override public boolean isAlias() {
    return true;
  }

  @Override public String getDescription() {
    if (this.description == null) {
      return getElements().get(0).getDescription();
    }
    return this.description;
  }
}
