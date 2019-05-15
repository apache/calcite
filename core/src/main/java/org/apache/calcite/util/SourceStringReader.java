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
package org.apache.calcite.util;

import java.io.StringReader;
import java.util.Objects;
import javax.annotation.Nonnull;

/**
 * Extension to {@link StringReader} that allows the original string to be
 * recovered.
 */
public class SourceStringReader extends StringReader {
  private final String s;

  /**
   * Creates a source string reader.
   *
   * @param s String providing the character stream
   */
  public SourceStringReader(@Nonnull String s) {
    super(Objects.requireNonNull(s));
    this.s = s;
  }

  /** Returns the source string. */
  public @Nonnull String getSourceString() {
    return s;
  }
}

// End SourceStringReader.java
