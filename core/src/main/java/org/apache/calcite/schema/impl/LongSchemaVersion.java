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
package org.apache.calcite.schema.impl;

import org.apache.calcite.schema.SchemaVersion;

/** Implementation of SchemaVersion that uses a long value as representation. */
public class LongSchemaVersion implements SchemaVersion {
  private final long value;

  public LongSchemaVersion(long value) {
    this.value = value;
  }

  public boolean isBefore(SchemaVersion other) {
    if (!(other instanceof LongSchemaVersion)) {
      throw new IllegalArgumentException(
          "Cannot compare a LongSchemaVersion object with a "
          + other.getClass() + " object.");
    }

    return this.value < ((LongSchemaVersion) other).value;
  }

  public boolean equals(Object obj) {
    if (this == obj) {
      return true;
    }

    if (!(obj instanceof LongSchemaVersion)) {
      return false;
    }

    return this.value == ((LongSchemaVersion) obj).value;
  }

  public int hashCode() {
    return Long.valueOf(value).hashCode();
  }

  public String toString() {
    return String.valueOf(value);
  }
}

// End LongSchemaVersion.java
