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
package org.eigenbase.rel;

/**
 * Describes the necessary parameters for an implementation in order to
 * identify and set dynamic variables
 */
public class Correlation implements Cloneable, Comparable<Correlation> {
  private final int id;
  private final int offset;

  /**
   * Creates a correlation.
   *
   * @param id     Identifier
   * @param offset Offset
   */
  public Correlation(int id, int offset) {
    this.id = id;
    this.offset = offset;
  }

  /**
   * Returns the identifier.
   *
   * @return identifier
   */
  public int getId() {
    return id;
  }

  /**
   * Returns this correlation's offset.
   *
   * @return offset
   */
  public int getOffset() {
    return offset;
  }

  public String toString() {
    return "var" + id + "=offset" + offset;
  }

  public int compareTo(Correlation other) {
    return id - other.id;
  }

  @Override public int hashCode() {
    return id;
  }

  @Override public boolean equals(Object obj) {
    return this == obj
        || obj instanceof Correlation
        && this.id == ((Correlation) obj).id
        && this.offset == ((Correlation) obj).offset;
  }
}

// End Correlation.java
