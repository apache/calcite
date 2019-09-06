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
package org.apache.calcite.linq4j;

import com.google.common.base.Preconditions;

/**
 * Represents an integer in modular arithmetic.
 * Its {@code value} is between 0 and {@code m - 1} for some modulus {@code m}.
 *
 * <p>This object is immutable; all operations create a new object.
 */
class ModularInteger {
  private final int value;
  private final int modulus;

  /** Creates a ModularInteger. */
  ModularInteger(int value, int modulus) {
    Preconditions.checkArgument(value >= 0 && value < modulus);
    this.value = value;
    this.modulus = modulus;
  }

  @Override public boolean equals(Object obj) {
    return obj == this
        || obj instanceof ModularInteger
        && value == ((ModularInteger) obj).value
        && modulus == ((ModularInteger) obj).modulus;
  }

  @Override public int hashCode() {
    // 8191 is prime and, as 2^13 - 1, can easily be multiplied by bit-shifting
    return value + 8191 * modulus;
  }

  public int get() {
    return this.value;
  }

  public ModularInteger plus(int operand) {
    if (operand < 0) {
      return minus(Math.abs(operand));
    }
    return new ModularInteger((value + operand) % modulus, modulus);
  }

  public ModularInteger minus(int operand) {
    assert operand >= 0;
    int r = value - operand;
    while (r < 0) {
      r = r + modulus;
    }
    return new ModularInteger(r, modulus);
  }

  @Override public String toString() {
    return value + " mod " + modulus;
  }
}

// End ModularInteger.java
