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
package org.apache.calcite.runtime;

import org.apache.calcite.linq4j.function.Deterministic;
import org.apache.calcite.linq4j.function.Parameter;

import java.util.Random;

/**
 * Function object for {@code RAND} and {@code RAND_INTEGER}, with and without
 * seed.
 */
@SuppressWarnings("unused")
public class RandomFunction {
  private Random random;

  /** Creates a RandomFunction.
   *
   * <p>Marked deterministic so that the code generator instantiates one once
   * per query, not once per row. */
  @Deterministic public RandomFunction() {
  }

  /** Implements the {@code RAND()} SQL function. */
  public double rand() {
    if (random == null) {
      random = new Random();
    }
    return random.nextDouble();
  }

  /** Implements the {@code RAND(seed)} SQL function. */
  public double randSeed(@Parameter(name = "seed") int seed) {
    if (random == null) {
      random = new Random(seed ^ (seed << 16));
    }
    return random.nextDouble();
  }

  /** Implements the {@code RAND_INTEGER(bound)} SQL function. */
  public int randInteger(@Parameter(name = "bound") int bound) {
    if (random == null) {
      random = new Random();
    }
    return random.nextInt(bound);
  }

  /** Implements the {@code RAND_INTEGER(seed, bound)} SQL function. */
  public int randIntegerSeed(@Parameter(name = "seed") int seed,
      @Parameter(name = "bound") int bound) {
    if (random == null) {
      random = new Random(seed);
    }
    return random.nextInt(bound);
  }

}

// End RandomFunction.java
