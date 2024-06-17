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
package org.apache.calcite.rel.metadata;

import net.bytebuddy.ByteBuddy;
import net.bytebuddy.description.modifier.SyntheticState;
import net.bytebuddy.description.modifier.Visibility;
import net.bytebuddy.dynamic.loading.ClassLoadingStrategy;
import net.bytebuddy.implementation.FixedValue;

/**
 * Constructs {@link MetadataHandler} classes useful for tests.
 */
class TestMetadataHandlers {
  /**
   * Returns a class representing an interface extending {@link MetadataHandler} and having
   * a synthetic method.
   *
   * @return MetadataHandler class with a synthetic method
   */
  static Class<? extends MetadataHandler<TestMetadata>> handlerClassWithSyntheticMethod() {
    return new ByteBuddy()
        .redefine(BlankMetadataHandler.class)
        .defineMethod("syntheticMethod", Void.class, SyntheticState.SYNTHETIC, Visibility.PUBLIC)
        .intercept(FixedValue.nullValue())
        .make()
        .load(TestMetadataHandlers.class.getClassLoader(), ClassLoadingStrategy.Default.CHILD_FIRST)
        .getLoaded();
  }

  private TestMetadataHandlers() {
    // prevent instantiation
  }

  /**
   * A blank {@link MetadataHandler} that is used as a base for adding a synthetic method.
   */
  private interface BlankMetadataHandler extends MetadataHandler<TestMetadata> {
  }
}
