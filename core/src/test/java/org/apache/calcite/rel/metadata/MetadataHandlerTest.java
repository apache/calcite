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

import com.google.common.collect.ImmutableList;

import org.junit.jupiter.api.Test;

import java.lang.reflect.Method;
import java.util.List;
import java.util.SortedMap;

import static org.hamcrest.CoreMatchers.is;
import static org.hamcrest.MatcherAssert.assertThat;
import static org.hamcrest.Matchers.empty;
import static org.hamcrest.Matchers.hasSize;

/**
 * Tests for {@link MetadataHandler}.
 */
class MetadataHandlerTest {
  @Test void findsHandlerMethods() {
    final SortedMap<String, Method> map =
        MetadataHandler.handlerMethods(TestMetadataHandler.class);
    final List<Method> methods = ImmutableList.copyOf(map.values());

    assertThat(methods, hasSize(1));
    assertThat(methods.get(0).getName(), is("getTestMetadata"));
  }

  @Test void getDefMethodInHandlerIsIgnored() {
    final SortedMap<String, Method> map =
        MetadataHandler.handlerMethods(
            MetadataHandlerWithGetDefMethodOnly.class);
    final List<Method> methods = ImmutableList.copyOf(map.values());

    assertThat(methods, empty());
  }

  @Test void staticMethodInHandlerIsIgnored() {
    final SortedMap<String, Method> map =
        MetadataHandler.handlerMethods(MetadataHandlerWithStaticMethod.class);
    final List<Method> methods = ImmutableList.copyOf(map.values());

    assertThat(methods, empty());
  }

  @Test void syntheticMethodInHandlerIsIgnored() {
    final SortedMap<String, Method> map =
        MetadataHandler.handlerMethods(
            TestMetadataHandlers.handlerClassWithSyntheticMethod());
    final List<Method> methods = ImmutableList.copyOf(map.values());

    assertThat(methods, empty());
  }

  /**
   * {@link MetadataHandler} which has a handler method.
   */
  interface TestMetadataHandler extends MetadataHandler<TestMetadata> {
    @SuppressWarnings("unused")
    TestMetadata getTestMetadata();
  }

  /**
   * {@link MetadataHandler} which only has getDef() method.
   */
  interface MetadataHandlerWithGetDefMethodOnly extends MetadataHandler<TestMetadata> {
    MetadataDef<TestMetadata> getDef();
  }
}
