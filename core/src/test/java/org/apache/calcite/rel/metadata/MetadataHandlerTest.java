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

import org.junit.jupiter.api.Test;

import java.lang.reflect.Method;

import static org.hamcrest.CoreMatchers.is;
import static org.hamcrest.MatcherAssert.assertThat;
import static org.hamcrest.Matchers.emptyArray;

/**
 * Tests for {@link MetadataHandler}.
 */
class MetadataHandlerTest {
  @Test void findsHandlerMethods() {
    Method[] methods = MetadataHandler.handlerMethods(TestMetadataHandler.class);

    assertThat(methods.length, is(1));
    assertThat(methods[0].getName(), is("getTestMetadata"));
  }

  @Test void getDefMethodInHandlerIsIgnored() {
    Method[] methods =
        MetadataHandler.handlerMethods(
            MetadataHandlerWithGetDefMethodOnly.class);

    assertThat(methods, is(emptyArray()));
  }

  @Test void staticMethodInHandlerIsIgnored() {
    Method[] methods =
        MetadataHandler.handlerMethods(MetadataHandlerWithStaticMethod.class);

    assertThat(methods, is(emptyArray()));
  }

  @Test void synthenticMethodInHandlerIsIgnored() {
    Method[] methods =
        MetadataHandler.handlerMethods(
            TestMetadataHandlers.handlerClassWithSyntheticMethod());

    assertThat(methods, is(emptyArray()));
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
