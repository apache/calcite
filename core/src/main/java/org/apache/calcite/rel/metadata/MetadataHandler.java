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

import java.lang.reflect.Method;
import java.lang.reflect.Modifier;
import java.util.Arrays;

/**
 * Marker interface for a handler of metadata.
 *
 * @param <M> Kind of metadata
 */
public interface MetadataHandler<M extends Metadata> {
  MetadataDef<M> getDef();

  /**
   * Finds handler methods defined by a {@link MetadataHandler}. Static and synthetic methods
   * are ignored.
   *
   * @param handlerClass the handler class to inspect
   * @return handler methods
   */
  static Method[] handlerMethods(Class<? extends MetadataHandler<?>> handlerClass) {
    return Arrays.stream(handlerClass.getDeclaredMethods())
        .filter(m -> !m.getName().equals("getDef"))
        .filter(m -> !m.isSynthetic())
        .filter(m -> !Modifier.isStatic(m.getModifiers()))
        .toArray(Method[]::new);
  }
}
