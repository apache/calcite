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
package org.apache.calcite.rel.metadata.janino;

import org.apiguardian.api.API;

import java.util.stream.IntStream;

/**
 * Functions used by generated code.
 */
@API(status = API.Status.INTERNAL)
public class CacheUtil {

  private CacheUtil() {
  }

  @API(status = API.Status.INTERNAL)
  public static Object[] generateRange(String description, int min, int max) {
    return IntStream.range(min, max)
        .mapToObj(i -> description + "(" + i + ")")
        .map(org.apache.calcite.rel.metadata.janino.DescriptiveCacheKey::new)
        .toArray();
  }

  @API(status = API.Status.INTERNAL)
  public static <E extends Enum<E>> Object[] generateEnum(String description, E[] values) {
    return java.util.Arrays.stream(values)
        .map(e -> description + "(" + e + ")")
        .map(org.apache.calcite.rel.metadata.janino.DescriptiveCacheKey::new)
        .toArray();
  }
}
