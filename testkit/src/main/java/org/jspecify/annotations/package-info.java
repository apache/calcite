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

/**
 * Compile-time stubs of the <a href="https://jspecify.dev/">JSpecify</a>
 * nullness annotations.
 *
 * <p>These stubs let test fixtures use JSpecify-style nullness annotations
 * without adding a dependency on the real {@code org.jspecify:jspecify}
 * artifact to the build. {@link org.apache.calcite.util.ReflectUtil} matches
 * nullness annotations by fully qualified name, so the stubs behave identically
 * to the real annotations at runtime. They must be removed if the real
 * JSpecify artifact ever comes onto the classpath.
 */
package org.jspecify.annotations;
