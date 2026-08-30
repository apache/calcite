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
 * Nullness annotations that JSpecify does not define.
 *
 * <p>JSpecify covers {@code @Nullable}, {@code @NonNull} and {@code @NullMarked} only. The
 * annotations here cover what Calcite additionally needs to express: a method whose result is
 * null exactly when an argument is null, a field that starts as null and stays non-null once
 * assigned, and pre- and postconditions on fields.
 *
 * <p>A nullness checker recognises them by the last component of their name rather than by their
 * package, so they need no dependency on the checker itself. NullAway is the checker Calcite
 * runs; see {@code NullabilityUtil.findAnnotation} and
 * {@code Nullness.isMonotonicNonNullAnnotation} in NullAway for the matching rules.
 */
@NullMarked
package org.apache.calcite.linq4j.annotations;

import org.jspecify.annotations.NullMarked;
