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
package org.jspecify.annotations;

import java.lang.annotation.Documented;
import java.lang.annotation.ElementType;
import java.lang.annotation.Retention;
import java.lang.annotation.RetentionPolicy;
import java.lang.annotation.Target;

/**
 * Compile-time stub of <a href="https://jspecify.dev/">JSpecify</a>'s
 * {@code @NullMarked} annotation.
 *
 * <p>This stub exists so that test fixtures can use JSpecify-style nullness
 * annotations without adding a dependency on the real {@code org.jspecify:jspecify}
 * artifact to the build. {@link org.apache.calcite.util.ReflectUtil} matches
 * nullness annotations by fully qualified name, so this stub behaves identically
 * to the real annotation at runtime. It must be removed if the real JSpecify
 * artifact ever comes onto the classpath.
 *
 * <p>Unlike the real annotation, the target list omits
 * {@link ElementType#MODULE}, which does not exist when compiling with
 * {@code --release 8}.
 */
@Documented
@Retention(RetentionPolicy.RUNTIME)
@Target({ElementType.ANNOTATION_TYPE, ElementType.CONSTRUCTOR,
    ElementType.FIELD, ElementType.METHOD, ElementType.PACKAGE,
    ElementType.PARAMETER, ElementType.TYPE})
public @interface NullMarked {
}
