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
package org.apache.calcite.plan;

import java.lang.annotation.ElementType;
import java.lang.annotation.Repeatable;
import java.lang.annotation.Retention;
import java.lang.annotation.RetentionPolicy;
import java.lang.annotation.Target;
import java.util.function.Supplier;

/**
 * Enables to declare that annotated {@link org.apache.calcite.rel.RelNode} (or its interface)
 * would always have given trait.
 * For instance, {@link org.apache.calcite.adapter.enumerable.EnumerableRel} specifies
 * {@code EnumerableConvention.INSTANCE}, which means each class that implements
 * {@link org.apache.calcite.adapter.enumerable.EnumerableRel} should have {@code ENUMERABLE}
 * convention.
 */
@Repeatable(value = ImplicitTraits.class)
@Retention(RetentionPolicy.RUNTIME)
@Target(ElementType.TYPE)
public @interface ImplicitTrait {
  Class<? extends Supplier<? extends RelTrait>> value();
}

// End ImplicitTrait.java
