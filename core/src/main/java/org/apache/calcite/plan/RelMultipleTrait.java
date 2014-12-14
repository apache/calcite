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

/**
 * Trait for which a given relational expression can have multiple values.
 *
 * <p>The most common example is sorted-ness (collation). The TIME dimension
 * table might be sorted by [year, month, date] and also by [time_id].
 */
public interface RelMultipleTrait
    extends RelTrait, Comparable<RelMultipleTrait> {
  /** Returns whether this trait is satisfied by every instance of the trait
   * (including itself). */
  boolean isTop();
}

// End RelMultipleTrait.java
