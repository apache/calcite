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
package org.apache.calcite.adapter.enumerable;

/**
 * Describes when a function/operator will return null.
 *
 * <p>STRICT and ANY are similar. STRICT says f(a0, a1) will NEVER return
 * null if a0 and a1 are not null. This means that we can check whether f
 * returns null just by checking its arguments. Use STRICT in preference to
 * ANY whenever possible.
 */
public enum NullPolicy {
  /** Returns null if and only if all of the arguments are null;
   * If all of the arguments are false return false otherwise true. */
  ALL,
  /** Returns null if and only if one of the arguments are null. */
  STRICT,
  /** Returns null if one of the arguments is null, and possibly other times. */
  SEMI_STRICT,
  /** If any of the arguments are null, return null.
   * @deprecated {@link #SEMI_STRICT} has identical semantics so use this instead. */
  @Deprecated
  ANY,
  /** If the first argument is null, return null. */
  ARG0,
  NONE
}
