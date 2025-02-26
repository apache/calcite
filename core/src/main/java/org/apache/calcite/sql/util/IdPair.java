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
package org.apache.calcite.sql.util;

import org.checkerframework.checker.nullness.qual.Nullable;

import java.util.List;
import java.util.Map;

import static java.util.Objects.requireNonNull;

/** Similar to {@link org.apache.calcite.util.Pair} but identity is based
 * on identity of values.
 *
 * <p>Also, uses {@code hashCode} algorithm of {@link List},
 * not {@link Map.Entry#hashCode()}.
 *
 * @param <L> Left type
 * @param <R> Right type
 */
public class IdPair<L, R> {
  final L left;
  final R right;

  /** Creates an IdPair. */
  public static <L, R> IdPair<L, R> of(L left, R right) {
    return new IdPair<>(left, right);
  }

  protected IdPair(L left, R right) {
    this.left = requireNonNull(left, "left");
    this.right = requireNonNull(right, "right");
  }

  @Override public String toString() {
    return left + "=" + right;
  }

  @Override public boolean equals(@Nullable Object obj) {
    return obj == this
        || obj instanceof IdPair
        && left == ((IdPair) obj).left
        && right == ((IdPair) obj).right;
  }

  @Override public int hashCode() {
    return (31
        + System.identityHashCode(left)) * 31
        + System.identityHashCode(right);
  }
}
