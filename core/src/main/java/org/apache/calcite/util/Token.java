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
package org.apache.calcite.util;

import java.util.Map;
import java.util.concurrent.ConcurrentHashMap;
import java.util.concurrent.atomic.AtomicInteger;

/** Hands out tokens, and throws if they are not all released.
 *
 * <p>Typical use:
 *
 * <blockquote><pre>{@code
 * Token.Pool pool = Token.pool();
 * Token token1 = pool.token();
 * Token token2 = pool.token();
 * token1.close();
 * pool.assertEmpty(); // throws because token2 has not been closed
 * }</pre></blockquote>
 * */
public class Token implements AutoCloseable {
  private final Pool pool;
  private final int id;
  private final StackTraceElement[] stackElements;

  /** Creates a Token. Should only be called from {@link Pool#token()}. */
  private Token(Pool pool, int id, StackTraceElement[] stackElements) {
    this.pool = pool;
    this.id = id;
    this.stackElements = stackElements;
  }

  @Override public String toString() {
    return Integer.toString(id);
  }

  /** Releases this Token. */
  @Override public void close() {
    if (!pool.release(id)) {
      final RuntimeException x =
          new RuntimeException("token " + id + " has already released");
      x.setStackTrace(stackElements);
      throw x;
    }
  }

  /** Creates a pool. */
  public static Pool pool() {
    return new Pool();
  }

  /** A collection of tokens.
   *
   * <p>It is thread-safe. */
  public static class Pool {
    private final Map<Integer, Token> map = new ConcurrentHashMap<>();
    private final AtomicInteger ordinal = new AtomicInteger();

    /** Creates a token. */
    public Token token() {
      return map.computeIfAbsent(ordinal.getAndIncrement(),
          id ->
              new Token(Pool.this, id, Thread.currentThread().getStackTrace()));
    }

    /** Releases a token id. Should be called from {@link Token#close()}. */
    @SuppressWarnings("resource")
    private boolean release(int id) {
      return map.remove(id) != null;
    }

    /** Throws if not all fixtures have been released. */
    public void assertEmpty() {
      int size = map.size();
      if (!map.isEmpty()) {
        final RuntimeException x =
            new RuntimeException("map should be empty, but contains " + size
                + " tokens");
        x.setStackTrace(map.values().iterator().next().stackElements);
        throw x;
      }
    }
  }
}
