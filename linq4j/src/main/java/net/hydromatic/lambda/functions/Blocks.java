/*
// Licensed to Julian Hyde under one or more contributor license
// agreements. See the NOTICE file distributed with this work for
// additional information regarding copyright ownership.
//
// Julian Hyde licenses this file to you under the Apache License,
// Version 2.0 (the "License"); you may not use this file except in
// compliance with the License. You may obtain a copy of the License at:
//
// http://www.apache.org/licenses/LICENSE-2.0
//
// Unless required by applicable law or agreed to in writing, software
// distributed under the License is distributed on an "AS IS" BASIS,
// WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
// See the License for the specific language governing permissions and
// limitations under the License.
*/
package net.hydromatic.lambda.functions;

import net.hydromatic.linq4j.Linq4j;

/**
 * Static utility methods pertaining to {@code Block} instances.
 *
 * <p>Based on {@code java.util.functions.Blocks}.</p>
 */
public final class Blocks {

  private static final Block<Object> NOP = new Block<Object>() {
    public void apply(Object o) {
      // no thing
    }

    public Block<Object> chain(Block<? super Object> second) {
      return Blocks.chain(this, second);
    }
  };

  private static final Block<Object> REQUIRE_NON_NULL = new Block<Object>() {
    public void apply(Object o) {
      Linq4j.requireNonNull(o);
    }

    public Block<Object> chain(Block<? super Object> second) {
      return Blocks.chain(this, second);
    }
  };

  private Blocks() {
    throw new AssertionError();
  }


  public static <T> Block<T> nop() {
    return (Block<T>) NOP;
  }

  public static <T> Block<T> requireNonNull() {
    return (Block<T>) REQUIRE_NON_NULL;
  }

  public static <T> Block<T> chain(final Block<? super T> first,
      final Block<? super T> second) {
    return new Block<T>() {
      public void apply(T t) {
        first.apply(t);
        second.apply(t);
      }

      public Block<T> chain(Block<? super T> second) {
        //noinspection unchecked
        return Blocks.chain((Block) this, second);
      }
    };
  }

  public static <T> Block<T> chain(final Block<? super T>... sequence) {
    return new Block<T>() {
      public void apply(T t) {
        for (Block<? super T> block : sequence) {
          block.apply(t);
        }
      }

      public Block<T> chain(Block<? super T> second) {
        //noinspection unchecked
        return Blocks.chain((Block) this, second);
      }
    };
  }

  public static <T> Block<T> chain(
      final Iterable<? extends Block<? super T>> sequence) {
    return new Block<T>() {
      public void apply(T t) {
        for (Block<? super T> block : sequence) {
          block.apply(t);
        }
      }

      public Block<T> chain(Block<? super T> second) {
        //noinspection unchecked
        return Blocks.chain((Block) this, second);
      }
    };
  }

}

// End Blocks.java
