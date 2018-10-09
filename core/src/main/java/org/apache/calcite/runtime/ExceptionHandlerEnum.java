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
package org.apache.calcite.runtime;

import org.apache.calcite.linq4j.AbstractEnumerable;
import org.apache.calcite.linq4j.DelegatingEnumerator;
import org.apache.calcite.linq4j.Enumerable;
import org.apache.calcite.linq4j.Enumerator;

import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

/**
 * Enumeration of exception handlers.
 */
public enum ExceptionHandlerEnum implements ExceptionHandler {
  THROW() {
    @Override public <T> Enumerable<T> wrap(Enumerable<T> delegate) {
      return delegate;
    }
  },

  DISCARD() {
    @Override protected void logException(Throwable t) {
      LOGGER.debug("Encountered exception while running SQL, discarded", t);
    }
  },

  LOG() {
    @Override protected void logException(Throwable t) {
      LOGGER.warn("Encountered exception while running SQL", t);
    }
  };

  private static final Logger LOGGER = LoggerFactory.getLogger("org.apache.calcite.runtime");

  @Override public <T> Enumerable<T> wrap(Enumerable<T> delegate) {
    return new AbstractEnumerable<T>() {
      @Override public Enumerator<T> enumerator() {
        return new DelegatingEnumerator<T>(delegate.enumerator()) {
          private T current;
          @Override public boolean moveNext() {
            boolean hasNext;
            for (;;) {
              try {
                hasNext = delegate.moveNext();
                if (!hasNext) {
                  return false;
                }
                current = delegate.current();
              } catch (Throwable t) {
                logException(t);
                continue;
              }
              return true;
            }
          }

          @Override public T current() {
            return current;
          }
        };
      }
    };
  }

  protected void logException(Throwable t) {
    // do nothing by default
  }
}

// End ExceptionHandlerEnum.java
