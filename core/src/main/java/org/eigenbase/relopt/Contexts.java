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
package org.eigenbase.relopt;

import net.hydromatic.optiq.config.OptiqConnectionConfig;

/**
 * Utilities for {@link Context}.
 */
public class Contexts {
  public static final EmptyContext EMPTY_CONTEXT = new EmptyContext();

  private Contexts() {}

  /** Returns a context that contains a
   * {@link net.hydromatic.optiq.config.OptiqConnectionConfig}. */
  public static Context withConfig(OptiqConnectionConfig config) {
    return new ConfigContext(config);
  }

  /** Returns a context that returns null for all inquiries. */
  public static Context empty() {
    return EMPTY_CONTEXT;
  }

  /** Context that contains a
   * {@link net.hydromatic.optiq.config.OptiqConnectionConfig}. */
  private static class ConfigContext implements Context {
    private OptiqConnectionConfig config;

    public ConfigContext(OptiqConnectionConfig config) {
      this.config = config;
    }

    public <T> T unwrap(Class<T> clazz) {
      if (clazz.isInstance(config)) {
        return clazz.cast(config);
      }
      return null;
    }
  }

  /** Empty context. */
  static class EmptyContext implements Context {
    public <T> T unwrap(Class<T> clazz) {
      return null;
    }
  }
}

// End Contexts.java
