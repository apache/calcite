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
package org.apache.calcite.adapter.elasticsearch;

import java.util.Locale;
import java.util.Objects;

/**
 * Identifies current ES version at runtime. Some queries have different syntax
 * depending on version (eg. 2 vs 5).
 */
enum ElasticsearchVersion {

  ES2(2),
  ES5(5),
  ES6(6),
  ES7(7),
  UNKNOWN(0);

  private final int elasticVersionMajor;

  ElasticsearchVersion(final int elasticVersionMajor) {
    this.elasticVersionMajor = elasticVersionMajor;
  }

  public int elasticVersionMajor() {
    return elasticVersionMajor;
  }

  static ElasticsearchVersion fromString(String version) {
    Objects.requireNonNull(version, "version");
    if (!version.matches("\\d+\\.\\d+\\.\\d+")) {
      final String message = String.format(Locale.ROOT, "Wrong version format. "
          + "Expected ${digit}.${digit}.${digit} but got %s", version);
      throw new IllegalArgumentException(message);
    }

    // version format is: major.minor.revision
    final int major = Integer.parseInt(version.substring(0, version.indexOf(".")));
    if (major == 2) {
      return ES2;
    } else if (major == 5) {
      return ES5;
    } else if (major == 6) {
      return ES6;
    } else if (major == 7) {
      return ES7;
    } else {
      return UNKNOWN;
    }
  }
}

// End ElasticsearchVersion.java
