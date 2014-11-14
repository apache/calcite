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
package org.apache.calcite.materialize;

import java.io.Serializable;
import java.util.UUID;

/**
 * Unique identifier for a materialization.
 *
 * <p>It is immutable and can only be created by the
 * {@link MaterializationService}. For communicating with the service.</p>
 */
public class MaterializationKey implements Serializable {
  private final UUID uuid = UUID.randomUUID();

  @Override public int hashCode() {
    return uuid.hashCode();
  }

  @Override public boolean equals(Object obj) {
    return this == obj
        || obj instanceof MaterializationKey
        && uuid.equals(((MaterializationKey) obj).uuid);
  }

  @Override public String toString() {
    return uuid.toString();
  }
}

// End MaterializationKey.java
