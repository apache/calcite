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
package org.apache.calcite.avatica.metrics.noop;

import org.apache.calcite.avatica.metrics.MetricsSystemConfiguration;
import org.apache.calcite.avatica.metrics.MetricsSystemFactory;

/**
 * A {@link MetricsSystemFactory} for the {@link NoopMetricsSystem}.
 *
 * No service file is provided for this implementation. It is the fallback implementation if
 * no implementation or more than one implementation is found on the classpath.
 */
public class NoopMetricsSystemFactory implements MetricsSystemFactory {

  @Override public NoopMetricsSystem create(MetricsSystemConfiguration<?> config) {
    return NoopMetricsSystem.getInstance();
  }
}

// End NoopMetricsSystemFactory.java
