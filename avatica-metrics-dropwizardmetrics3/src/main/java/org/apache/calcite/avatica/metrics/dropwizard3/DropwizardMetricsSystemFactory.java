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
package org.apache.calcite.avatica.metrics.dropwizard3;

import org.apache.calcite.avatica.metrics.MetricsSystemConfiguration;
import org.apache.calcite.avatica.metrics.MetricsSystemFactory;

/**
 * A {@link MetricsSystemFactory} for {@link DropwizardMetricsSystem}.
 */
public class DropwizardMetricsSystemFactory implements MetricsSystemFactory {

  @Override public DropwizardMetricsSystem create(MetricsSystemConfiguration<?> config) {
    // Verify we got configuration this factory can use
    if (config instanceof DropwizardMetricsSystemConfiguration) {
      DropwizardMetricsSystemConfiguration typedConfig =
          (DropwizardMetricsSystemConfiguration) config;

      return new DropwizardMetricsSystem(typedConfig.get());
    }

    throw new IllegalStateException("Expected instance of "
        + DropwizardMetricsSystemConfiguration.class.getName() + " but got "
        + (null == config ? "null" : config.getClass().getName()));
  }
}

// End DropwizardMetricsSystemFactory.java
