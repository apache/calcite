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
package org.apache.calcite.adapter.file;

import org.junit.jupiter.api.extension.ConditionEvaluationResult;
import org.junit.jupiter.api.extension.ExecutionCondition;
import org.junit.jupiter.api.extension.ExtensionContext;
import org.junit.platform.commons.support.AnnotationSupport;

import java.net.Socket;

import static org.junit.jupiter.api.extension.ConditionEvaluationResult.disabled;
import static org.junit.jupiter.api.extension.ConditionEvaluationResult.enabled;

/**
 * Enables to activate test conditionally if the specified host is reachable.
 * Note: it is recommended to avoid creating tests that depend on external servers.
 */
public class RequiresNetworkExtension implements ExecutionCondition {
  @Override public ConditionEvaluationResult evaluateExecutionCondition(ExtensionContext context) {
    return context.getElement()
        .flatMap(element -> AnnotationSupport.findAnnotation(element, RequiresNetwork.class))
        .map(net -> {
          try (Socket ignored = new Socket(net.host(), net.port())) {
            return enabled(net.host() + ":" + net.port() + " is reachable");
          } catch (Exception e) {
            return disabled(net.host() + ":" + net.port() + " is unreachable: " + e.getMessage());
          }
        })
        .orElseGet(() -> enabled("@RequiresNetwork is not found"));
  }
}
