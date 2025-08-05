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
package org.apache.calcite.adapter.governance.provider;

import java.util.List;
import java.util.Map;

/**
 * Interface for cloud provider implementations.
 */
public interface CloudProvider {
  /**
   * Query Kubernetes clusters.
   */
  List<Map<String, Object>> queryKubernetesClusters(List<String> accountIds);

  /**
   * Query storage resources.
   */
  List<Map<String, Object>> queryStorageResources(List<String> accountIds);

  /**
   * Query compute instances.
   */
  List<Map<String, Object>> queryComputeInstances(List<String> accountIds);

  /**
   * Query network resources.
   */
  List<Map<String, Object>> queryNetworkResources(List<String> accountIds);

  /**
   * Query IAM resources.
   */
  List<Map<String, Object>> queryIAMResources(List<String> accountIds);

  /**
   * Query database resources.
   */
  List<Map<String, Object>> queryDatabaseResources(List<String> accountIds);

  /**
   * Query container registries.
   */
  List<Map<String, Object>> queryContainerRegistries(List<String> accountIds);
}
