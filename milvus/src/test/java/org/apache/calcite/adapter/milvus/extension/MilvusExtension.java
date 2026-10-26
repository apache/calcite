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
package org.apache.calcite.adapter.milvus.extension;

import org.apache.calcite.adapter.milvus.util.ContainerUtil;

import io.milvus.client.MilvusServiceClient;
import io.milvus.param.ConnectParam;
import io.milvus.v2.client.ConnectConfig;
import io.milvus.v2.client.MilvusClientV2;

import org.junit.jupiter.api.Assumptions;
import org.junit.jupiter.api.extension.AfterAllCallback;
import org.junit.jupiter.api.extension.BeforeAllCallback;
import org.junit.jupiter.api.extension.ExtensionContext;
import org.testcontainers.DockerClientFactory;

import java.util.HashMap;
import java.util.Map;

/**
 * JUnit 5 Extension for managing Milvus container lifecycle.
 * This extension is responsible for:
 * <ul>
 *   <li>Starting Milvus containers before all tests in a class</li>
 *   <li>Creating Milvus client connections</li>
 *   <li>Cleaning up resources after all tests complete</li>
 * </ul>
 *
 * <p>Usage: Add {@code @ExtendWith(MilvusExtension.class)} to your test class.
 */
public class MilvusExtension implements BeforeAllCallback, AfterAllCallback {

  private static ContainerUtil containerUtil;
  private static MilvusClientV2 milvusClientV2;
  private static MilvusServiceClient milvusServiceClient; // V1 client for test data setup


  @Override public void beforeAll(ExtensionContext context) {
    Assumptions.assumeTrue(
        DockerClientFactory.instance().isDockerAvailable(),
        "Docker is not available, skipping Milvus tests");

    if (containerUtil == null) {
      containerUtil = new ContainerUtil();
      containerUtil.startMilvusContainers();
    }

    if (milvusClientV2 == null) {
      String host = containerUtil.getMilvusHost();
      Integer port = containerUtil.getMilvusPort();
      ConnectConfig connectConfig = ConnectConfig.builder()
          .uri("http://" + host + ":" + port)
          .build();
      milvusClientV2 = new MilvusClientV2(connectConfig);
    }

    if (milvusServiceClient == null) {
      String host = containerUtil.getMilvusHost();
      Integer port = containerUtil.getMilvusPort();
      ConnectParam cp = ConnectParam.newBuilder()
          .withHost(host)
          .withPort(port)
          .build();
      milvusServiceClient = new MilvusServiceClient(cp);
    }
  }

  @Override public void afterAll(ExtensionContext context) {
    // In Gradle builds, test classes can run concurrently even within the same worker.
    // Tearing down a shared Testcontainers-backed Milvus instance here can break other test
    // classes that are still executing.
    // We intentionally keep the container alive for the duration of the test JVM.
  }

  /**
   * Get the Milvus V2 client.
   * Note: This should only be called after the extension has been initialized.
   *
   * @return the MilvusClientV2 instance
   */
  public static MilvusClientV2 getMilvusClient() {
    if (milvusClientV2 == null) {
      throw new IllegalStateException(
          "Milvus client not initialized. Ensure @ExtendWith(MilvusExtension.class) is used.");
    }
    return milvusClientV2;
  }

  /**
   * Get the Milvus V1 client (for test data preparation only).
   * Note: This should only be called after the extension has been initialized.
   *
   * @return the MilvusServiceClient instance
   */
  public static MilvusServiceClient getMilvusClientV1() {
    if (milvusServiceClient == null) {
      throw new IllegalStateException(
          "Milvus V1 client not initialized. Ensure @ExtendWith(MilvusExtension.class) is used.");
    }
    return milvusServiceClient;
  }

  /**
   * Get Milvus connection parameters.
   *
   * @return map containing host and port
   */
  public static Map<String, Object> getConnectionParams() {
    if (containerUtil == null) {
      throw new IllegalStateException(
          "Container not initialized. Ensure @ExtendWith(MilvusExtension.class) is used.");
    }
    Map<String, Object> params = new HashMap<>();
    params.put("host", containerUtil.getMilvusHost());
    params.put("port", containerUtil.getMilvusPort());
    return params;
  }
}
