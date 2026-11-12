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
package org.apache.calcite.adapter.milvus.util;

import org.slf4j.Logger;
import org.slf4j.LoggerFactory;
import org.testcontainers.containers.GenericContainer;
import org.testcontainers.containers.Network;
import org.testcontainers.containers.output.Slf4jLogConsumer;
import org.testcontainers.lifecycle.Startables;
import org.testcontainers.utility.DockerImageName;

import java.net.HttpURLConnection;
import java.net.URI;
import java.net.URL;
import java.time.Duration;
import java.util.HashMap;
import java.util.Map;
import java.util.stream.Stream;

/**
 * Utility class for managing Milvus test containers.
 */
public class ContainerUtil {

  public static final String ETCD_NET_ALIAS = "etcd";
  public static final String MINIO_NET_ALIAS = "minio";
  public static final String MILVUS_NET_ALIAS = "milvus";
  public static String etcdImageVersion = "quay.io/coreos/etcd:v3.5.18";
  public static String minioImageVersion = "minio/minio:RELEASE.2024-05-28T17-19-04Z";
  public static String milvusImageVersion = "milvusdb/milvus:v2.5.18";

  private static Logger logger = LoggerFactory.getLogger(ContainerUtil.class);
  private GenericContainer<?> etcdContainer;
  private GenericContainer<?> minioContainer;
  private GenericContainer<?> milvusContainer;
  private Network network;

  public ContainerUtil() {
  }

  private GenericContainer<?> initialContainer(
      String imageName,
      Map<String, String> env,
      String command,
      String netAliases,
      Integer... exporsedPort) {
    GenericContainer<?> container = new GenericContainer<>(DockerImageName.parse(imageName));

    return container
        .withEnv(env)
        .withCommand(command)
        .withLogConsumer(new Slf4jLogConsumer(logger))
        .withNetwork(network)
        .withNetworkAliases(netAliases)
        .withExposedPorts(exporsedPort);
  }

  public void startMilvusContainers() {
    this.network = Network.newNetwork();
    etcdContainer = initialEtcdContainer();
    minioContainer = initialMinioContainer();
    milvusContainer = initialMilvusContainer();
    etcdContainer.withStartupTimeout(Duration.ofMinutes(3));
    minioContainer.withStartupTimeout(Duration.ofMinutes(3));
    milvusContainer.withStartupTimeout(Duration.ofMinutes(5));
    milvusContainer.dependsOn(etcdContainer, minioContainer);
    Startables.deepStart(Stream.of(etcdContainer, minioContainer, milvusContainer)).join();
    waitForMilvusReady();
  }

  public String getMilvusHost() {
    return milvusContainer.getHost();
  }

  public Integer getMilvusPort() {
    return milvusContainer.getMappedPort(19530);
  }

  public void clearAll() {
    milvusContainer.close();
    minioContainer.close();
    etcdContainer.close();
    network.close();
  }

  private GenericContainer<?> initialEtcdContainer() {
    Map<String, String> etcdEnv =
        new HashMap<String, String>() {
          {
            this.put("ETCD_AUTO_COMPACTION_MODE", "revision");
            this.put("ETCD_AUTO_COMPACTION_RETENTION", "1000");
            this.put("ETCD_QUOTA_BACKEND_BYTES", "4294967296");
            this.put("ETCD_SNAPSHOT_COUNT", "50000");
          }
        };
    String command =
        "etcd -advertise-client-urls=http://etcd:2379 -listen-client-urls"
            + " http://0.0.0.0:2379 --data-dir /etcd ";
    return initialContainer(etcdImageVersion, etcdEnv, command, ETCD_NET_ALIAS, 2379);
  }

  private GenericContainer<?> initialMinioContainer() {
    Map<String, String> minioEnv =
        new HashMap<String, String>() {
          {
            this.put("MINIO_ACCESS_KEY", "minioadmin");
            this.put("MINIO_SECRET_KEY", "minioadmin");
          }
        };
    String command = "minio server /minio_data --console-address :9001";
    return initialContainer(
        minioImageVersion, minioEnv, command, MINIO_NET_ALIAS, 9000, 9001);
  }

  private GenericContainer<?> initialMilvusContainer() {
    Map<String, String> milvusEnv =
        new HashMap<String, String>() {
          {
            this.put("ETCD_ENDPOINTS", ETCD_NET_ALIAS + ":" + "2379");
            this.put("MINIO_ADDRESS", MINIO_NET_ALIAS + ":" + "9000");
            this.put("MINIO_REGION", "us-east-1");
          }
        };

    String command = "milvus run standalone";
    return initialContainer(
        milvusImageVersion, milvusEnv, command, MILVUS_NET_ALIAS, 19530, 9091);
  }

  private void waitForMilvusReady() {
    int maxRetries = 30; // maximum retry count
    int retryInterval = 5000; // retry interval in milliseconds

    for (int i = 0; i < maxRetries; i++) {
      try {
        String host = milvusContainer.getHost();
        int port = milvusContainer.getMappedPort(9091);
        URI healthUri = new URI("http://" + host + ":" + port + "/healthz");
        URL healthUrl = healthUri.toURL();
        HttpURLConnection connection = (HttpURLConnection) healthUrl.openConnection();
        connection.setRequestMethod("GET");
        connection.setConnectTimeout(5000);
        connection.setReadTimeout(5000);

        int responseCode = connection.getResponseCode();
        if (responseCode == 200) {
          logger.info("Milvus health check passed, service is ready");
          return;
        } else {
          logger.warn("Milvus health check returned code: {}, retrying...", responseCode);
        }
      } catch (Exception e) {
        logger.info("Waiting for Milvus to be ready... (attempt {}/{})", i + 1, maxRetries);
      }

      try {
        Thread.sleep(retryInterval);
      } catch (InterruptedException ie) {
        Thread.currentThread().interrupt();
        throw new RuntimeException("Interrupted while waiting for Milvus to be ready", ie);
      }
    }

    throw new RuntimeException("Milvus failed to become ready within timeout");
  }

}
