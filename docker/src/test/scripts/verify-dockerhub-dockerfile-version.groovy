/*
 * Licensed to the Apache Software Foundation (ASF) under one
 * or more contributor license agreements.  See the NOTICE file
 * distributed with this work for additional information
 * regarding copyright ownership.  The ASF licenses this file
 * to you under the Apache License, Version 2.0 (the
 * "License"); you may not use this file except in compliance
 * with the License.  You may obtain a copy of the License at
 *
 * http://www.apache.org/licenses/LICENSE-2.0
 *
 * Unless required by applicable law or agreed to in writing, software
 * distributed under the License is distributed on an "AS IS" BASIS,
 * WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
 * See the License for the specific language governing permissions and
 * limitations under the License.
 */

import java.io.File;
import java.nio.file.Files;

System.out.println("Verifying the included version in the Dockerhub Dockerfile");

String expectedPrefix = "ARG AVATICA_VERSION=";
String expectedVersion = project.getVersion();

if (null == expectedVersion) {
  throw new IllegalArgumentException("Did not find Maven project version");
}

String dockerfilePath = "src/main/dockerhub/Dockerfile";
File dockerfile = new File(basedir, dockerfilePath);
if (!dockerfile.isFile()) {
  throw new FileNotFoundException("Could not file dockerhub Dockerfile at " + dockerfilePath);
}

List<String> lines = Files.readAllLines(dockerfile.toPath());
for (String line : lines) {
  line = line.trim();
  if (line.startsWith(expectedPrefix)) {
    String value = line.substring(expectedPrefix.length());
    // Trim leading and trailing quotation marks
    value = value.substring(1, value.length() - 1);
    if (expectedVersion.equals(value)) {
      System.out.println("Found expected version in DockerHub dockerfile of " + value);
      return true;
    } else {
      throw new IllegalArgumentException("Expected Avatica version of " + expectedVersion + " but got " + value);
    }
  }
}

throw new IllegalArgumentException("Could not extract Avatica version from " + dockerfile);
