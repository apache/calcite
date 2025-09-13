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
package org.apache.calcite.adapter.govdata;

import java.io.BufferedReader;
import java.io.File;
import java.io.FileReader;
import java.io.IOException;
import java.nio.file.Files;
import java.nio.file.Path;
import java.nio.file.Paths;
import java.util.HashMap;
import java.util.Map;

/**
 * Utility class to load environment variables from .env.test file for tests.
 * This ensures all tests have consistent access to configuration.
 */
public class TestEnvironmentLoader {
  private static final Map<String, String> ENV_VARS = new HashMap<>();
  private static boolean loaded = false;

  static {
    loadEnvironment();
  }

  /**
   * Load environment variables from .env.test file.
   */
  private static void loadEnvironment() {
    if (loaded) {
      return;
    }

    // Look for .env.test file in the govdata module directory
    Path[] possiblePaths = {
        Paths.get("govdata/.env.test"),
        Paths.get(".env.test"),
        Paths.get("../govdata/.env.test"),
        Paths.get("/Users/kennethstott/calcite/govdata/.env.test")
    };

    File envFile = null;
    for (Path path : possiblePaths) {
      if (Files.exists(path)) {
        envFile = path.toFile();
        break;
      }
    }

    if (envFile == null || !envFile.exists()) {
      System.err.println("Warning: .env.test file not found. Tests may fail if they require environment variables.");
      loaded = true;
      return;
    }

    try (BufferedReader reader = new BufferedReader(new FileReader(envFile))) {
      String line;
      while ((line = reader.readLine()) != null) {
        // Skip comments and empty lines
        if (line.trim().isEmpty() || line.trim().startsWith("#")) {
          continue;
        }

        // Parse KEY=VALUE pairs
        int equalsIndex = line.indexOf('=');
        if (equalsIndex > 0) {
          String key = line.substring(0, equalsIndex).trim();
          String value = line.substring(equalsIndex + 1).trim();
          
          // Remove quotes if present
          if (value.startsWith("\"") && value.endsWith("\"")) {
            value = value.substring(1, value.length() - 1);
          }
          
          ENV_VARS.put(key, value);
          // Also set as system property for Calcite's EnvironmentVariableSubstitutor
          System.setProperty(key, value);
        }
      }
      
      System.out.println("Loaded " + ENV_VARS.size() + " environment variables from .env.test");
      loaded = true;
      
    } catch (IOException e) {
      System.err.println("Warning: Failed to load .env.test file: " + e.getMessage());
      loaded = true;
    }
  }

  /**
   * Get an environment variable value.
   * First checks System.getenv(), then falls back to loaded values from .env.test.
   */
  public static String getEnv(String key) {
    // First check actual environment
    String value = System.getenv(key);
    if (value != null) {
      return value;
    }
    
    // Then check loaded values
    return ENV_VARS.get(key);
  }

  /**
   * Ensure environment is loaded.
   * Call this at the beginning of each test to ensure .env.test is loaded.
   */
  public static void ensureLoaded() {
    if (!loaded) {
      loadEnvironment();
    }
  }
}