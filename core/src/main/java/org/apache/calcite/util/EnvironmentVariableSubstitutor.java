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
package org.apache.calcite.util;

import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.util.HashMap;
import java.util.Map;
import java.util.regex.Matcher;
import java.util.regex.Pattern;

/**
 * Utility class for substituting environment variables in configuration strings.
 * 
 * <p>Supports the following syntax patterns:
 * <ul>
 *   <li>{@code ${VAR_NAME}} - Substitutes with environment variable value, fails if not found</li>
 *   <li>{@code ${VAR_NAME:default}} - Substitutes with environment variable value, uses default if not found</li>
 * </ul>
 * 
 * <p>Example usage:
 * <pre>{@code
 * String input = "Directory: ${DATA_DIR:/data}, Engine: ${ENGINE_TYPE:LINQ4J}";
 * String result = EnvironmentVariableSubstitutor.substitute(input);
 * }</pre>
 */
public class EnvironmentVariableSubstitutor {
  private static final Logger LOGGER = LoggerFactory.getLogger(EnvironmentVariableSubstitutor.class);
  
  // Pattern to match ${VAR} or ${VAR:default}
  // Captures: group(1) = variable name, group(2) = optional default value (without colon)
  private static final Pattern ENV_VAR_PATTERN = Pattern.compile("\\$\\{([^:}]+)(?::([^}]*))?\\}");
  
  private EnvironmentVariableSubstitutor() {
    // Utility class, prevent instantiation
  }
  
  /**
   * Substitutes environment variables in the given input string.
   * 
   * @param input the input string containing environment variable placeholders
   * @return the string with environment variables substituted
   * @throws IllegalArgumentException if an environment variable is not found and no default is provided
   */
  public static String substitute(String input) {
    // Create a combined map that includes both environment variables and system properties
    // System properties take precedence for testing purposes
    Map<String, String> combined = new HashMap<>(System.getenv());
    System.getProperties().forEach((k, v) -> combined.put(k.toString(), v.toString()));
    return substitute(input, combined);
  }
  
  /**
   * Substitutes environment variables in the given input string using a custom environment map.
   * This overload is primarily for testing.
   * 
   * @param input the input string containing environment variable placeholders
   * @param environment the environment variables map
   * @return the string with environment variables substituted
   * @throws IllegalArgumentException if an environment variable is not found and no default is provided
   */
  public static String substitute(String input, Map<String, String> environment) {
    if (input == null) {
      return null;
    }
    
    StringBuffer result = new StringBuffer();
    Matcher matcher = ENV_VAR_PATTERN.matcher(input);
    
    while (matcher.find()) {
      String varName = matcher.group(1);
      String defaultValue = matcher.group(2);
      String value = environment.get(varName);
      
      if (value == null) {
        if (defaultValue != null) {
          value = defaultValue;
          LOGGER.debug("Environment variable '{}' not found, using default: '{}'", varName, defaultValue);
        } else {
          throw new IllegalArgumentException(
              String.format("Environment variable '%s' is not defined and no default value was provided", varName));
        }
      } else {
        LOGGER.debug("Substituting environment variable '{}' with value: '{}'", varName, value);
      }
      
      // Use Matcher.quoteReplacement to handle special characters in the replacement string
      matcher.appendReplacement(result, Matcher.quoteReplacement(value));
    }
    matcher.appendTail(result);
    
    return result.toString();
  }
  
  /**
   * Checks if the given string contains any environment variable placeholders.
   * 
   * @param input the input string to check
   * @return true if the string contains environment variable placeholders, false otherwise
   */
  public static boolean containsVariables(String input) {
    if (input == null) {
      return false;
    }
    return ENV_VAR_PATTERN.matcher(input).find();
  }
  
  /**
   * Substitutes environment variables in a JSON string.
   * This method handles the substitution within JSON value strings while preserving
   * the JSON structure and handling type conversions for numbers and booleans.
   * 
   * @param jsonString the JSON string containing environment variable placeholders
   * @return the JSON string with environment variables substituted
   * @throws IllegalArgumentException if an environment variable is not found and no default is provided
   */
  public static String substituteInJson(String jsonString) {
    // Create a combined map that includes both environment variables and system properties
    // System properties take precedence for testing purposes
    Map<String, String> combined = new HashMap<>(System.getenv());
    System.getProperties().forEach((k, v) -> combined.put(k.toString(), v.toString()));
    return substituteInJson(jsonString, combined);
  }
  
  /**
   * Substitutes environment variables in a JSON string using a custom environment map.
   * 
   * @param jsonString the JSON string containing environment variable placeholders
   * @param environment the environment variables map
   * @return the JSON string with environment variables substituted
   * @throws IllegalArgumentException if an environment variable is not found and no default is provided
   */
  public static String substituteInJson(String jsonString, Map<String, String> environment) {
    if (jsonString == null) {
      return null;
    }
    
    // Pattern to match JSON string values that contain ONLY a variable (for type conversion)
    Pattern standaloneVarPattern = Pattern.compile("\"(\\$\\{[^:}]+(?::[^}]*)?\\})\"");
    Matcher standaloneMatcher = standaloneVarPattern.matcher(jsonString);
    StringBuffer result = new StringBuffer();
    
    while (standaloneMatcher.find()) {
      String variable = standaloneMatcher.group(1);
      String value = substitute(variable, environment);
      
      // Determine if the value should be unquoted (number or boolean)
      if (isNumber(value) || isBoolean(value)) {
        // Replace with unquoted value for proper JSON type
        standaloneMatcher.appendReplacement(result, Matcher.quoteReplacement(value));
      } else {
        // Keep as quoted string, but escape properly
        value = value.replace("\\", "\\\\")
                    .replace("\"", "\\\"")
                    .replace("\n", "\\n")
                    .replace("\r", "\\r")
                    .replace("\t", "\\t");
        standaloneMatcher.appendReplacement(result, Matcher.quoteReplacement("\"" + value + "\""));
      }
    }
    standaloneMatcher.appendTail(result);
    
    // Second pass: substitute variables that are part of larger strings
    // Pattern to match JSON string values that might contain variables mixed with other text
    String intermediate = result.toString();
    Pattern mixedStringPattern = Pattern.compile("\"([^\"]*\\$\\{[^}]+\\}[^\"]*)\"");
    Matcher mixedMatcher = mixedStringPattern.matcher(intermediate);
    StringBuffer finalResult = new StringBuffer();
    
    while (mixedMatcher.find()) {
      String stringValue = mixedMatcher.group(1);
      // Check if this is just a plain variable (already handled in first pass)
      if (stringValue.matches("\\$\\{[^:}]+(?::[^}]*)?\\}")) {
        // Skip - already handled
        continue;
      }
      String substituted = substitute(stringValue, environment);
      // Escape any special characters for JSON
      substituted = substituted.replace("\\", "\\\\")
                              .replace("\"", "\\\"")
                              .replace("\n", "\\n")
                              .replace("\r", "\\r")
                              .replace("\t", "\\t");
      // Use Matcher.quoteReplacement to handle $ and \ in the replacement
      mixedMatcher.appendReplacement(finalResult, Matcher.quoteReplacement("\"" + substituted + "\""));
    }
    mixedMatcher.appendTail(finalResult);
    
    // If no mixed patterns were found, return the result from the first pass
    return finalResult.length() > 0 ? finalResult.toString() : intermediate;
  }
  
  /**
   * Checks if a string value represents a number.
   */
  private static boolean isNumber(String value) {
    if (value == null) {
      return false;
    }
    try {
      // Try parsing as integer first
      Long.parseLong(value);
      return true;
    } catch (NumberFormatException e1) {
      try {
        // Try parsing as double
        Double.parseDouble(value);
        return true;
      } catch (NumberFormatException e2) {
        return false;
      }
    }
  }
  
  /**
   * Checks if a string value represents a boolean.
   */
  private static boolean isBoolean(String value) {
    return "true".equalsIgnoreCase(value) || "false".equalsIgnoreCase(value);
  }
}