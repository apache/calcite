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
package org.apache.calcite.adapter.file.util;

import java.util.Collections;
import java.util.HashMap;
import java.util.Locale;
import java.util.Map;
import java.util.concurrent.ConcurrentHashMap;
import java.util.regex.Matcher;
import java.util.regex.Pattern;

/**
 * Utility class for intelligent PostgreSQL-style snake_case conversion.
 *
 * <p>This class provides smart conversion from camelCase/PascalCase to snake_case
 * using a comprehensive dictionary of known acronyms and product names to avoid
 * over-segmentation and maintain readability.
 *
 * <p>Examples:
 * <ul>
 *   <li>XMLHttpRequest → xml_http_request</li>
 *   <li>PostgreSQLDatabase → postgresql_database</li>
 *   <li>OAuth2Token → oauth2_token</li>
 *   <li>JSONAPIResponse → jsonapi_response</li>
 * </ul>
 */
public final class SmartCasing {

  /**
   * Default dictionary with common term mappings.
   * Maps input terms to their preferred snake_case output.
   */
  private static final Map<String, String> DEFAULT_MAPPINGS;

  static {
    Map<String, String> defaults = new java.util.HashMap<>();

    // Programming & Web Technologies
    defaults.put("API", "api");
    defaults.put("REST", "rest");
    defaults.put("SOAP", "soap");
    defaults.put("HTTP", "http");
    defaults.put("HTTPS", "https");
    defaults.put("HTTP2", "http2");
    defaults.put("FTP", "ftp");
    defaults.put("SFTP", "sftp");
    defaults.put("SSH", "ssh");
    defaults.put("SSL", "ssl");
    defaults.put("TLS", "tls");
    defaults.put("JSON", "json");
    defaults.put("XML", "xml");
    defaults.put("HTML", "html");
    defaults.put("CSS", "css");
    defaults.put("JS", "js");
    defaults.put("TS", "ts");
    defaults.put("SQL", "sql");
    defaults.put("NoSQL", "nosql");
    defaults.put("OAuth", "oauth");
    defaults.put("OAuth2", "oauth2");
    defaults.put("JWT", "jwt");
    defaults.put("SAML", "saml");
    defaults.put("LDAP", "ldap");
    defaults.put("AD", "ad");
    defaults.put("SSO", "sso");
    defaults.put("URL", "url");
    defaults.put("URI", "uri");
    defaults.put("UUID", "uuid");
    defaults.put("GUID", "guid");
    defaults.put("ID", "id");
    defaults.put("IDs", "ids");

    // Database Technologies
    defaults.put("MySQL", "mysql");
    defaults.put("PostgreSQL", "postgresql");
    defaults.put("MongoDB", "mongodb");
    defaults.put("Redis", "redis");
    defaults.put("SQLite", "sqlite");
    defaults.put("Oracle", "oracle");
    defaults.put("MSSQL", "mssql");
    defaults.put("DB2", "db2");
    defaults.put("JDBC", "jdbc");
    defaults.put("ODBC", "odbc");
    defaults.put("ORM", "orm");
    defaults.put("DDL", "ddl");
    defaults.put("DML", "dml");
    defaults.put("ACID", "acid");

    // Cloud & Infrastructure
    defaults.put("AWS", "aws");
    defaults.put("GCP", "gcp");
    defaults.put("Azure", "azure");
    defaults.put("IBM", "ibm");
    defaults.put("Salesforce", "salesforce");
    defaults.put("S3", "s3");
    defaults.put("EC2", "ec2");
    defaults.put("RDS", "rds");
    defaults.put("VPC", "vpc");
    defaults.put("IAM", "iam");
    defaults.put("CDN", "cdn");
    defaults.put("DNS", "dns");
    defaults.put("VPN", "vpn");
    defaults.put("Docker", "docker");
    defaults.put("K8s", "k8s");
    defaults.put("Kubernetes", "kubernetes");
    defaults.put("OpenShift", "openshift");
    defaults.put("CI", "ci");
    defaults.put("CD", "cd");
    defaults.put("DevOps", "devops");
    defaults.put("MLOps", "mlops");

    // File Formats & Protocols
    defaults.put("CSV", "csv");
    defaults.put("TSV", "tsv");
    defaults.put("XLSX", "xlsx");
    defaults.put("PDF", "pdf");
    defaults.put("DOCX", "docx");
    defaults.put("PPTX", "pptx");
    defaults.put("ZIP", "zip");
    defaults.put("GZIP", "gzip");
    defaults.put("TAR", "tar");
    defaults.put("RAR", "rar");
    defaults.put("MIME", "mime");
    defaults.put("BASE64", "base64");
    defaults.put("UTF8", "utf8");
    defaults.put("ASCII", "ascii");

    // Programming Languages & Frameworks
    defaults.put("Java", "java");
    defaults.put("Python", "python");
    defaults.put("JavaScript", "javascript");
    defaults.put("TypeScript", "typescript");
    defaults.put("Golang", "golang");
    defaults.put("Rust", "rust");
    defaults.put("Scala", "scala");
    defaults.put("Kotlin", "kotlin");
    defaults.put("Spring", "spring");
    defaults.put("React", "react");
    defaults.put("Angular", "angular");
    defaults.put("Vue", "vue");
    defaults.put("Node", "node");
    defaults.put("Express", "express");
    defaults.put("JVM", "jvm");
    defaults.put("JRE", "jre");
    defaults.put("JDK", "jdk");
    defaults.put("NPM", "npm");
    defaults.put("YARN", "yarn");
    defaults.put("Maven", "maven");
    defaults.put("Gradle", "gradle");

    // Business & Enterprise
    defaults.put("CRM", "crm");
    defaults.put("ERP", "erp");
    defaults.put("HR", "hr");
    defaults.put("IT", "it");
    defaults.put("UI", "ui");
    defaults.put("UX", "ux");
    defaults.put("QA", "qa");
    defaults.put("QE", "qe");
    defaults.put("CEO", "ceo");
    defaults.put("CTO", "cto");
    defaults.put("CIO", "cio");
    defaults.put("CFO", "cfo");
    defaults.put("COO", "coo");

    // Common compound terms with specific mappings
    defaults.put("GraphQL", "graphql");
    defaults.put("GitHub", "github");
    defaults.put("GitLab", "gitlab");
    defaults.put("LinkedIn", "linkedin");

    DEFAULT_MAPPINGS = Collections.unmodifiableMap(defaults);
  }

  /**
   * User-configured additional term mappings.
   */
  private static final Map<String, String> USER_ADDITIONS = new ConcurrentHashMap<>();

  /**
   * User-configured override mappings (override defaults).
   */
  private static final Map<String, String> USER_OVERRIDES = new ConcurrentHashMap<>();

  /**
   * Pattern to match word boundaries in camelCase/PascalCase strings.
   * Matches transitions from lowercase to uppercase, sequences of uppercase letters
   * followed by lowercase, and digits.
   */
  private static final Pattern WORD_BOUNDARY_PATTERN =
      Pattern.compile("(?<=[a-z])(?=[A-Z])|" +     // camelCase boundary: lowercase to uppercase
      "(?<=[A-Z])(?=[A-Z][a-z])|" + // PascalCase boundary: uppercase sequence to mixed case
      "(?<=[a-zA-Z])(?=[0-9])|" +   // letter to digit
      "(?<=[0-9])(?=[a-zA-Z])|" +   // digit to letter
      "(?<=[0-9])(?=[0-9])");       // digit to digit (for version numbers like TLS12 -> TLS1_2

  private SmartCasing() {
    // Utility class - prevent instantiation
  }

  /**
   * Adds additional term mappings to the smart casing dictionary.
   * These extend the default mappings without overriding them.
   *
   * @param termMappings Map of input terms to their desired output forms
   */
  public static void addTermMappings(Map<String, String> termMappings) {
    if (termMappings != null) {
      USER_ADDITIONS.putAll(termMappings);
    }
  }

  /**
   * Adds term mappings that override the default dictionary.
   * Use this when you need different behavior than the built-in defaults.
   *
   * @param termMappings Map of input terms to their desired output forms
   */
  public static void overrideTermMappings(Map<String, String> termMappings) {
    if (termMappings != null) {
      USER_OVERRIDES.putAll(termMappings);
    }
  }

  /**
   * Clears all user-configured additional term mappings.
   */
  public static void clearAdditionalTerms() {
    USER_ADDITIONS.clear();
  }

  /**
   * Clears all user-configured override mappings.
   */
  public static void clearOverrides() {
    USER_OVERRIDES.clear();
  }

  /**
   * Clears all user-configured mappings (both additions and overrides).
   */
  public static void clearAllUserMappings() {
    USER_ADDITIONS.clear();
    USER_OVERRIDES.clear();
    TERM_CACHE.get().clear();
    PLACEHOLDER_COUNTER.set(0);
  }

  /**
   * Converts a string to PostgreSQL-style snake_case using intelligent segmentation.
   *
   * @param input The input string in camelCase, PascalCase, or mixed format
   * @return The converted snake_case string, or null if input is null
   */
  public static String toSnakeCase(String input) {
    if (input == null || input.isEmpty()) {
      return input;
    }

    // Check if there's an exact mapping for the entire input
    String exactMapping = getTermMapping(input);
    if (exactMapping != null) {
      return exactMapping;
    }

    // Special handling for simple compounds with preserved terms
    // If input ends with a preserved term and has only 2 parts, consider preserving the entire input
    if (hasSimplePreservedTermSuffix(input)) {
      return input;
    }

    // If input already contains underscores, process each part separately
    if (input.contains("_")) {
      String[] parts = input.split("_");
      StringBuilder result = new StringBuilder();
      for (int i = 0; i < parts.length; i++) {
        if (i > 0) {
          result.append("_");
        }
        // Process each part through snake case conversion
        result.append(toSnakeCase(parts[i]));
      }
      return result.toString();
    }

    // Special case: if the input is all uppercase and not an acronym, just convert to lowercase
    // This handles table names like DEPTS, EMPS that shouldn't be split
    if (isAllUpperCase(input) && !isKnownAcronym(input)) {
      return input.toLowerCase(Locale.ROOT);
    }

    // First, handle known terms by protecting them from segmentation
    String processed = protectKnownTerms(input);

    // Split on word boundaries, but be careful with placeholders
    java.util.List<String> parts = splitWithPlaceholderProtection(processed);

    StringBuilder result = new StringBuilder();
    for (int i = 0; i < parts.size(); i++) {
      String part = parts.get(i);

      // Skip empty parts
      if (part.isEmpty()) {
        continue;
      }

      // Restore the current part to determine underscore logic
      String currentRestored = restoreKnownTerms(part);

      // Determine if we should add an underscore before this part
      boolean shouldAddUnderscore = false;
      if (i > 0 && result.length() > 0) {
        String previousResult = result.toString();

        // Check if current part is a preserved term by checking the placeholder cache
        boolean isPreservedTerm = false;
        if (part.matches("§§\\d+§§")) {
          // This is a placeholder, check what it maps to
          String placeholderValue = TERM_CACHE.get().get(part);
          if (placeholderValue != null) {
            // Check if the mapping preserves the original (find the key that maps to this value)
            String termMapping = getTermMapping(placeholderValue);
            isPreservedTerm = termMapping != null && termMapping.equals(placeholderValue);
          }
        } else {
          // This is regular text, check if it maps to itself
          String termMapping = getTermMapping(part);
          isPreservedTerm = termMapping != null && termMapping.equals(part);
        }

        // Don't add underscore if:
        // 1. Previous part ends with a letter and current part is a single digit
        // 2. Both parts are single digits (for version numbers like TLS12 -> tls1_2 not tls_1_2)
        // 3. Current part is a preserved term that maintains original casing
        char lastChar = previousResult.charAt(previousResult.length() - 1);
        boolean prevEndsWithLetter = Character.isLetter(lastChar);
        boolean currIsSingleDigit = currentRestored.length() == 1 && Character.isDigit(currentRestored.charAt(0));
        boolean prevIsSingleDigit = previousResult.length() == 1 && Character.isDigit(previousResult.charAt(0));

        if (prevEndsWithLetter && currIsSingleDigit) {
          // Letter followed by digit: no underscore (TLS1 not TLS_1)
          shouldAddUnderscore = false;
        } else if (prevIsSingleDigit && currIsSingleDigit) {
          // Digit followed by digit: add underscore (1_2 not 12)
          shouldAddUnderscore = true;
        } else if (isPreservedTerm && Character.isLowerCase(lastChar) && Character.isUpperCase(currentRestored.charAt(0)) && parts.size() == 2) {
          // If current term is preserved, we have camelCase boundary, and it's a simple 2-part compound, don't add underscore
          // This handles cases like userID -> userID (not user_ID)
          shouldAddUnderscore = false;
        } else {
          // Default case: add underscore
          shouldAddUnderscore = true;
        }
      }

      if (shouldAddUnderscore) {
        result.append('_');
      }

      // Add the already restored current part
      result.append(currentRestored);
    }

    return result.toString();
  }

  /**
   * Checks if the input is a simple compound ending with a preserved term.
   * This handles cases like userID -> userID when ID maps to ID.
   */
  private static boolean hasSimplePreservedTermSuffix(String input) {
    // Split the input to see if it has exactly 2 parts
    String[] parts = WORD_BOUNDARY_PATTERN.split(input);
    if (parts.length != 2) {
      return false;
    }

    // Check if the second part is preserved (maps to itself)
    String secondPart = parts[1];
    String mapping = getTermMapping(secondPart);
    if (mapping != null && mapping.equals(secondPart)) {
      // The suffix is preserved, so preserve the entire compound
      return true;
    }

    return false;
  }

  /**
   * Checks if a string is all uppercase letters.
   */
  private static boolean isAllUpperCase(String input) {
    for (char c : input.toCharArray()) {
      if (!Character.isUpperCase(c) && Character.isLetter(c)) {
        return false;
      }
    }
    return true;
  }

  /**
   * Checks if the input is a known acronym (exists in our dictionary).
   */
  private static boolean isKnownAcronym(String input) {
    return getTermMapping(input) != null;
  }

  /**
   * Gets the mapping for a term using the layered dictionary system.
   * Priority: USER_OVERRIDES > USER_ADDITIONS > DEFAULT_MAPPINGS
   */
  private static String getTermMapping(String term) {
    // Check overrides first
    if (USER_OVERRIDES.containsKey(term)) {
      return USER_OVERRIDES.get(term);
    }

    // Then check user additions
    if (USER_ADDITIONS.containsKey(term)) {
      return USER_ADDITIONS.get(term);
    }

    // Finally check defaults
    if (DEFAULT_MAPPINGS.containsKey(term)) {
      return DEFAULT_MAPPINGS.get(term);
    }

    return null;
  }

  /**
   * Protects known terms using a greedy longest-match algorithm.
   * This approach handles consecutive acronyms better by finding the best segmentation.
   */
  private static String protectKnownTerms(String input) {
    // Clear previous cache to ensure clean state for this conversion
    TERM_CACHE.get().clear();

    // Get all known terms from the layered dictionary
    Map<String, String> allMappings = new HashMap<>();
    allMappings.putAll(DEFAULT_MAPPINGS);
    allMappings.putAll(USER_ADDITIONS);
    allMappings.putAll(USER_OVERRIDES); // overrides win

    // Reset placeholder counter for this call to ensure consistent placeholders
    int startCounter = 0;
    PLACEHOLDER_COUNTER.set(startCounter);

    // Use greedy longest-match approach with term keys
    return protectTermsGreedy(input, allMappings.keySet(), startCounter);
  }

  /**
   * Recursive greedy longest-match algorithm for term protection.
   * Only matches terms at proper word boundaries to avoid false matches.
   */
  private static String protectTermsGreedy(String input, java.util.Set<String> terms, int placeholderCounter) {
    if (input.isEmpty()) {
      return input;
    }

    // Find the longest matching term at the current position that respects word boundaries
    String longestMatch = null;
    int longestLength = 0;

    for (String term : terms) {
      if (input.length() >= term.length()) {
        String prefix = input.substring(0, term.length());
        // Only match if the cases match exactly OR if we're at the start of the input
        // This prevents "form" from matching "ORM" in "Platform"
        if (prefix.equals(term) && isValidTermBoundary(input, term.length())) {
          if (term.length() > longestLength) {
            longestMatch = term;
            longestLength = term.length();
          }
        }
      }
    }

    if (longestMatch != null) {
      // Replace the matched term with a placeholder and store its mapping
      String placeholderKey = "§§" + placeholderCounter + "§§";
      String termMapping = getTermMapping(longestMatch);
      if (termMapping != null) {
        TERM_CACHE.get().put(placeholderKey, termMapping);
      } else {
        // Fallback to lowercase if no mapping found
        TERM_CACHE.get().put(placeholderKey, longestMatch.toLowerCase(Locale.ROOT));
      }

      // Recursively process the rest of the string
      String remaining = input.substring(longestLength);
      return placeholderKey + protectTermsGreedy(remaining, terms, placeholderCounter + 1);
    } else {
      // No match found, take the first character and continue
      char firstChar = input.charAt(0);
      String remaining = input.substring(1);
      return firstChar + protectTermsGreedy(remaining, terms, placeholderCounter);
    }
  }

  /**
   * Checks if a term match at the current position respects word boundaries.
   * This prevents matching terms in the middle of words (like "IDE" in "Provider").
   */
  private static boolean isValidTermBoundary(String input, int termLength) {
    if (termLength >= input.length()) {
      // Term extends to end of input - valid
      return true;
    }

    char nextChar = input.charAt(termLength);

    // For valid term boundaries, the next character should indicate a word break:
    // - Uppercase letter (camelCase/PascalCase boundary)
    // - Digit (version numbers, etc.)
    // - Non-letter character (punctuation, whitespace, etc.)
    //
    // Lowercase letters after a term usually indicate the term is part of a larger word
    return Character.isUpperCase(nextChar) || Character.isDigit(nextChar) || !Character.isLetter(nextChar);
  }

  /**
   * Thread-local cache to store placeholder-to-term mappings during processing.
   * Each thread gets its own cache to avoid interference.
   */
  private static final ThreadLocal<java.util.Map<String, String>> TERM_CACHE =
      ThreadLocal.withInitial(java.util.HashMap::new);

  /**
   * Thread-local counter for generating unique placeholders.
   * Each thread gets its own counter to avoid conflicts.
   */
  private static final ThreadLocal<Integer> PLACEHOLDER_COUNTER =
      ThreadLocal.withInitial(() -> 0);

  /**
   * Restores protected terms from placeholders using cached mappings.
   */
  private static String restoreKnownTerms(String input) {
    String result = input;

    // Get the thread-local cache
    Map<String, String> cache = TERM_CACHE.get();

    // Sort by placeholder key length (longest first) to avoid partial replacements
    java.util.List<java.util.Map.Entry<String, String>> sortedEntries =
        new java.util.ArrayList<>(cache.entrySet());
    sortedEntries.sort((a, b) -> Integer.compare(b.getKey().length(), a.getKey().length()));

    for (java.util.Map.Entry<String, String> entry : sortedEntries) {
      if (result.contains(entry.getKey())) {
        result = result.replace(entry.getKey(), entry.getValue());
      }
    }

    // If no placeholders were found, convert to lowercase as fallback
    if (result.equals(input) && !result.matches(".*§§\\d+§§.*")) {
      result = result.toLowerCase(Locale.ROOT);
    }

    return result;
  }

  /**
   * Splits a string on word boundaries while protecting placeholders from being split.
   */
  private static java.util.List<String> splitWithPlaceholderProtection(String input) {
    java.util.List<String> parts = new java.util.ArrayList<>();

    // Pattern to match placeholders (support both old and new formats)
    Pattern placeholderPattern = Pattern.compile("§§\\d+(?:_\\d+)?§§");
    Matcher placeholderMatcher = placeholderPattern.matcher(input);

    int lastEnd = 0;
    while (placeholderMatcher.find()) {
      // Add parts before the placeholder
      String beforePlaceholder = input.substring(lastEnd, placeholderMatcher.start());
      if (!beforePlaceholder.isEmpty()) {
        addWordBoundaryParts(beforePlaceholder, parts);
      }

      // Add the placeholder as a single part
      parts.add(placeholderMatcher.group());
      lastEnd = placeholderMatcher.end();
    }

    // Add remaining parts after the last placeholder
    if (lastEnd < input.length()) {
      String afterPlaceholder = input.substring(lastEnd);
      if (!afterPlaceholder.isEmpty()) {
        addWordBoundaryParts(afterPlaceholder, parts);
      }
    }

    // If no placeholders were found, split the entire string
    if (parts.isEmpty()) {
      addWordBoundaryParts(input, parts);
    }

    return parts;
  }

  /**
   * Splits a string on word boundaries and adds the parts to the list.
   */
  private static void addWordBoundaryParts(String input, java.util.List<String> parts) {
    String[] splitParts = WORD_BOUNDARY_PATTERN.split(input);
    for (String part : splitParts) {
      if (!part.isEmpty()) {
        parts.add(part);
      }
    }
  }

  /**
   * Applies column name casing transformation based on the specified strategy.
   *
   * @param name The column name to transform
   * @param casing The casing strategy: "UPPER", "LOWER", "UNCHANGED", or "SMART_CASING"
   * @return The transformed column name
   */
  public static String applyCasing(String name, String casing) {
    if (name == null) {
      return null;
    }

    // First sanitize to convert hyphens to underscores
    String sanitized = org.apache.calcite.adapter.file.converters.ConverterUtils.sanitizeIdentifier(name);

    switch (casing) {
    case "UPPER":
      return sanitized.toUpperCase(Locale.ROOT);
    case "LOWER":
      return sanitized.toLowerCase(Locale.ROOT);
    case "SMART_CASING":
      return toSnakeCase(sanitized);
    case "UNCHANGED":
    default:
      return sanitized;
    }
  }
}
