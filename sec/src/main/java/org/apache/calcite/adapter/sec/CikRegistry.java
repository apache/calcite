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
package org.apache.calcite.adapter.sec;

import com.fasterxml.jackson.databind.JsonNode;
import com.fasterxml.jackson.databind.ObjectMapper;

import java.io.File;
import java.io.IOException;
import java.io.InputStream;
import java.util.ArrayList;
import java.util.Arrays;
import java.util.Collections;
import java.util.HashMap;
import java.util.List;
import java.util.Map;
import java.util.logging.Logger;

/**
 * Registry of common CIK aliases loaded from cik-registry.json.
 *
 * <p>Supports:
 * <ul>
 *   <li>Stock tickers (AAPL, MSFT, etc.)</li>
 *   <li>Company groups (FAANG, MAGNIFICENT7, etc.)</li>
 *   <li>Custom registry files via environment variable or property</li>
 * </ul>
 *
 * <p>Custom registry file can be specified via:
 * <ul>
 *   <li>Environment variable: XBRL_CIK_REGISTRY</li>
 *   <li>System property: sec.cikRegistry or cikRegistry</li>
 * </ul>
 */
public class CikRegistry {
  private static final Logger LOGGER = Logger.getLogger(CikRegistry.class.getName());

  // Registry data loaded from file
  private static Map<String, String> TICKER_TO_CIK = new HashMap<>();
  private static Map<String, List<String>> GROUP_TO_CIKS = new HashMap<>();
  private static Map<String, String> GROUP_ALIASES = new HashMap<>();

  private static boolean initialized = false;

  static {
    // Load registry on first use
    loadRegistry();
  }

  /**
   * Load registry from file or defaults.
   */
  private static synchronized void loadRegistry() {
    if (initialized) return;

    try {
      // First, always load the default registry from classpath
      loadRegistryFromResource();
      LOGGER.fine("Loaded default CIK registry from classpath");

      // Then check for custom registry file to extend/override
      String registryPath = System.getenv("XBRL_CIK_REGISTRY");
      if (registryPath == null) {
        registryPath = System.getProperty("sec.cikRegistry");
      }
      if (registryPath == null) {
        registryPath = System.getProperty("cikRegistry");
      }

      if (registryPath != null && new File(registryPath).exists()) {
        // Load custom file to extend/override defaults
        loadCustomRegistryFromFile(new File(registryPath));
        LOGGER.info("Extended/overrode CIK registry with: " + registryPath);
      }

      initialized = true;
    } catch (Exception e) {
      LOGGER.warning("Failed to load CIK registry, using hardcoded defaults: " + e.getMessage());
      // Fall back to hardcoded values
      initializeHardcodedDefaults();
      initialized = true;
    }
  }

  /**
   * Load registry from classpath resource.
   */
  private static void loadRegistryFromResource() throws IOException {
    InputStream is = CikRegistry.class.getResourceAsStream("/cik-registry.json");
    if (is == null) {
      throw new IOException("CIK registry resource not found: /cik-registry.json");
    }

    try {
      ObjectMapper mapper = new ObjectMapper();
      JsonNode root = mapper.readTree(is);
      parseRegistry(root);
    } finally {
      is.close();
    }
  }

  /**
   * Load registry from external file (replaces all data).
   */
  private static void loadRegistryFromFile(File file) throws IOException {
    ObjectMapper mapper = new ObjectMapper();
    JsonNode root = mapper.readTree(file);
    parseRegistry(root);
  }

  /**
   * Load custom registry from external file (extends/overrides defaults).
   */
  private static void loadCustomRegistryFromFile(File file) throws IOException {
    ObjectMapper mapper = new ObjectMapper();
    JsonNode root = mapper.readTree(file);
    parseCustomRegistry(root);
  }

  /**
   * Parse registry JSON.
   */
  private static void parseRegistry(JsonNode root) {
    // Clear existing data
    TICKER_TO_CIK.clear();
    GROUP_TO_CIKS.clear();
    GROUP_ALIASES.clear();

    // Load tickers
    if (root.has("tickers")) {
      JsonNode tickers = root.get("tickers");
      tickers.fields().forEachRemaining(entry -> {
        TICKER_TO_CIK.put(entry.getKey().toUpperCase(), entry.getValue().asText());
      });
    }

    // Load groups (two passes to handle dependencies)
    if (root.has("groups")) {
      JsonNode groups = root.get("groups");

      // First pass: load aliases and raw CIK members
      groups.fields().forEachRemaining(entry -> {
        String groupName = entry.getKey();
        JsonNode groupData = entry.getValue();

        if (groupData.has("alias")) {
          // It's an alias
          GROUP_ALIASES.put(groupName, groupData.get("alias").asText());
        } else if (groupData.has("members")) {
          // It's a group definition - store raw for now
          List<String> members = new ArrayList<>();
          groupData.get("members").forEach(node -> members.add(node.asText()));
          GROUP_TO_CIKS.put(groupName, members);
        }
      });

      // Second pass: resolve any references in members
      Map<String, List<String>> resolvedGroups = new HashMap<>();
      GROUP_TO_CIKS.forEach((groupName, members) -> {
        List<String> resolvedMembers = new ArrayList<>();
        for (String member : members) {
          // Check if member is a ticker, group, or CIK
          List<String> resolved = resolveMember(member);
          resolvedMembers.addAll(resolved);
        }
        resolvedGroups.put(groupName, resolvedMembers);
      });
      GROUP_TO_CIKS.putAll(resolvedGroups);
    }

    // Load custom groups if present
    if (root.has("customGroups")) {
      JsonNode customGroups = root.get("customGroups");
      customGroups.fields().forEachRemaining(entry -> {
        if (!entry.getKey().startsWith("_")) { // Skip comments
          JsonNode groupData = entry.getValue();
          if (groupData.has("members")) {
            List<String> members = new ArrayList<>();
            groupData.get("members").forEach(node -> {
              // Resolve references in custom groups too
              List<String> resolved = resolveMember(node.asText());
              members.addAll(resolved);
            });
            GROUP_TO_CIKS.put(entry.getKey(), members);
          }
        }
      });
    }
  }

  /**
   * Resolve a member reference which could be a ticker, group, or CIK.
   */
  private static List<String> resolveMember(String member) {
    String upperMember = member.toUpperCase();

    // Check for special markers (don't resolve during initialization)
    // They will be resolved dynamically when accessed
    if (member.startsWith("_")) {
      return Arrays.asList(member);
    }

    // Check if it's a ticker
    if (TICKER_TO_CIK.containsKey(upperMember)) {
      return Arrays.asList(TICKER_TO_CIK.get(upperMember));
    }

    // Check if it's a group reference (avoid recursion)
    if (GROUP_TO_CIKS.containsKey(upperMember)) {
      List<String> groupMembers = GROUP_TO_CIKS.get(upperMember);
      // Only return if all members look like CIKs (start with 0)
      boolean allCiks = groupMembers.stream().allMatch(m -> m.startsWith("0"));
      if (allCiks) {
        return new ArrayList<>(groupMembers);
      }
    }

    // Check if it's a group alias
    if (GROUP_ALIASES.containsKey(upperMember)) {
      String actualGroup = GROUP_ALIASES.get(upperMember);
      if (GROUP_TO_CIKS.containsKey(actualGroup)) {
        return new ArrayList<>(GROUP_TO_CIKS.get(actualGroup));
      }
    }

    // Assume it's a raw CIK
    return Arrays.asList(member);
  }

  /**
   * Parse custom registry JSON (extends/overrides defaults).
   */
  private static void parseCustomRegistry(JsonNode root) {
    // Load/override tickers
    if (root.has("tickers")) {
      JsonNode tickers = root.get("tickers");
      tickers.fields().forEachRemaining(entry -> {
        String ticker = entry.getKey().toUpperCase();
        String cik = entry.getValue().asText();
        TICKER_TO_CIK.put(ticker, cik);
        LOGGER.fine("Added/overrode ticker: " + ticker + " -> " + cik);
      });
    }

    // Load/override groups
    if (root.has("groups")) {
      JsonNode groups = root.get("groups");
      groups.fields().forEachRemaining(entry -> {
        String groupName = entry.getKey();
        JsonNode groupData = entry.getValue();

        if (groupData.has("alias")) {
          // It's an alias
          GROUP_ALIASES.put(groupName, groupData.get("alias").asText());
          LOGGER.fine("Added/overrode group alias: " + groupName);
        } else if (groupData.has("members")) {
          // It's a group definition - resolve any references
          List<String> members = new ArrayList<>();
          groupData.get("members").forEach(node -> {
            String memberRef = node.asText();
            // Resolve ticker/group references to CIKs
            List<String> resolved = resolveCiks(memberRef);
            members.addAll(resolved);
          });
          GROUP_TO_CIKS.put(groupName, members);
          LOGGER.fine("Added/overrode group: " + groupName + " with " + members.size() + " members");
        }
      });
    }

    // Load custom groups if present
    if (root.has("customGroups")) {
      JsonNode customGroups = root.get("customGroups");
      customGroups.fields().forEachRemaining(entry -> {
        if (!entry.getKey().startsWith("_")) { // Skip comments
          JsonNode groupData = entry.getValue();
          if (groupData.has("members")) {
            List<String> members = new ArrayList<>();
            groupData.get("members").forEach(node -> {
              String memberRef = node.asText();
              // Resolve ticker/group references to CIKs
              List<String> resolved = resolveCiks(memberRef);
              members.addAll(resolved);
            });
            GROUP_TO_CIKS.put(entry.getKey(), members);
            LOGGER.fine("Added custom group: " + entry.getKey() + " with " + members.size() + " members");
          }
        }
      });
    }
  }

  /**
   * Initialize hardcoded defaults as fallback.
   */
  private static void initializeHardcodedDefaults() {
    // Basic tickers
    TICKER_TO_CIK.put("AAPL", "0000320193");
    TICKER_TO_CIK.put("MSFT", "0000789019");
    TICKER_TO_CIK.put("GOOGL", "0001652044");
    TICKER_TO_CIK.put("AMZN", "0001018724");
    TICKER_TO_CIK.put("META", "0001326801");
    TICKER_TO_CIK.put("TSLA", "0001318605");
    TICKER_TO_CIK.put("NVDA", "0001045810");

    // Basic groups
    GROUP_TO_CIKS.put(
        "FAANG", Arrays.asList(
        "0001326801", "0000320193", "0001018724", "0001065280", "0001652044"));

    GROUP_TO_CIKS.put(
        "MAGNIFICENT7", Arrays.asList(
        "0000320193", "0000789019", "0001652044", "0001018724",
        "0001326801", "0001318605", "0001045810"));
  }

  /**
   * Resolve a CIK identifier or alias to a list of actual CIKs.
   *
   * @param identifier Can be:
   *   - A raw CIK number (e.g., "0000320193")
   *   - A stock ticker (e.g., "AAPL")
   *   - A group alias (e.g., "FAANG", "MAGNIFICENT7")
   * @return List of resolved CIK numbers
   */
  public static List<String> resolveCiks(String identifier) {
    if (identifier == null || identifier.isEmpty()) {
      return Collections.emptyList();
    }

    // Ensure registry is loaded
    if (!initialized) {
      loadRegistry();
    }

    String upperIdentifier = identifier.toUpperCase();

    // Check if it's a group alias (including those starting with _)
    if (GROUP_ALIASES.containsKey(upperIdentifier)) {
      String actualGroup = GROUP_ALIASES.get(upperIdentifier);
      if (GROUP_TO_CIKS.containsKey(actualGroup)) {
        List<String> members = GROUP_TO_CIKS.get(actualGroup);
        // Check if the group contains special markers that need resolution
        List<String> resolved = new ArrayList<>();
        for (String member : members) {
          if (member.startsWith("_")) {
            // Resolve special marker dynamically
            List<String> markerCiks = handleSpecialMarker(member);
            resolved.addAll(markerCiks);
          } else {
            resolved.add(member);
          }
        }
        return resolved;
      }
    }

    // Check if it's a direct group
    if (GROUP_TO_CIKS.containsKey(upperIdentifier)) {
      List<String> members = GROUP_TO_CIKS.get(upperIdentifier);
      // Check if the group contains special markers that need resolution
      List<String> resolved = new ArrayList<>();
      for (String member : members) {
        if (member.startsWith("_")) {
          // Resolve special marker dynamically
          List<String> markerCiks = handleSpecialMarker(member);
          resolved.addAll(markerCiks);
        } else {
          resolved.add(member);
        }
      }
      return resolved;
    }

    // Check if it's a ticker symbol
    if (TICKER_TO_CIK.containsKey(upperIdentifier)) {
      return Arrays.asList(TICKER_TO_CIK.get(upperIdentifier));
    }

    // Check for special markers that require dynamic loading
    if (identifier.startsWith("_")) {
      return handleSpecialMarker(identifier);
    }

    // Assume it's a raw CIK
    return Arrays.asList(identifier);
  }

  /**
   * Handle special marker identifiers that require dynamic loading.
   * @param marker Special marker like _ALL_EDGAR_FILERS
   * @return List of CIKs or empty list with warning
   */
  private static List<String> handleSpecialMarker(String marker) {
    switch (marker) {
      case "_ALL_EDGAR_FILERS":
        LOGGER.info("Loading all EDGAR filers dynamically...");
        try {
          List<String> allCiks = SecDataFetcher.fetchAllEdgarFilers();
          if (!allCiks.isEmpty()) {
            LOGGER.info("Successfully loaded " + allCiks.size() + " CIKs from SEC EDGAR");
            return allCiks;
          }
        } catch (Exception e) {
          LOGGER.severe("Failed to fetch all EDGAR filers: " + e.getMessage());
        }
        LOGGER.warning("Returning empty list for " + marker);
        return Collections.emptyList();

      case "_SP500_CONSTITUENTS":
        LOGGER.info("Loading S&P 500 constituents dynamically...");
        try {
          List<String> sp500Ciks = SecDataFetcher.fetchSP500Constituents();
          if (!sp500Ciks.isEmpty()) {
            LOGGER.info("Successfully loaded " + sp500Ciks.size() + " S&P 500 CIKs");
            return sp500Ciks;
          }
        } catch (Exception e) {
          LOGGER.severe("Failed to fetch S&P 500 constituents: " + e.getMessage());
        }
        LOGGER.warning("Returning empty list for " + marker);
        return Collections.emptyList();

      case "_RUSSELL2000_CONSTITUENTS":
        LOGGER.info("Loading Russell 2000 constituents dynamically...");
        try {
          List<String> russell2000Ciks = SecDataFetcher.fetchRussell2000Constituents();
          if (!russell2000Ciks.isEmpty()) {
            LOGGER.info("Successfully loaded " + russell2000Ciks.size() + " Russell 2000 CIKs");
            return russell2000Ciks;
          }
        } catch (Exception e) {
          LOGGER.severe("Failed to fetch Russell 2000 constituents: " + e.getMessage());
        }
        LOGGER.warning("Returning empty list for " + marker);
        return Collections.emptyList();

      case "_RUSSELL1000_CONSTITUENTS":
        LOGGER.info("Loading Russell 1000 constituents dynamically...");
        try {
          List<String> russell1000Ciks = SecDataFetcher.fetchRussell1000Constituents();
          if (!russell1000Ciks.isEmpty()) {
            LOGGER.info("Successfully loaded " + russell1000Ciks.size() + " Russell 1000 CIKs");
            return russell1000Ciks;
          }
        } catch (Exception e) {
          LOGGER.severe("Failed to fetch Russell 1000 constituents: " + e.getMessage());
        }
        LOGGER.warning("Returning empty list for " + marker);
        return Collections.emptyList();

      case "_RUSSELL3000_CONSTITUENTS":
        LOGGER.info("Loading Russell 3000 constituents dynamically...");
        try {
          List<String> russell3000Ciks = SecDataFetcher.fetchRussell3000Constituents();
          if (!russell3000Ciks.isEmpty()) {
            LOGGER.info("Successfully loaded " + russell3000Ciks.size() + " Russell 3000 CIKs");
            return russell3000Ciks;
          }
        } catch (Exception e) {
          LOGGER.severe("Failed to fetch Russell 3000 constituents: " + e.getMessage());
        }
        LOGGER.warning("Returning empty list for " + marker);
        return Collections.emptyList();

      case "_DJI_CONSTITUENTS":
        LOGGER.info("Loading DJI constituents dynamically...");
        try {
          List<String> djiCiks = SecDataFetcher.fetchDJIConstituents();
          if (!djiCiks.isEmpty()) {
            LOGGER.info("Successfully loaded " + djiCiks.size() + " DJI CIKs");
            return djiCiks;
          }
        } catch (Exception e) {
          LOGGER.severe("Failed to fetch DJI constituents: " + e.getMessage());
        }
        LOGGER.warning("Returning empty list for " + marker);
        return Collections.emptyList();

      case "_NASDAQ100_CONSTITUENTS":
        LOGGER.info("Loading NASDAQ-100 constituents dynamically...");
        try {
          List<String> nasdaq100Ciks = SecDataFetcher.fetchNasdaq100Constituents();
          if (!nasdaq100Ciks.isEmpty()) {
            LOGGER.info("Successfully loaded " + nasdaq100Ciks.size() + " NASDAQ-100 CIKs");
            return nasdaq100Ciks;
          }
        } catch (Exception e) {
          LOGGER.severe("Failed to fetch NASDAQ-100 constituents: " + e.getMessage());
        }
        LOGGER.warning("Returning empty list for " + marker);
        return Collections.emptyList();

      case "_NYSE_LISTED":
        LOGGER.info("Loading NYSE-listed companies dynamically...");
        try {
          List<String> nyseCiks = SecDataFetcher.fetchNYSEListedCompanies();
          if (!nyseCiks.isEmpty()) {
            LOGGER.info("Successfully loaded " + nyseCiks.size() + " NYSE-listed CIKs");
            return nyseCiks;
          }
        } catch (Exception e) {
          LOGGER.severe("Failed to fetch NYSE-listed companies: " + e.getMessage());
        }
        LOGGER.warning("Returning empty list for " + marker);
        return Collections.emptyList();

      case "_NASDAQ_LISTED":
        LOGGER.info("Loading NASDAQ-listed companies dynamically...");
        try {
          List<String> nasdaqCiks = SecDataFetcher.fetchNASDAQListedCompanies();
          if (!nasdaqCiks.isEmpty()) {
            LOGGER.info("Successfully loaded " + nasdaqCiks.size() + " NASDAQ-listed CIKs");
            return nasdaqCiks;
          }
        } catch (Exception e) {
          LOGGER.severe("Failed to fetch NASDAQ-listed companies: " + e.getMessage());
        }
        LOGGER.warning("Returning empty list for " + marker);
        return Collections.emptyList();

      case "_WILSHIRE5000_CONSTITUENTS":
        LOGGER.info("Loading Wilshire 5000 constituents dynamically...");
        try {
          List<String> wilshire5000Ciks = SecDataFetcher.fetchWilshire5000Constituents();
          if (!wilshire5000Ciks.isEmpty()) {
            LOGGER.info("Successfully loaded " + wilshire5000Ciks.size() + " Wilshire 5000 CIKs");
            return wilshire5000Ciks;
          }
        } catch (Exception e) {
          LOGGER.severe("Failed to fetch Wilshire 5000 constituents: " + e.getMessage());
        }
        LOGGER.warning("Returning empty list for " + marker);
        return Collections.emptyList();

      case "_FTSE100_US_LISTED":
        LOGGER.info("Loading FTSE 100 companies with US listings dynamically...");
        try {
          List<String> ftse100Ciks = SecDataFetcher.fetchFTSE100USListed();
          if (!ftse100Ciks.isEmpty()) {
            LOGGER.info("Successfully loaded " + ftse100Ciks.size() + " FTSE 100 US-listed CIKs");
            return ftse100Ciks;
          }
        } catch (Exception e) {
          LOGGER.severe("Failed to fetch FTSE 100 US-listed companies: " + e.getMessage());
        }
        LOGGER.warning("Returning empty list for " + marker);
        return Collections.emptyList();

      case "_GLOBAL_MEGA_CAP":
        LOGGER.info("Loading global mega-cap companies (market cap > $200B) dynamically...");
        try {
          List<String> megaCapCiks = SecDataFetcher.fetchGlobalMegaCap();
          if (!megaCapCiks.isEmpty()) {
            LOGGER.info("Successfully loaded " + megaCapCiks.size() + " global mega-cap CIKs");
            return megaCapCiks;
          }
        } catch (Exception e) {
          LOGGER.severe("Failed to fetch global mega-cap companies: " + e.getMessage());
        }
        LOGGER.warning("Returning empty list for " + marker);
        return Collections.emptyList();

      case "_US_LARGE_CAP":
        LOGGER.info("Loading US large-cap companies (market cap > $10B) dynamically...");
        try {
          List<String> largeCapCiks = SecDataFetcher.fetchUSLargeCap();
          if (!largeCapCiks.isEmpty()) {
            LOGGER.info("Successfully loaded " + largeCapCiks.size() + " US large-cap CIKs");
            return largeCapCiks;
          }
        } catch (Exception e) {
          LOGGER.severe("Failed to fetch US large-cap companies: " + e.getMessage());
        }
        LOGGER.warning("Returning empty list for " + marker);
        return Collections.emptyList();

      case "_US_MID_CAP":
        LOGGER.info("Loading US mid-cap companies (market cap $2B-$10B) dynamically...");
        try {
          List<String> midCapCiks = SecDataFetcher.fetchUSMidCap();
          if (!midCapCiks.isEmpty()) {
            LOGGER.info("Successfully loaded " + midCapCiks.size() + " US mid-cap CIKs");
            return midCapCiks;
          }
        } catch (Exception e) {
          LOGGER.severe("Failed to fetch US mid-cap companies: " + e.getMessage());
        }
        LOGGER.warning("Returning empty list for " + marker);
        return Collections.emptyList();

      case "_US_SMALL_CAP":
        LOGGER.info("Loading US small-cap companies (market cap $300M-$2B) dynamically...");
        try {
          List<String> smallCapCiks = SecDataFetcher.fetchUSSmallCap();
          if (!smallCapCiks.isEmpty()) {
            LOGGER.info("Successfully loaded " + smallCapCiks.size() + " US small-cap CIKs");
            return smallCapCiks;
          }
        } catch (Exception e) {
          LOGGER.severe("Failed to fetch US small-cap companies: " + e.getMessage());
        }
        LOGGER.warning("Returning empty list for " + marker);
        return Collections.emptyList();

      case "_US_MICRO_CAP":
        LOGGER.info("Loading US micro-cap companies (market cap < $300M) dynamically...");
        try {
          List<String> microCapCiks = SecDataFetcher.fetchUSMicroCap();
          if (!microCapCiks.isEmpty()) {
            LOGGER.info("Successfully loaded " + microCapCiks.size() + " US micro-cap CIKs");
            return microCapCiks;
          }
        } catch (Exception e) {
          LOGGER.severe("Failed to fetch US micro-cap companies: " + e.getMessage());
        }
        LOGGER.warning("Returning empty list for " + marker);
        return Collections.emptyList();

      default:
        LOGGER.warning("Unknown special marker: " + marker);
        return Collections.emptyList();
    }
  }

  /**
   * Set a custom registry file path and reload.
   */
  public static void setCustomRegistryPath(String path) {
    System.setProperty("sec.cikRegistry", path);
    initialized = false;
    loadRegistry();
  }

  /**
   * Get all available group aliases.
   */
  public static List<String> getAvailableGroups() {
    if (!initialized) {
      loadRegistry();
    }
    List<String> groups = new ArrayList<>();
    groups.addAll(GROUP_TO_CIKS.keySet());
    groups.addAll(GROUP_ALIASES.keySet());
    Collections.sort(groups);
    return groups;
  }

  /**
   * Get all available ticker symbols.
   */
  public static List<String> getAvailableTickers() {
    if (!initialized) {
      loadRegistry();
    }
    List<String> tickers = new ArrayList<>(TICKER_TO_CIK.keySet());
    Collections.sort(tickers);
    return tickers;
  }

  /**
   * Reload the registry (useful after changing custom registry path).
   */
  public static void reload() {
    initialized = false;
    loadRegistry();
  }
}
