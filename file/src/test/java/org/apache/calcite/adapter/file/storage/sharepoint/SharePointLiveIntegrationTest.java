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

import org.apache.calcite.adapter.file.storage.MicrosoftGraphStorageProvider;
import org.apache.calcite.adapter.file.storage.MicrosoftGraphTokenManager;
import org.apache.calcite.adapter.file.storage.StorageProvider;
import org.apache.calcite.jdbc.CalciteConnection;
import org.apache.calcite.schema.Schema;
import org.apache.calcite.schema.SchemaPlus;

import org.junit.jupiter.api.BeforeAll;
import org.junit.jupiter.api.Tag;
import org.junit.jupiter.api.Test;

import java.io.ByteArrayInputStream;
import java.io.File;
import java.io.FileInputStream;
import java.io.IOException;
import java.io.InputStream;
import java.nio.charset.StandardCharsets;
import java.sql.Connection;
import java.sql.DriverManager;
import java.sql.ResultSet;
import java.sql.ResultSetMetaData;
import java.sql.Statement;
import java.util.ArrayList;
import java.util.HashMap;
import java.util.List;
import java.util.Locale;
import java.util.Map;
import java.util.Properties;

import static org.junit.jupiter.api.Assertions.*;

/**
 * Live integration test for SharePoint with Calcite using Microsoft Graph API.
 * Tests the full stack: Azure AD auth → Microsoft Graph → Calcite queries.
 */
@Tag("integration")
public class SharePointLiveIntegrationTest {

  private static String tenantId;
  private static String clientId;
  private static String clientSecret;
  private static String siteUrl;
  private static MicrosoftGraphTokenManager tokenManager;

  @BeforeAll
  static void setup() {
    // Load properties from local-test.properties file first
    Properties localProps = loadLocalProperties();

    tenantId = getRequiredConfig("SHAREPOINT_TENANT_ID", localProps);
    clientId = getRequiredConfig("SHAREPOINT_CLIENT_ID", localProps);
    clientSecret = getRequiredConfig("SHAREPOINT_CLIENT_SECRET", localProps);
    siteUrl = getRequiredConfig("SHAREPOINT_SITE_URL", localProps);

    // Create token manager
    tokenManager = new MicrosoftGraphTokenManager(tenantId, clientId, clientSecret, siteUrl);

    System.out.println("SharePoint Integration Test Setup:");
    System.out.println("  Site: " + siteUrl);
    System.out.println("  Client ID: " + clientId);
  }

  private static Properties loadLocalProperties() {
    Properties props = new Properties();
    File propsFile = new File("local-test.properties");
    if (!propsFile.exists()) {
      // Try relative to file module
      propsFile = new File("file/local-test.properties");
    }
    if (!propsFile.exists()) {
      // Try calcite/file path
      propsFile = new File("calcite/file/local-test.properties");
    }
    if (!propsFile.exists()) {
      // Try absolute path
      propsFile = new File("/Users/kennethstott/ndc-calcite/calcite-rs-jni/calcite/file/local-test.properties");
    }

    if (propsFile.exists()) {
      try (InputStream is = new FileInputStream(propsFile)) {
        props.load(is);
        System.out.println("Loaded properties from: " + propsFile.getAbsolutePath());
      } catch (IOException e) {
        System.err.println("Failed to load properties file: " + e.getMessage());
      }
    }
    return props;
  }

  private static String getRequiredConfig(String key, Properties localProps) {
    // Try environment variable first
    String value = System.getenv(key);
    if (value == null || value.isEmpty()) {
      // Try system property
      value = System.getProperty(key);
    }
    if (value == null || value.isEmpty()) {
      // Try local properties file
      value = localProps.getProperty(key);
    }
    if (value == null || value.isEmpty()) {
      throw new IllegalStateException(key + " must be set in environment, system properties, or local-test.properties");
    }
    return value;
  }

  @Test void testSharePointDirectAccess() throws Exception {
    // Test direct SharePoint access
    MicrosoftGraphStorageProvider provider = new MicrosoftGraphStorageProvider(tokenManager);

    // List root directory first
    System.out.println("=== Listing root directory ===");
    List<StorageProvider.FileEntry> rootEntries = provider.listFiles("/", false);
    System.out.println("Found " + rootEntries.size() + " items at root:");
    for (StorageProvider.FileEntry entry : rootEntries) {
      String type = entry.isDirectory() ? "[DIR]" : "[FILE]";
      System.out.println("  " + type + " " + entry.getName() + " (path: " + entry.getPath() + ")");
    }

    // List Shared Documents
    System.out.println("\n=== Listing /Shared Documents ===");
    List<StorageProvider.FileEntry> entries = provider.listFiles("/Shared Documents", false);
    assertNotNull(entries);
    System.out.println("Found " + entries.size() + " items in /Shared Documents:");

    // List all files with their types and look for subdirectories
    List<StorageProvider.FileEntry> directories = new ArrayList<>();
    for (StorageProvider.FileEntry entry : entries) {
      String type = entry.isDirectory() ? "[DIR]" : "[FILE]";
      String extension = "";
      if (!entry.isDirectory() && entry.getName().contains(".")) {
        extension = entry.getName().substring(entry.getName().lastIndexOf("."));
      }
      System.out.println("  " + type + " " + entry.getName() +
          (extension.isEmpty() ? "" : " (type: " + extension + ", size: " + entry.getSize() + " bytes)") +
          " (path: " + entry.getPath() + ")");

      if (entry.isDirectory()) {
        directories.add(entry);
      }
    }

    // Traverse into each subdirectory to look for CSV files
    List<StorageProvider.FileEntry> allCsvFiles = new ArrayList<>();
    for (StorageProvider.FileEntry dir : directories) {
      System.out.println("\n=== Listing inside " + dir.getName() + " folder (path: " + dir.getPath() + ") ===");
      try {
        List<StorageProvider.FileEntry> innerEntries = provider.listFiles(dir.getPath(), false);
        System.out.println("Found " + innerEntries.size() + " items inside " + dir.getName() + ":");

        for (StorageProvider.FileEntry entry : innerEntries) {
          String type = entry.isDirectory() ? "[DIR]" : "[FILE]";
          String extension = "";
          if (!entry.isDirectory() && entry.getName().contains(".")) {
            extension = entry.getName().substring(entry.getName().lastIndexOf("."));
          }
          System.out.println("  " + type + " " + entry.getName() +
              (extension.isEmpty() ? "" : " (type: " + extension + ", size: " + entry.getSize() + " bytes)") +
              " (path: " + entry.getPath() + ")");

          // Collect CSV files
          if (!entry.isDirectory() && entry.getName().toLowerCase(Locale.ROOT).endsWith(".csv")) {
            allCsvFiles.add(entry);
            System.out.println("    ^-- Found CSV file!");
          }

          // Traverse into any subdirectory, especially "Shared Documents" or "Documents"
          if (entry.isDirectory()) {
            System.out.println("\n    === Found subdirectory: " + entry.getName() + ", listing contents ===");
            try {
              List<StorageProvider.FileEntry> subEntries = provider.listFiles(entry.getPath(), false);
              System.out.println("    Found " + subEntries.size() + " items in " + entry.getName() + ":");
              for (StorageProvider.FileEntry subEntry : subEntries) {
                String subType = subEntry.isDirectory() ? "[DIR]" : "[FILE]";
                String subExt = "";
                if (!subEntry.isDirectory() && subEntry.getName().contains(".")) {
                  subExt = subEntry.getName().substring(subEntry.getName().lastIndexOf("."));
                }
                System.out.println("      " + subType + " " + subEntry.getName() +
                    (subExt.isEmpty() ? "" : " (type: " + subExt + ", size: " + subEntry.getSize() + " bytes)"));

                if (!subEntry.isDirectory() && subEntry.getName().toLowerCase(Locale.ROOT).endsWith(".csv")) {
                  allCsvFiles.add(subEntry);
                  System.out.println("        ^-- Found CSV file in " + entry.getName() + "!");
                }
              }
            } catch (Exception e) {
              System.out.println("    Error accessing " + entry.getName() + ": " + e.getMessage());
            }
          }
        }
      } catch (Exception e) {
        System.out.println("Error accessing " + dir.getName() + ": " + e.getMessage());
      }
    }

    // Try specific paths that might exist
    System.out.println("\n=== Trying specific paths ===");
    String[] pathsToTry = {
        "/Shared Documents/Shared Documents",  // The nested structure from the URL
        "Shared Documents/Shared Documents",
        "/Shared Documents/Documents",
        "/Documents",
        "Shared Documents/Documents",
        "Documents"
    };

    for (String path : pathsToTry) {
      System.out.println("Trying path: " + path);
      try {
        List<StorageProvider.FileEntry> pathEntries = provider.listFiles(path, false);
        System.out.println("  Success! Found " + pathEntries.size() + " items");
        for (StorageProvider.FileEntry entry : pathEntries) {
          if (!entry.isDirectory() && entry.getName().toLowerCase(Locale.ROOT).endsWith(".csv")) {
            allCsvFiles.add(entry);
            System.out.println("    [CSV] " + entry.getName() + " (size: " + entry.getSize() + " bytes)");
          }
        }
      } catch (Exception e) {
        System.out.println("  Failed: " + e.getMessage());
      }
    }

    // Report all CSV files found
    if (!allCsvFiles.isEmpty()) {
      System.out.println("\n=== Summary: Found " + allCsvFiles.size() + " CSV files total ===");
      for (StorageProvider.FileEntry csv : allCsvFiles) {
        System.out.println("  - " + csv.getName() + " at path: " + csv.getPath());
      }
    } else {
      System.out.println("\n=== No CSV files found in any location ===");
    }
  }

  @Test void testDownloadFileFromSharePoint() throws Exception {
    // Test downloading and reading any file from SharePoint
    MicrosoftGraphStorageProvider provider = new MicrosoftGraphStorageProvider(tokenManager);

    // List documents to find any file
    List<StorageProvider.FileEntry> entries = provider.listFiles("/Shared Documents", false);
    StorageProvider.FileEntry fileToDownload = entries.stream()
        .filter(e -> !e.isDirectory())
        .findFirst()
        .orElse(null);

    if (fileToDownload == null) {
      System.out.println("No files found in SharePoint to test download - skipping test");
      return;
    }

    System.out.println("Testing download of file: " + fileToDownload.getName());
    System.out.println("File size: " + fileToDownload.getSize() + " bytes");

    // Download and read the file
    try (InputStream is = provider.openInputStream(fileToDownload.getPath())) {
      assertNotNull(is, "Should be able to open input stream for file");

      // Read content (up to 1000 bytes for preview)
      byte[] buffer = new byte[Math.min(1000, (int)fileToDownload.getSize())];
      int bytesRead = is.read(buffer);
      assertTrue(bytesRead > 0, "Should be able to read content from file");

      String content = new String(buffer, 0, bytesRead, StandardCharsets.UTF_8);
      System.out.println("\nFile content (first " + bytesRead + " bytes):");
      System.out.println("----------------------------------------");
      System.out.println(content);
      System.out.println("----------------------------------------");

      // For CSV files, check CSV format
      if (fileToDownload.getName().toLowerCase(Locale.ROOT).endsWith(".csv")) {
        assertTrue(content.contains(",") || content.contains("\n"),
            "CSV content should contain commas or newlines");
      }
    }

    // Test file metadata
    StorageProvider.FileMetadata metadata = provider.getMetadata(fileToDownload.getPath());
    assertNotNull(metadata, "Should be able to get file metadata");
    assertEquals(fileToDownload.getPath(), metadata.getPath());
    assertEquals(fileToDownload.getSize(), metadata.getSize(), "Metadata size should match file entry size");
    assertNotNull(metadata.getLastModified(), "Should have last modified date");
    System.out.println("\nFile metadata:");
    System.out.println("  Path: " + metadata.getPath());
    System.out.println("  Size: " + metadata.getSize() + " bytes");
    System.out.println("  Last modified: " + metadata.getLastModified());
    System.out.println("  ETag: " + metadata.getEtag());
  }


  @Test void testTokenRefreshDuringQuery() throws Exception {
    // Force token refresh
    System.out.println("\nTesting token refresh mechanism...");

    MicrosoftGraphStorageProvider provider = new MicrosoftGraphStorageProvider(tokenManager);

    // Initial access
    provider.listFiles("/Shared Documents", false);
    System.out.println("Initial access successful");

    // Invalidate token
    tokenManager.invalidateToken();
    System.out.println("Token invalidated");

    // Access again (should trigger refresh)
    List<StorageProvider.FileEntry> entries = provider.listFiles("/Shared Documents", false);
    assertNotNull(entries);
    System.out.println("Access after invalidation successful - token refreshed!");
  }

  @Test void testCalciteQueryWithCSV() throws Exception {
    // Test Calcite queries on CSV files in SharePoint
    MicrosoftGraphStorageProvider provider = new MicrosoftGraphStorageProvider(tokenManager);

    // Look for CSV files in multiple locations
    List<StorageProvider.FileEntry> allCsvFiles = new ArrayList<>();
    String[] pathsToCheck = {
        "/Shared Documents/Shared Documents",  // The correct nested path
        "Shared Documents/Shared Documents",
        "/Shared Documents",
        "/Shared Documents/Documents",
        "Shared Documents/Documents",
        "/Documents",
        "Documents"
    };

    for (String path : pathsToCheck) {
      try {
        List<StorageProvider.FileEntry> entries = provider.listFiles(path, false);
        for (StorageProvider.FileEntry entry : entries) {
          if (!entry.isDirectory() && entry.getName().toLowerCase(Locale.ROOT).endsWith(".csv")) {
            // Create a corrected file entry with the full path
            String fullPath = path + "/" + entry.getName();
            // Remove leading slash if present for consistency
            if (fullPath.startsWith("/")) {
              fullPath = fullPath.substring(1);
            }
            // Create a new FileEntry with corrected path
            StorageProvider.FileEntry correctedEntry = new StorageProvider.FileEntry(
                fullPath, entry.getName(), entry.isDirectory(), entry.getSize(), entry.getLastModified()
            );
            allCsvFiles.add(correctedEntry);
            System.out.println("Found CSV in " + path + ": " + entry.getName() + " (corrected path: " + fullPath + ")");
          }
          // Also check subdirectories named "Documents"
          if (entry.isDirectory() && entry.getName().equalsIgnoreCase("Documents")) {
            try {
              List<StorageProvider.FileEntry> docEntries = provider.listFiles(entry.getPath(), false);
              for (StorageProvider.FileEntry docEntry : docEntries) {
                if (!docEntry.isDirectory() && docEntry.getName().toLowerCase(Locale.ROOT).endsWith(".csv")) {
                  String docFullPath = entry.getPath() + "/" + docEntry.getName();
                  if (docFullPath.startsWith("/")) {
                    docFullPath = docFullPath.substring(1);
                  }
                  StorageProvider.FileEntry correctedDocEntry = new StorageProvider.FileEntry(
                      docFullPath, docEntry.getName(), docEntry.isDirectory(), docEntry.getSize(), docEntry.getLastModified()
                  );
                  allCsvFiles.add(correctedDocEntry);
                  System.out.println("Found CSV in " + entry.getPath() + ": " + docEntry.getName() + " (corrected path: " + docFullPath + ")");
                }
              }
            } catch (Exception e) {
              // Ignore errors in subdirectories
            }
          }
        }
      } catch (Exception e) {
        // Path doesn't exist or isn't accessible
      }
    }

    StorageProvider.FileEntry csvFile = allCsvFiles.isEmpty() ? null : allCsvFiles.get(0);

    if (csvFile == null) {
      System.out.println("No CSV file found in SharePoint. To test this functionality:");
      System.out.println("1. Upload a CSV file to SharePoint's Shared Documents folder");
      System.out.println("2. Re-run this test");
      System.out.println("\nExample CSV content:");
      System.out.println("id,name,value,category");
      System.out.println("1,Product A,100.50,Electronics");
      System.out.println("2,Product B,75.25,Clothing");
      System.out.println("3,Product C,150.00,Electronics");
      return;
    }

    System.out.println("Found CSV file: " + csvFile.getName() + " at path: " + csvFile.getPath());

    // Download and display CSV content
    System.out.println("\nDownloading CSV content from path: " + csvFile.getPath());
    String csvContent;
    try (InputStream is = provider.openInputStream(csvFile.getPath())) {
      byte[] buffer = new byte[4096];
      int bytesRead = is.read(buffer);
      csvContent = new String(buffer, 0, bytesRead, StandardCharsets.UTF_8);
      System.out.println("CSV content preview:");
      String[] lines = csvContent.split("\n");
      for (int i = 0; i < Math.min(5, lines.length); i++) {
        System.out.println("  " + lines[i]);
      }
      if (lines.length > 5) {
        System.out.println("  ... (" + (lines.length - 5) + " more lines)");
      }
    }

    // Test with Calcite
    System.out.println("\nTesting Calcite queries on CSV file...");
    Properties info = new Properties();
    info.setProperty("lex", "ORACLE");
    info.put("unquotedCasing", "TO_LOWER");

    try (Connection conn = DriverManager.getConnection("jdbc:calcite:", info)) {
      CalciteConnection calciteConn = conn.unwrap(CalciteConnection.class);
      SchemaPlus rootSchema = calciteConn.getRootSchema();

      // Create SharePoint-backed schema - try different directory configurations
      System.out.println("\n=== Testing different directory configurations ===");

      // First try: Point directly to nested Shared Documents
      Map<String, Object> operand = new HashMap<>();
      operand.put("directory", "/Shared Documents/Shared Documents");
      operand.put("storageType", "sharepoint");
      operand.put("recursive", true);  // Enable recursive traversal

      Map<String, Object> storageConfig = new HashMap<>();
      storageConfig.put("siteUrl", siteUrl);
      storageConfig.put("tenantId", tenantId);
      storageConfig.put("clientId", clientId);
      storageConfig.put("clientSecret", clientSecret);
      operand.put("storageConfig", storageConfig);

      FileSchemaFactory factory = FileSchemaFactory.INSTANCE;

      // Try creating schema with the nested path first
      System.out.println("Creating schema with directory: " + operand.get("directory"));
      Schema sharePointSchema = factory.create(rootSchema, "sharepoint", operand);
      rootSchema.add("sharepoint", sharePointSchema);

      // Log that schema was created
      System.out.println("\nSharePoint schema created and added to root schema");

      // Force table discovery via reflection
      if (sharePointSchema instanceof FileSchema) {
        FileSchema fs = (FileSchema) sharePointSchema;
        try {
          java.lang.reflect.Method method = FileSchema.class.getDeclaredMethod("getTableMap");
          method.setAccessible(true);
          Map<String, ?> tables = (Map<String, ?>) method.invoke(fs);
          System.out.println("Direct getTableMap call returned " + tables.size() + " tables: " + tables.keySet());
        } catch (Exception e) {
          System.err.println("Error calling getTableMap: " + e.getMessage());
          e.printStackTrace();
        }
      }

      try (Statement stmt = conn.createStatement()) {
        // Try to query the table directly
        System.out.println("\nTrying direct query on discovered table:");
        try {
          ResultSet rs = stmt.executeQuery("SELECT * FROM sharepoint.DEPARTMENTS LIMIT 5");
          ResultSetMetaData rsmd = rs.getMetaData();
          int columnCount = rsmd.getColumnCount();
          System.out.println("Direct query SUCCESS! Columns: " + columnCount);
          for (int i = 1; i <= columnCount; i++) {
            System.out.print(rsmd.getColumnName(i) + " ");
          }
          System.out.println();
          while (rs.next()) {
            for (int i = 1; i <= columnCount; i++) {
              System.out.print(rs.getString(i) + " | ");
            }
            System.out.println();
          }
        } catch (Exception e) {
          System.out.println("Direct query failed: " + e.getMessage());
        }

        // List all available tables - try different schema name cases
        System.out.println("\nChecking for tables in SharePoint schema (uppercase):");
        ResultSet tables = conn.getMetaData().getTables(null, "SHAREPOINT", "%", null);
        int tableCount = 0;
        while (tables.next()) {
          String foundTable = tables.getString("TABLE_NAME");
          System.out.println("  - Found table: " + foundTable);
          tableCount++;
        }

        if (tableCount == 0) {
          System.out.println("No tables found with uppercase SHAREPOINT, trying lowercase...");
          tables = conn.getMetaData().getTables(null, "sharepoint", "%", null);
          while (tables.next()) {
            String foundTable = tables.getString("TABLE_NAME");
            System.out.println("  - Found table: " + foundTable);
            tableCount++;
          }
        }

        if (tableCount == 0) {
          System.out.println("No tables found with /Shared Documents/Shared Documents");

          // Try alternative: Use root /Shared Documents with recursive=true
          System.out.println("\nTrying alternative: /Shared Documents with recursive=true");
          Map<String, Object> operand2 = new HashMap<>(operand);
          operand2.put("directory", "/Shared Documents");
          operand2.put("recursive", true);
          rootSchema.add("sharepoint2", factory.create(rootSchema, "sharepoint2", operand2));

          tables = conn.getMetaData().getTables(null, "SHAREPOINT2", "%", null);
          while (tables.next()) {
            String foundTable = tables.getString("TABLE_NAME");
            System.out.println("  - Found table: " + foundTable);
            tableCount++;
          }

          if (tableCount > 0) {
            // Use sharepoint2 for queries
            operand = operand2;
          }
        }

        if (tableCount == 0) {
          System.out.println("No tables found with /Shared Documents recursive");

          // Try alternative: Use just Shared Documents/Shared Documents without leading slash
          System.out.println("\nTrying alternative: Shared Documents/Shared Documents");
          Map<String, Object> operand3 = new HashMap<>(operand);
          operand3.put("directory", "Shared Documents/Shared Documents");
          operand3.put("recursive", true);
          rootSchema.add("sharepoint3", factory.create(rootSchema, "sharepoint3", operand3));

          tables = conn.getMetaData().getTables(null, "SHAREPOINT3", "%", null);
          while (tables.next()) {
            String foundTable = tables.getString("TABLE_NAME");
            System.out.println("  - Found table: " + foundTable);
            tableCount++;
          }

          if (tableCount > 0) {
            // Use sharepoint3 for queries
            operand = operand3;
          }
        }

        // Now try to query the CSV files if we found any tables
        if (tableCount > 0) {
          // Determine which schema has the tables
          String schemaName = "sharepoint";
          if (operand.get("directory").equals("/Shared Documents")) {
            schemaName = "sharepoint2";
          } else if (operand.get("directory").equals("Shared Documents/Shared Documents")) {
            schemaName = "sharepoint3";
          }

          // Query departments table - use the actual table names discovered
          System.out.println("\n=== Querying departments table from schema: " + schemaName + " ===");

          // First, list all tables to find the actual table names
          ResultSet allTables = conn.getMetaData().getTables(null, schemaName.toLowerCase(), "%", null);
          String departmentsTable = null;
          String employeesTable = null;
          while (allTables.next()) {
            String tableName = allTables.getString("TABLE_NAME");
            if (tableName.toUpperCase().contains("DEPARTMENTS")) {
              departmentsTable = tableName;
            } else if (tableName.toUpperCase().contains("EMPLOYEES")) {
              employeesTable = tableName;
            }
          }

          if (departmentsTable != null) {
            System.out.println("Found departments table: " + departmentsTable);
            try {
              ResultSet rs = stmt.executeQuery("SELECT COUNT(*) as cnt FROM " + schemaName + "." + departmentsTable);
              if (rs.next()) {
                System.out.println("Departments row count: " + rs.getInt("cnt"));
              }

              rs = stmt.executeQuery("SELECT * FROM " + schemaName + "." + departmentsTable + " LIMIT 5");
              ResultSetMetaData rsmd = rs.getMetaData();
              int columnCount = rsmd.getColumnCount();

            System.out.print("Columns: ");
            for (int i = 1; i <= columnCount; i++) {
              if (i > 1) System.out.print(", ");
              System.out.print(rsmd.getColumnName(i));
            }
            System.out.println("\nSample data:");
            while (rs.next()) {
              System.out.print("  ");
              for (int i = 1; i <= columnCount; i++) {
                if (i > 1) System.out.print(" | ");
                System.out.print(rs.getString(i));
              }
              System.out.println();
            }
          } catch (Exception e) {
            System.out.println("Error querying departments table: " + e.getMessage());
          }
          } else {
            System.out.println("No departments table found");
          }

          // Query employees table
          if (employeesTable != null) {
            System.out.println("\n=== Querying employees table from schema: " + schemaName + " ===");
            System.out.println("Found employees table: " + employeesTable);
            try {
              ResultSet rs = stmt.executeQuery("SELECT COUNT(*) as cnt FROM " + schemaName + "." + employeesTable);
              if (rs.next()) {
                System.out.println("Employees row count: " + rs.getInt("cnt"));
              }

              rs = stmt.executeQuery("SELECT * FROM " + schemaName + "." + employeesTable + " LIMIT 5");
              ResultSetMetaData rsmd = rs.getMetaData();
              int columnCount = rsmd.getColumnCount();

            System.out.print("Columns: ");
            for (int i = 1; i <= columnCount; i++) {
              if (i > 1) System.out.print(", ");
              System.out.print(rsmd.getColumnName(i));
            }
            System.out.println("\nSample data:");
            while (rs.next()) {
              System.out.print("  ");
              for (int i = 1; i <= columnCount; i++) {
                if (i > 1) System.out.print(" | ");
                System.out.print(rs.getString(i));
              }
              System.out.println();
            }
          } catch (Exception e) {
            System.out.println("Error querying employees table: " + e.getMessage());
          }
          } else {
            System.out.println("No employees table found");
          }
        } else {
          System.out.println("\nNo tables found in any configuration - FileSchema may not be traversing correctly");
        }
      }
    }
  }

}
