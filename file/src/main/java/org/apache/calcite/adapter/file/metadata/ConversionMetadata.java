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
package org.apache.calcite.adapter.file.metadata;

import com.fasterxml.jackson.databind.ObjectMapper;
import com.fasterxml.jackson.databind.SerializationFeature;

import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.io.File;
import java.io.IOException;
import java.io.RandomAccessFile;
import java.nio.channels.FileChannel;
import java.nio.channels.FileLock;
import java.nio.file.Files;
import java.nio.file.StandardCopyOption;
import java.util.HashMap;
import java.util.Map;
import java.util.concurrent.ConcurrentHashMap;

/**
 * Tracks the lineage of converted files (Excel→JSON, HTML→JSON, etc.).
 * This metadata persists across restarts so RefreshableTable can monitor
 * the correct original source files.
 */
public class ConversionMetadata {
  private static final Logger LOGGER = LoggerFactory.getLogger(ConversionMetadata.class);
  private static final String METADATA_FILE = ".calcite_conversions.json";
  private static final ObjectMapper MAPPER = new ObjectMapper()
      .enable(SerializationFeature.INDENT_OUTPUT);
  
  // Static field for central metadata directory - using volatile for thread safety
  private static volatile File centralMetadataDirectory = null;
  
  private final File metadataFile;
  private final Map<String, ConversionRecord> conversions = new ConcurrentHashMap<>();
  
  /**
   * Record of a file conversion.
   */
  public static class ConversionRecord {
    public String originalFile;  // Original source (e.g., Excel file)
    public String convertedFile; // Result of conversion (e.g., JSON file)
    public String conversionType; // Type of conversion (EXCEL_TO_JSON, HTML_TO_JSON, etc.)
    public long timestamp;        // When conversion happened
    
    public ConversionRecord() {} // For Jackson
    
    public ConversionRecord(String originalFile, String convertedFile, 
        String conversionType) {
      this.originalFile = originalFile;
      this.convertedFile = convertedFile;
      this.conversionType = conversionType;
      this.timestamp = System.currentTimeMillis();
    }
    
    @com.fasterxml.jackson.annotation.JsonIgnore
    public String getOriginalPath() {
      return originalFile;
    }
    
    public String getConversionType() {
      return conversionType;
    }
  }
  
  /**
   * Sets the central metadata directory for all instances.
   * This should be called once at startup if you want to use a shared location.
   * Typically this would be set to parquetCacheDirectory/metadata/{storageType}.
   * 
   * @param directory The central directory for metadata storage
   * @param storageType The storage type (e.g., "local", "http", "s3", "sharepoint") 
   */
  public static synchronized void setCentralMetadataDirectory(File directory, String storageType) {
    if (directory != null) {
      // Create storage-specific metadata subdirectory
      String subdirName = storageType != null ? storageType : "local";
      File metadataDir = new File(directory, "metadata/" + subdirName);
      if (!metadataDir.exists()) {
        metadataDir.mkdirs();
      }
      centralMetadataDirectory = metadataDir;
      LOGGER.info("Set central metadata directory to: {} for storage type: {}", 
          metadataDir, subdirName);
    }
  }
  
  /**
   * Sets the central metadata directory for local storage (backward compatibility).
   * 
   * @param directory The central directory for metadata storage
   */
  public static void setCentralMetadataDirectory(File directory) {
    setCentralMetadataDirectory(directory, "local");
  }
  
  /**
   * Creates a metadata tracker for the given directory.
   * If a central metadata directory is configured, uses that instead.
   */
  public ConversionMetadata(File directory) {
    File metadataFile;
    if (centralMetadataDirectory != null) {
      try {
        // Use canonical path to handle symlinks consistently (e.g., /var vs /private/var on macOS)
        String canonicalPath = directory.getCanonicalPath();
        String subdir = Integer.toHexString(canonicalPath.hashCode());
        File metadataDir = new File(centralMetadataDirectory, subdir);
        if (!metadataDir.exists()) {
          metadataDir.mkdirs();
        }
        metadataFile = new File(metadataDir, METADATA_FILE);
        LOGGER.debug("Using central metadata file: {}", metadataFile);
      } catch (IOException e) {
        // Fallback to absolute path if canonical path fails
        String subdir = Integer.toHexString(directory.getAbsolutePath().hashCode());
        File metadataDir = new File(centralMetadataDirectory, subdir);
        if (!metadataDir.exists()) {
          metadataDir.mkdirs();
        }
        metadataFile = new File(metadataDir, METADATA_FILE);
        LOGGER.warn("Failed to get canonical path for {}, using absolute path", directory, e);
      }
    } else {
      // Use local directory (backward compatibility)
      metadataFile = new File(directory, METADATA_FILE);
    }
    this.metadataFile = metadataFile;
    loadMetadata();
  }
  
  /**
   * Records a file conversion.
   * 
   * @param originalFile The source file (e.g., Excel)
   * @param convertedFile The converted file (e.g., JSON)
   * @param conversionType The type of conversion
   */
  public void recordConversion(File originalFile, File convertedFile, String conversionType) {
    try {
      String key = convertedFile.getCanonicalPath();
      ConversionRecord record = new ConversionRecord(
          originalFile.getCanonicalPath(),
          convertedFile.getCanonicalPath(),
          conversionType
      );
      
      conversions.put(key, record);
      saveMetadata();
      
      LOGGER.debug("Recorded conversion: {} -> {} ({})", 
          originalFile.getName(), convertedFile.getName(), conversionType);
    } catch (IOException e) {
      LOGGER.error("Failed to record conversion metadata", e);
    }
  }
  
  /**
   * Finds the original source file for a converted file.
   * 
   * @param convertedFile The converted file (e.g., JSON)
   * @return The original source file, or null if not found
   */
  public File findOriginalSource(File convertedFile) {
    try {
      String key = convertedFile.getCanonicalPath();
      ConversionRecord record = conversions.get(key);
      
      if (record != null) {
        File originalFile = new File(record.originalFile);
        if (originalFile.exists()) {
          return originalFile;
        } else {
          // Original file was deleted or moved
          LOGGER.debug("Original source {} no longer exists", record.originalFile);
          conversions.remove(key);
          saveMetadata();
        }
      }
    } catch (IOException e) {
      LOGGER.error("Failed to find original source", e);
    }
    
    return null;
  }
  
  /**
   * Finds all files that were derived from a given source file.
   * This is useful for finding JSONPath extractions that need to be refreshed
   * when the source file changes.
   * 
   * @param sourceFile The source file to find derivatives for
   * @return List of derived files, empty if none found
   */
  public java.util.List<File> findDerivedFiles(File sourceFile) {
    java.util.List<File> derivedFiles = new java.util.ArrayList<>();
    
    try {
      String sourcePath = sourceFile.getCanonicalPath();
      
      for (ConversionRecord record : conversions.values()) {
        if (sourcePath.equals(record.originalFile)) {
          File derivedFile = new File(record.convertedFile);
          if (derivedFile.exists()) {
            derivedFiles.add(derivedFile);
          } else {
            // Clean up metadata for non-existent files
            LOGGER.debug("Derived file {} no longer exists", record.convertedFile);
          }
        }
      }
    } catch (IOException e) {
      LOGGER.error("Failed to find derived files for {}", sourceFile.getName(), e);
    }
    
    return derivedFiles;
  }
  
  /**
   * Gets the conversion record for a converted file.
   * 
   * @param convertedFile The converted file
   * @return The conversion record, or null if not found
   */
  public ConversionRecord getConversionRecord(File convertedFile) {
    try {
      String key = convertedFile.getCanonicalPath();
      return conversions.get(key);
    } catch (IOException e) {
      LOGGER.error("Failed to get conversion record", e);
      return null;
    }
  }
  
  /**
   * Loads metadata from disk with file locking for concurrent access.
   */
  private void loadMetadata() {
    if (!metadataFile.exists()) {
      return;
    }
    
    File lockFile = new File(metadataFile.getParentFile(), metadataFile.getName() + ".lock");
    
    try (RandomAccessFile raf = new RandomAccessFile(lockFile, "rw");
         FileChannel channel = raf.getChannel()) {
      
      // Acquire shared lock for reading
      try (FileLock lock = channel.lock(0, Long.MAX_VALUE, true)) {
        @SuppressWarnings("unchecked")
        Map<String, ConversionRecord> loaded = MAPPER.readValue(metadataFile, 
            MAPPER.getTypeFactory().constructMapType(HashMap.class, 
                String.class, ConversionRecord.class));
        
        conversions.putAll(loaded);
        LOGGER.debug("Loaded {} conversion records from metadata", loaded.size());
        
        // Clean up entries for files that no longer exist
        boolean needsCleanup = false;
        for (Map.Entry<String, ConversionRecord> entry : loaded.entrySet()) {
          File convertedFile = new File(entry.getKey());
          File originalFile = new File(entry.getValue().originalFile);
          if (convertedFile.exists() && originalFile.exists()) {
            conversions.put(entry.getKey(), entry.getValue());
          } else {
            LOGGER.debug("Skipping stale conversion record: {}", entry.getKey());
            needsCleanup = true;
          }
        }
        
        // Save cleaned version if needed (will use exclusive lock)
        if (needsCleanup) {
          // Release shared lock before acquiring exclusive lock
          lock.release();
          saveMetadata();
        }
      }
    } catch (Exception e) {
      LOGGER.error("Failed to load conversion metadata", e);
    }
  }
  
  /**
   * Saves metadata to disk with file locking for concurrent access.
   */
  private void saveMetadata() {
    File tempFile = new File(metadataFile.getParentFile(), metadataFile.getName() + ".tmp");
    File lockFile = new File(metadataFile.getParentFile(), metadataFile.getName() + ".lock");
    
    try (RandomAccessFile raf = new RandomAccessFile(lockFile, "rw");
         FileChannel channel = raf.getChannel()) {
      
      // Acquire exclusive lock
      try (FileLock lock = channel.lock()) {
        // Write to temp file first
        MAPPER.writeValue(tempFile, conversions);
        
        // Atomically move temp file to actual file
        Files.move(tempFile.toPath(), metadataFile.toPath(), 
                   StandardCopyOption.REPLACE_EXISTING, 
                   StandardCopyOption.ATOMIC_MOVE);
        
        LOGGER.debug("Saved {} conversion records to metadata", conversions.size());
      }
    } catch (IOException e) {
      LOGGER.error("Failed to save conversion metadata", e);
      // Clean up temp file if it exists
      if (tempFile.exists()) {
        tempFile.delete();
      }
    }
  }
  
  /**
   * Clears all metadata (mainly for testing).
   */
  public void clear() {
    conversions.clear();
    if (metadataFile.exists()) {
      metadataFile.delete();
    }
  }
}