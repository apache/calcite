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
package org.apache.calcite.adapter.file.refresh;

import org.apache.calcite.adapter.file.converters.SafeExcelToJsonConverter;
import org.apache.calcite.adapter.file.converters.HtmlToJsonConverter;
import org.apache.calcite.adapter.file.converters.XmlToJsonConverter;

import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.io.File;
import java.time.Duration;
import java.util.Map;
import java.util.concurrent.ConcurrentHashMap;
import java.util.concurrent.Executors;
import java.util.concurrent.ScheduledExecutorService;
import java.util.concurrent.TimeUnit;

/**
 * Watches source files that need conversion (Excel, HTML, XML) and 
 * re-runs conversion when they change. This ensures that the generated
 * JSON files stay up-to-date, which then triggers the normal refresh
 * mechanism for tables based on those JSON files.
 */
public class ConversionFileWatcher {
  private static final Logger LOGGER = LoggerFactory.getLogger(ConversionFileWatcher.class);
  
  private static ConversionFileWatcher instance;
  private final ScheduledExecutorService executor;
  private final Map<File, FileInfo> watchedFiles = new ConcurrentHashMap<>();
  
  private static class FileInfo {
    final FileType type;
    long lastModified;
    long lastChecked;
    
    FileInfo(FileType type, long lastModified) {
      this.type = type;
      this.lastModified = lastModified;
      this.lastChecked = System.currentTimeMillis();
    }
  }
  
  private enum FileType {
    EXCEL, HTML, XML
  }
  
  private ConversionFileWatcher() {
    this.executor = Executors.newSingleThreadScheduledExecutor(r -> {
      Thread t = new Thread(r, "ConversionFileWatcher");
      t.setDaemon(true);
      return t;
    });
  }
  
  /**
   * Gets the singleton instance of the watcher.
   */
  public static synchronized ConversionFileWatcher getInstance() {
    if (instance == null) {
      instance = new ConversionFileWatcher();
    }
    return instance;
  }
  
  /**
   * Registers a file to be watched for changes.
   * 
   * @param file The file to watch
   * @param refreshInterval How often to check for changes
   */
  public void watchFile(File file, Duration refreshInterval) {
    if (file == null || !file.exists()) {
      return;
    }
    
    String name = file.getName().toLowerCase();
    FileType type = null;
    
    if (name.endsWith(".xlsx") || name.endsWith(".xls")) {
      type = FileType.EXCEL;
    } else if (name.endsWith(".html") || name.endsWith(".htm")) {
      type = FileType.HTML;
    } else if (name.endsWith(".xml")) {
      type = FileType.XML;
    }
    
    if (type != null) {
      FileInfo info = new FileInfo(type, file.lastModified());
      FileInfo existing = watchedFiles.putIfAbsent(file, info);
      
      if (existing == null) {
        LOGGER.debug("Started watching {} file: {}", type, file.getAbsolutePath());
        
        // Schedule periodic checks
        long intervalMillis = refreshInterval != null ? 
            refreshInterval.toMillis() : 60000; // Default 1 minute
            
        executor.scheduleWithFixedDelay(() -> checkFile(file), 
            intervalMillis, intervalMillis, TimeUnit.MILLISECONDS);
      }
    }
  }
  
  /**
   * Stops watching a file.
   */
  public void unwatchFile(File file) {
    watchedFiles.remove(file);
  }
  
  /**
   * Checks if a file has been modified and re-runs conversion if needed.
   */
  private void checkFile(File file) {
    FileInfo info = watchedFiles.get(file);
    if (info == null) {
      return; // File no longer being watched
    }
    
    if (!file.exists()) {
      LOGGER.warn("Watched file no longer exists: {}", file.getAbsolutePath());
      watchedFiles.remove(file);
      return;
    }
    
    long currentModified = file.lastModified();
    if (currentModified > info.lastModified) {
      LOGGER.info("Detected change in {} file: {}", info.type, file.getName());
      
      try {
        // Re-run the appropriate conversion
        switch (info.type) {
        case EXCEL:
          SafeExcelToJsonConverter.convertIfNeeded(file, true);
          LOGGER.info("Re-converted Excel file to JSON: {}", file.getName());
          break;
          
        case HTML:
          // HTML conversion
          try {
            File outputDir = file.getParentFile();
            HtmlToJsonConverter.convert(file, outputDir);
            LOGGER.info("Re-converted HTML file to JSON: {}", file.getName());
          } catch (Exception e) {
            LOGGER.error("Failed to re-convert HTML file: {}", file.getName(), e);
          }
          break;
          
        case XML:
          // XML conversion  
          try {
            File outputDir = file.getParentFile();
            XmlToJsonConverter.convert(file, outputDir);
            LOGGER.info("Re-converted XML file to JSON: {}", file.getName());
          } catch (Exception e) {
            LOGGER.error("Failed to re-convert XML file: {}", file.getName(), e);
          }
          break;
        }
        
        // Update the last modified time
        info.lastModified = currentModified;
        
      } catch (Exception e) {
        LOGGER.error("Failed to re-convert {} file {}: {}", 
            info.type, file.getName(), e.getMessage(), e);
      }
    }
    
    info.lastChecked = System.currentTimeMillis();
  }
  
  /**
   * Shuts down the watcher.
   */
  public void shutdown() {
    executor.shutdown();
    try {
      if (!executor.awaitTermination(5, TimeUnit.SECONDS)) {
        executor.shutdownNow();
      }
    } catch (InterruptedException e) {
      executor.shutdownNow();
      Thread.currentThread().interrupt();
    }
  }
}