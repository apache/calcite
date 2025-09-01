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
package org.apache.calcite.adapter.file.iceberg;

import org.apache.iceberg.DataFile;
import org.apache.iceberg.Snapshot;
import org.apache.iceberg.Table;
import org.apache.iceberg.hadoop.HadoopTables;
import org.apache.iceberg.io.CloseableIterable;

import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.time.Instant;
import java.util.ArrayList;
import java.util.List;
import java.util.Map;

/**
 * Resolves Iceberg snapshots within a specified time range and collects
 * the associated Parquet data files.
 */
public class IcebergTimeRangeResolver {
  private static final Logger LOGGER = LoggerFactory.getLogger(IcebergTimeRangeResolver.class);

  /**
   * Information about a data file from an Iceberg snapshot.
   */
  public static class IcebergDataFile {
    private final String filePath;
    private final Instant snapshotTime;
    private final long snapshotId;

    public IcebergDataFile(String filePath, Instant snapshotTime, long snapshotId) {
      this.filePath = filePath;
      this.snapshotTime = snapshotTime;
      this.snapshotId = snapshotId;
    }

    public String getFilePath() { return filePath; }
    public Instant getSnapshotTime() { return snapshotTime; }
    public long getSnapshotId() { return snapshotId; }
  }

  /**
   * Resolves all data files from snapshots within the specified time range.
   *
   * @param tablePath Path to the Iceberg table
   * @param startTime Start of time range (inclusive)
   * @param endTime End of time range (inclusive)
   * @return List of data files with their snapshot information
   */
  public List<IcebergDataFile> resolveTimeRange(String tablePath, Instant startTime, Instant endTime) {
    LOGGER.debug("Resolving Iceberg time range: {} to {} for table: {}", startTime, endTime, tablePath);

    List<IcebergDataFile> dataFiles = new ArrayList<>();

    try {
      HadoopTables tables = new HadoopTables();
      Table icebergTable = tables.load(tablePath);

      // Get all snapshots
      Iterable<Snapshot> snapshots = icebergTable.snapshots();

      for (Snapshot snapshot : snapshots) {
        long timestampMillis = snapshot.timestampMillis();
        Instant snapshotTime = Instant.ofEpochMilli(timestampMillis);

        // Check if snapshot is within time range
        if (!snapshotTime.isBefore(startTime) && !snapshotTime.isAfter(endTime)) {
          LOGGER.debug("Including snapshot {} from {}", snapshot.snapshotId(), snapshotTime);

          // Get data files from this snapshot
          try (CloseableIterable<org.apache.iceberg.FileScanTask> files = icebergTable.newScan()
                  .useSnapshot(snapshot.snapshotId())
                  .planFiles()) {

            for (org.apache.iceberg.FileScanTask task : files) {
              DataFile dataFile = task.file();
              String filePath = dataFile.path().toString();
              dataFiles.add(new IcebergDataFile(filePath, snapshotTime, snapshot.snapshotId()));
            }
          }
        } else {
          LOGGER.debug("Excluding snapshot {} from {} (outside time range)", snapshot.snapshotId(), snapshotTime);
        }
      }

      LOGGER.info("Resolved {} data files from {} snapshots in time range", dataFiles.size(),
                 dataFiles.stream().mapToLong(IcebergDataFile::getSnapshotId).distinct().count());

    } catch (Exception e) {
      LOGGER.error("Failed to resolve Iceberg time range for table: {}", tablePath, e);
      throw new RuntimeException("Failed to resolve Iceberg time range", e);
    }

    return dataFiles;
  }

  /**
   * Parses time range configuration from model definition.
   *
   * @param timeRangeConfig Time range configuration map
   * @return Array containing [startTime, endTime]
   */
  public static Instant[] parseTimeRange(Map<String, Object> timeRangeConfig) {
    String startStr = (String) timeRangeConfig.get("start");
    String endStr = (String) timeRangeConfig.get("end");

    if (startStr == null || endStr == null) {
      throw new IllegalArgumentException("Time range must specify both 'start' and 'end' times");
    }

    try {
      Instant startTime = Instant.parse(startStr);
      Instant endTime = Instant.parse(endStr);

      if (startTime.isAfter(endTime)) {
        throw new IllegalArgumentException("Start time must be before end time");
      }

      return new Instant[]{startTime, endTime};
    } catch (Exception e) {
      throw new IllegalArgumentException("Invalid time range format. Use ISO-8601 format like '2024-01-01T00:00:00Z'", e);
    }
  }
}
