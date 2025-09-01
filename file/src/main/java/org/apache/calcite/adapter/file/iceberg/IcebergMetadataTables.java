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

import org.apache.calcite.DataContext;
import org.apache.calcite.linq4j.AbstractEnumerable;
import org.apache.calcite.linq4j.Enumerable;
import org.apache.calcite.linq4j.Enumerator;
import org.apache.calcite.rel.type.RelDataType;
import org.apache.calcite.rel.type.RelDataTypeFactory;
import org.apache.calcite.schema.ScannableTable;
import org.apache.calcite.schema.Table;
import org.apache.calcite.schema.impl.AbstractTable;
import org.apache.calcite.sql.type.SqlTypeName;

import org.apache.iceberg.DataFile;
import org.apache.iceberg.ManifestFile;
import org.apache.iceberg.Snapshot;
import org.apache.iceberg.StructLike;
import org.apache.iceberg.io.CloseableIterable;

import java.sql.Timestamp;
import java.util.ArrayList;
import java.util.HashMap;
import java.util.List;
import java.util.Map;

/**
 * Provides access to Iceberg metadata tables (history, snapshots, files, etc.).
 */
public class IcebergMetadataTables {

  /**
   * Creates all metadata tables for an Iceberg table.
   */
  public static Map<String, Table> createMetadataTables(IcebergTable icebergTable) {
    Map<String, Table> tables = new HashMap<>();

    tables.put("history", new HistoryTable(icebergTable));
    tables.put("snapshots", new SnapshotsTable(icebergTable));
    tables.put("files", new FilesTable(icebergTable));
    tables.put("manifests", new ManifestsTable(icebergTable));
    tables.put("partitions", new PartitionsTable(icebergTable));

    return tables;
  }

  /**
   * Base class for Iceberg metadata tables.
   */
  private abstract static class MetadataTable extends AbstractTable implements ScannableTable {
    protected final IcebergTable icebergTable;

    MetadataTable(IcebergTable icebergTable) {
      this.icebergTable = icebergTable;
    }
  }

  /**
   * History metadata table ($history).
   */
  private static class HistoryTable extends MetadataTable {
    HistoryTable(IcebergTable icebergTable) {
      super(icebergTable);
    }

    @Override public RelDataType getRowType(RelDataTypeFactory typeFactory) {
      return typeFactory.builder()
          .add("made_current_at", SqlTypeName.TIMESTAMP)
          .add("snapshot_id", SqlTypeName.BIGINT)
          .add("parent_id", SqlTypeName.BIGINT)
          .add("is_current_ancestor", SqlTypeName.BOOLEAN)
          .build();
    }

    @Override public Enumerable<Object[]> scan(DataContext root) {
      return new AbstractEnumerable<Object[]>() {
        @Override public Enumerator<Object[]> enumerator() {
          List<Object[]> rows = new ArrayList<>();

          try {
            org.apache.iceberg.Table table = icebergTable.getIcebergTable();
            for (Snapshot snapshot : table.snapshots()) {
              rows.add(new Object[]{
                  new Timestamp(snapshot.timestampMillis()),
                  snapshot.snapshotId(),
                  snapshot.parentId(),
                  false // TODO: Calculate if this is a current ancestor
              });
            }
          } catch (Exception e) {
            throw new RuntimeException("Failed to read history", e);
          }

          return new ListEnumerator<>(rows);
        }
      };
    }
  }

  /**
   * Snapshots metadata table ($snapshots).
   */
  private static class SnapshotsTable extends MetadataTable {
    SnapshotsTable(IcebergTable icebergTable) {
      super(icebergTable);
    }

    @Override public RelDataType getRowType(RelDataTypeFactory typeFactory) {
      return typeFactory.builder()
          .add("committed_at", SqlTypeName.TIMESTAMP)
          .add("snapshot_id", SqlTypeName.BIGINT)
          .add("parent_id", SqlTypeName.BIGINT)
          .add("operation", SqlTypeName.VARCHAR)
          .add("manifest_list", SqlTypeName.VARCHAR)
          .add("summary", SqlTypeName.VARCHAR)
          .build();
    }

    @Override public Enumerable<Object[]> scan(DataContext root) {
      return new AbstractEnumerable<Object[]>() {
        @Override public Enumerator<Object[]> enumerator() {
          List<Object[]> rows = new ArrayList<>();

          try {
            org.apache.iceberg.Table table = icebergTable.getIcebergTable();
            for (Snapshot snapshot : table.snapshots()) {
              rows.add(new Object[]{
                  new Timestamp(snapshot.timestampMillis()),
                  snapshot.snapshotId(),
                  snapshot.parentId(),
                  snapshot.operation(),
                  snapshot.manifestListLocation(),
                  snapshot.summary().toString()
              });
            }
          } catch (Exception e) {
            throw new RuntimeException("Failed to read snapshots", e);
          }

          return new ListEnumerator<>(rows);
        }
      };
    }
  }

  /**
   * Files metadata table ($files).
   */
  private static class FilesTable extends MetadataTable {
    FilesTable(IcebergTable icebergTable) {
      super(icebergTable);
    }

    @Override public RelDataType getRowType(RelDataTypeFactory typeFactory) {
      return typeFactory.builder()
          .add("content", SqlTypeName.INTEGER)
          .add("file_path", SqlTypeName.VARCHAR)
          .add("file_format", SqlTypeName.VARCHAR)
          .add("spec_id", SqlTypeName.INTEGER)
          .add("record_count", SqlTypeName.BIGINT)
          .add("file_size_in_bytes", SqlTypeName.BIGINT)
          .build();
    }

    @Override public Enumerable<Object[]> scan(DataContext root) {
      return new AbstractEnumerable<Object[]>() {
        @Override public Enumerator<Object[]> enumerator() {
          List<Object[]> rows = new ArrayList<>();

          try {
            org.apache.iceberg.Table table = icebergTable.getIcebergTable();
            Snapshot currentSnapshot = table.currentSnapshot();

            if (currentSnapshot != null) {
              // Scan all data files in the current snapshot
              try (CloseableIterable<org.apache.iceberg.FileScanTask> fileScanTasks =
                   table.newScan().planFiles()) {

                for (org.apache.iceberg.FileScanTask fileScanTask : fileScanTasks) {
                  DataFile dataFile = fileScanTask.file();
                  rows.add(new Object[]{
                      dataFile.content().id(),  // content type (0=DATA, 1=POSITION_DELETES, 2=EQUALITY_DELETES)
                      dataFile.path().toString(),
                      dataFile.format().toString(),
                      dataFile.specId(),
                      dataFile.recordCount(),
                      dataFile.fileSizeInBytes()
                  });
                }
              }
            }
          } catch (Exception e) {
            throw new RuntimeException("Failed to read data files", e);
          }

          return new ListEnumerator<>(rows);
        }
      };
    }
  }

  /**
   * Manifests metadata table ($manifests).
   */
  private static class ManifestsTable extends MetadataTable {
    ManifestsTable(IcebergTable icebergTable) {
      super(icebergTable);
    }

    @Override public RelDataType getRowType(RelDataTypeFactory typeFactory) {
      return typeFactory.builder()
          .add("path", SqlTypeName.VARCHAR)
          .add("length", SqlTypeName.BIGINT)
          .add("partition_spec_id", SqlTypeName.INTEGER)
          .add("added_snapshot_id", SqlTypeName.BIGINT)
          .add("added_data_files_count", SqlTypeName.INTEGER)
          .add("existing_data_files_count", SqlTypeName.INTEGER)
          .add("deleted_data_files_count", SqlTypeName.INTEGER)
          .build();
    }

    @Override public Enumerable<Object[]> scan(DataContext root) {
      return new AbstractEnumerable<Object[]>() {
        @Override public Enumerator<Object[]> enumerator() {
          List<Object[]> rows = new ArrayList<>();

          try {
            org.apache.iceberg.Table table = icebergTable.getIcebergTable();
            Snapshot currentSnapshot = table.currentSnapshot();

            if (currentSnapshot != null && currentSnapshot.allManifests(table.io()) != null) {
              // Scan all manifest files in the current snapshot
              for (ManifestFile manifestFile : currentSnapshot.allManifests(table.io())) {
                rows.add(new Object[]{
                    manifestFile.path(),
                    manifestFile.length(),
                    manifestFile.partitionSpecId(),
                    manifestFile.snapshotId(),
                    manifestFile.addedFilesCount(),
                    manifestFile.existingFilesCount(),
                    manifestFile.deletedFilesCount()
                });
              }
            }
          } catch (Exception e) {
            throw new RuntimeException("Failed to read manifest files", e);
          }

          return new ListEnumerator<>(rows);
        }
      };
    }
  }

  /**
   * Partitions metadata table ($partitions).
   */
  private static class PartitionsTable extends MetadataTable {
    PartitionsTable(IcebergTable icebergTable) {
      super(icebergTable);
    }

    @Override public RelDataType getRowType(RelDataTypeFactory typeFactory) {
      return typeFactory.builder()
          .add("partition", SqlTypeName.VARCHAR)
          .add("record_count", SqlTypeName.BIGINT)
          .add("file_count", SqlTypeName.INTEGER)
          .build();
    }

    @Override public Enumerable<Object[]> scan(DataContext root) {
      return new AbstractEnumerable<Object[]>() {
        @Override public Enumerator<Object[]> enumerator() {
          List<Object[]> rows = new ArrayList<>();

          try {
            org.apache.iceberg.Table table = icebergTable.getIcebergTable();
            Snapshot currentSnapshot = table.currentSnapshot();

            if (currentSnapshot != null) {
              // Group files by partition to calculate statistics
              Map<String, PartitionStats> partitionStats = new HashMap<>();

              try (CloseableIterable<org.apache.iceberg.FileScanTask> fileScanTasks =
                   table.newScan().planFiles()) {

                for (org.apache.iceberg.FileScanTask fileScanTask : fileScanTasks) {
                  DataFile dataFile = fileScanTask.file();
                  // Convert partition data to string representation
                  String partitionKey = partitionToString(dataFile.partition());

                  PartitionStats stats =
                      partitionStats.computeIfAbsent(partitionKey, k -> new PartitionStats());
                  stats.recordCount += dataFile.recordCount();
                  stats.fileCount++;
                }
              }

              // Convert to rows
              for (Map.Entry<String, PartitionStats> entry : partitionStats.entrySet()) {
                rows.add(new Object[]{
                    entry.getKey(),
                    entry.getValue().recordCount,
                    entry.getValue().fileCount
                });
              }
            }
          } catch (Exception e) {
            throw new RuntimeException("Failed to read partition information", e);
          }

          return new ListEnumerator<>(rows);
        }

        private String partitionToString(StructLike partition) {
          if (partition == null) {
            return "{}";
          }

          StringBuilder sb = new StringBuilder();
          sb.append("{");
          for (int i = 0; i < partition.size(); i++) {
            if (i > 0) sb.append(", ");
            Object value = partition.get(i, Object.class);
            sb.append(value != null ? value.toString() : "null");
          }
          sb.append("}");
          return sb.toString();
        }
      };
    }

    private static class PartitionStats {
      long recordCount = 0;
      int fileCount = 0;
    }
  }

  /**
   * Simple list-based enumerator.
   */
  private static class ListEnumerator<T> implements Enumerator<T> {
    private final List<T> list;
    private int index = -1;

    ListEnumerator(List<T> list) {
      this.list = list;
    }

    @Override public T current() {
      return list.get(index);
    }

    @Override public boolean moveNext() {
      return ++index < list.size();
    }

    @Override public void reset() {
      index = -1;
    }

    @Override public void close() {
      // Nothing to close
    }
  }
}
