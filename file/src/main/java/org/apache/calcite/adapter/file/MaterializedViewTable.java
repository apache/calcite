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

import org.apache.calcite.jdbc.CalciteConnection;
import org.apache.calcite.plan.RelOptTable;
import org.apache.calcite.rel.RelNode;
import org.apache.calcite.rel.logical.LogicalTableScan;
import org.apache.calcite.rel.type.RelDataType;
import org.apache.calcite.rel.type.RelDataTypeFactory;
import org.apache.calcite.schema.ScannableTable;
import org.apache.calcite.schema.SchemaPlus;
import org.apache.calcite.schema.Table;
import org.apache.calcite.schema.TranslatableTable;
import org.apache.calcite.schema.impl.AbstractSchema;
import org.apache.calcite.schema.impl.AbstractTable;

import org.apache.avro.Schema;
import org.apache.avro.SchemaBuilder;
import org.apache.avro.generic.GenericData;
import org.apache.avro.generic.GenericRecord;
import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.fs.Path;
import org.apache.parquet.avro.AvroParquetWriter;
import org.apache.parquet.hadoop.ParquetWriter;
import org.apache.parquet.hadoop.metadata.CompressionCodecName;

import java.io.File;
import java.sql.Connection;
import java.sql.DriverManager;
import java.sql.ResultSet;
import java.sql.ResultSetMetaData;
import java.sql.Statement;
import java.sql.Types;
import java.util.Map;
import java.util.concurrent.atomic.AtomicBoolean;

import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

/**
 * Table that materializes query results to Parquet on first access.
 */
public class MaterializedViewTable extends AbstractTable implements TranslatableTable {
  private static final Logger LOGGER = LoggerFactory.getLogger(MaterializedViewTable.class);
  
  private final SchemaPlus parentSchema;
  private final String schemaName;
  private final String viewName;
  private final String sql;
  protected final File parquetFile;
  private final Map<String, Table> existingTables;
  protected final AtomicBoolean materialized = new AtomicBoolean(false);

  public MaterializedViewTable(SchemaPlus parentSchema, String schemaName, String viewName,
      String sql, File parquetFile, Map<String, Table> existingTables) {
    this.parentSchema = parentSchema;
    this.schemaName = schemaName;
    this.viewName = viewName;
    this.sql = sql;
    this.parquetFile = parquetFile;
    this.existingTables = existingTables;
  }

  private void materialize() {
    if (materialized.compareAndSet(false, true)) {
      try {
        LOGGER.debug("Materializing view {} by executing SQL: {}", viewName, sql);

        // Execute the SQL and write results to Parquet
        try (Connection conn = DriverManager.getConnection("jdbc:calcite:");
             CalciteConnection calciteConn = conn.unwrap(CalciteConnection.class)) {

          // Add current schema with existing tables
          SchemaPlus rootSchema = calciteConn.getRootSchema();
          SchemaPlus schema = rootSchema.add(schemaName, new AbstractSchema() {
            @Override protected Map<String, Table> getTableMap() {
              return existingTables;
            }
          });

          try (Statement stmt = conn.createStatement();
               ResultSet rs = stmt.executeQuery(sql)) {

            ResultSetMetaData rsmd = rs.getMetaData();
            int columnCount = rsmd.getColumnCount();

            // Build Avro schema from ResultSet metadata
            SchemaBuilder.FieldAssembler<Schema> fieldAssembler =
                SchemaBuilder.record(viewName + "_record")
                .namespace("org.apache.calcite.adapter.file")
                .fields();

            for (int i = 1; i <= columnCount; i++) {
              String columnName = rsmd.getColumnName(i);
              int sqlType = rsmd.getColumnType(i);

              switch (sqlType) {
              case Types.INTEGER:
              case Types.BIGINT:
                fieldAssembler.name(columnName).type().longType().noDefault();
                break;
              case Types.FLOAT:
              case Types.DOUBLE:
              case Types.DECIMAL:
                fieldAssembler.name(columnName).type().doubleType().noDefault();
                break;
              case Types.BOOLEAN:
                fieldAssembler.name(columnName).type().booleanType().noDefault();
                break;
              default:
                fieldAssembler.name(columnName).type().stringType().noDefault();
                break;
              }
            }

            Schema avroSchema = fieldAssembler.endRecord();

            // Write to Parquet
            Path hadoopPath = new Path(parquetFile.getAbsolutePath());
            Configuration conf = new Configuration();

            @SuppressWarnings("deprecation")
            ParquetWriter<GenericRecord> writer =
                AvroParquetWriter.<GenericRecord>builder(hadoopPath)
                .withConf(conf)
                .withSchema(avroSchema)
                .withCompressionCodec(CompressionCodecName.SNAPPY)
                .build();

            try {
              while (rs.next()) {
                GenericRecord record = new GenericData.Record(avroSchema);

                for (int i = 1; i <= columnCount; i++) {
                  String columnName = rsmd.getColumnName(i);
                  int sqlType = rsmd.getColumnType(i);

                  // Get value first to check for nulls
                  Object value = rs.getObject(i);
                  if (value != null && !rs.wasNull()) {
                    switch (sqlType) {
                    case Types.INTEGER:
                    case Types.BIGINT:
                      record.put(columnName, rs.getLong(i));
                      break;
                    case Types.FLOAT:
                    case Types.DOUBLE:
                    case Types.DECIMAL:
                      record.put(columnName, rs.getDouble(i));
                      break;
                    case Types.BOOLEAN:
                      record.put(columnName, rs.getBoolean(i));
                      break;
                    default:
                      record.put(columnName, rs.getString(i));
                      break;
                    }
                  }
                }

                writer.write(record);
              }
            } finally {
              writer.close();
            }

            LOGGER.debug("Materialized view written to Parquet: {}", parquetFile.getAbsolutePath());
          }
        }
      } catch (Exception e) {
        LOGGER.error("Failed to materialize view {}: {}", viewName, e.getMessage());
        throw new RuntimeException("Failed to materialize view", e);
      }
    }
  }


  @Override public RelDataType getRowType(RelDataTypeFactory typeFactory) {
    materialize();
    // Get the ParquetTable and return its row type
    Table parquetTable = getParquetTable();
    return parquetTable.getRowType(typeFactory);
  }

  @Override public RelNode toRel(RelOptTable.ToRelContext context, RelOptTable relOptTable) {
    materialize();
    // Get the ParquetTable and delegate to it
    Table parquetTable = getParquetTable();
    if (parquetTable instanceof TranslatableTable) {
      return ((TranslatableTable) parquetTable).toRel(context, relOptTable);
    } else if (parquetTable instanceof ScannableTable) {
      // For ScannableTable, we need to create a table scan
      return LogicalTableScan.create(context.getCluster(), relOptTable,
          context.getTableHints());
    } else {
      throw new RuntimeException(
          "Parquet table doesn't implement TranslatableTable or ScannableTable");
    }
  }

  private Table getParquetTable() {
    // Use our custom ParquetTranslatableTable that works with the file adapter
    return new ParquetTranslatableTable(parquetFile);
  }
}
