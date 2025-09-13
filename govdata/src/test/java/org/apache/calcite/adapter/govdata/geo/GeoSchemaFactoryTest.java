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
package org.apache.calcite.adapter.govdata.geo;

import org.apache.calcite.adapter.govdata.GovDataSchemaFactory;
import org.apache.calcite.jdbc.CalciteConnection;
import org.apache.calcite.schema.Schema;
import org.apache.calcite.schema.SchemaPlus;

import org.junit.jupiter.api.BeforeEach;
import org.junit.jupiter.api.Test;
import org.junit.jupiter.api.Tag;
import org.junit.jupiter.api.io.TempDir;

import java.io.File;
import java.sql.Connection;
import java.sql.DatabaseMetaData;
import java.sql.DriverManager;
import java.sql.ResultSet;
import java.sql.Statement;
import java.util.HashMap;
import java.util.Map;
import java.util.Properties;

import static org.junit.jupiter.api.Assertions.assertNotNull;
import static org.junit.jupiter.api.Assertions.assertTrue;
import static org.junit.jupiter.api.Assertions.assertEquals;
import static org.junit.jupiter.api.Assertions.assertFalse;

/**
 * Unit tests for the Geographic data adapter.
 */
@Tag("unit")
public class GeoSchemaFactoryTest {

  @TempDir
  File tempDir;

  private Map<String, Object> operand;

  @BeforeEach
  public void setUp() {
    operand = new HashMap<>();
    operand.put("dataSource", "geo");
    operand.put("cacheDir", tempDir.getAbsolutePath());
    operand.put("autoDownload", false); // Disable auto-download for unit tests
  }

  @Test
  public void testGeoSchemaFactoryCreation() {
    GeoSchemaFactory factory = new GeoSchemaFactory();
    assertNotNull(factory, "GeoSchemaFactory should be created");
  }

  @Test
  public void testGeoSchemaCreation() throws Exception {
    // Create schema using the factory
    GeoSchemaFactory factory = new GeoSchemaFactory();

    Connection conn = DriverManager.getConnection("jdbc:calcite:");
    CalciteConnection calciteConn = conn.unwrap(CalciteConnection.class);
    SchemaPlus rootSchema = calciteConn.getRootSchema();

    Schema geoSchema = factory.create(rootSchema, "GEO", operand);
    assertNotNull(geoSchema, "Geographic schema should be created");
    assertTrue(geoSchema instanceof GeoSchema, "Schema should be instance of GeoSchema");
  }

  @Test
  public void testGovDataFactoryRoutesToGeo() throws Exception {
    // Test that GovDataSchemaFactory correctly routes to GeoSchemaFactory
    GovDataSchemaFactory factory = new GovDataSchemaFactory();

    Connection conn = DriverManager.getConnection("jdbc:calcite:");
    CalciteConnection calciteConn = conn.unwrap(CalciteConnection.class);
    SchemaPlus rootSchema = calciteConn.getRootSchema();

    Schema schema = factory.create(rootSchema, "GOV", operand);
    assertNotNull(schema, "Schema should be created via GovDataSchemaFactory");
    assertTrue(schema instanceof GeoSchema, "Should route to GeoSchema");
  }

  @Test
  public void testCensusApiClientCreation() {
    File cacheDir = new File(tempDir, "census-cache");
    CensusApiClient client = new CensusApiClient("test-api-key", cacheDir);
    assertNotNull(client, "Census API client should be created");
    assertTrue(cacheDir.exists(), "Cache directory should be created");
  }

  @Test
  public void testTigerDataDownloaderCreation() {
    File cacheDir = new File(tempDir, "tiger-data");
    TigerDataDownloader downloader = new TigerDataDownloader(cacheDir, 2024, false);
    assertNotNull(downloader, "TIGER data downloader should be created");
    assertTrue(cacheDir.exists(), "Cache directory should be created");

    // Test that shapefile is not available (since we disabled auto-download)
    assertFalse(downloader.isShapefileAvailable("states"),
        "States shapefile should not be available without download");
  }

  @Test
  public void testHudCrosswalkFetcherCreation() {
    File cacheDir = new File(tempDir, "hud-crosswalk");
    HudCrosswalkFetcher fetcher = new HudCrosswalkFetcher("test-user", "test-pass", cacheDir);
    assertNotNull(fetcher, "HUD crosswalk fetcher should be created");
    assertTrue(cacheDir.exists(), "Cache directory should be created");
  }

  @Test
  public void testGeocodeResultStructure() {
    CensusApiClient.GeocodeResult result = new CensusApiClient.GeocodeResult();
    result.latitude = 38.8977;
    result.longitude = -77.0365;
    result.stateFips = "11";
    result.countyFips = "001";
    result.tractCode = "006202";
    result.blockGroup = "1";

    String expected = "GeocodeResult[lat=38.897700, lon=-77.036500, state=11, county=001, tract=006202]";
    assertEquals(expected, result.toString(), "GeocodeResult toString should format correctly");
  }

  @Test
  public void testCrosswalkRecordStructure() {
    HudCrosswalkFetcher.CrosswalkRecord record = new HudCrosswalkFetcher.CrosswalkRecord();
    record.zip = "20001";
    record.geoCode = "11001";
    record.resRatio = 0.85;
    record.busRatio = 0.10;
    record.othRatio = 0.05;
    record.totRatio = 1.00;

    String expected = "Crosswalk[zip=20001, geo=11001, res=0.850, bus=0.100, tot=1.000]";
    assertEquals(expected, record.toString(), "CrosswalkRecord toString should format correctly");
  }

  @Test
  public void testSchemaWithModelJson() throws Exception {
    // Create a model JSON string
    String modelJson = "{"
        + "\"version\": \"1.0\","
        + "\"defaultSchema\": \"GEO\","
        + "\"schemas\": [{"
        + "  \"name\": \"GEO\","
        + "  \"type\": \"custom\","
        + "  \"factory\": \"org.apache.calcite.adapter.govdata.GovDataSchemaFactory\","
        + "  \"operand\": {"
        + "    \"dataSource\": \"geo\","
        + "    \"cacheDir\": \"" + tempDir.getAbsolutePath().replace("\\", "\\\\") + "\","
        + "    \"autoDownload\": false"
        + "  }"
        + "}]"
        + "}";

    // Write model to file
    File modelFile = new File(tempDir, "geo-model.json");
    java.nio.file.Files.write(modelFile.toPath(), modelJson.getBytes());

    // Connect using the model
    Properties props = new Properties();
    props.setProperty("model", modelFile.getAbsolutePath());
    props.setProperty("lex", "ORACLE");
    props.setProperty("unquotedCasing", "TO_LOWER");

    try (Connection conn = DriverManager.getConnection("jdbc:calcite:", props)) {
      assertNotNull(conn, "Connection should be established");

      // Check that the schema exists
      DatabaseMetaData metaData = conn.getMetaData();
      try (ResultSet schemas = metaData.getSchemas()) {
        boolean foundGeoSchema = false;
        while (schemas.next()) {
          if ("GEO".equals(schemas.getString("TABLE_SCHEM"))) {
            foundGeoSchema = true;
            break;
          }
        }
        assertTrue(foundGeoSchema, "GEO schema should exist");
      }
    }
  }
}
