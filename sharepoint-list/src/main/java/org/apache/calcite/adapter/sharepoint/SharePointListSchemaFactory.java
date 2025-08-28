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
package org.apache.calcite.adapter.sharepoint;

import org.apache.calcite.schema.Schema;
import org.apache.calcite.schema.SchemaFactory;
import org.apache.calcite.schema.SchemaPlus;

import java.util.Map;

/**
 * Factory for creating SharePoint list schemas.
 */
public class SharePointListSchemaFactory implements SchemaFactory {

  public static final SharePointListSchemaFactory INSTANCE = new SharePointListSchemaFactory();

  private SharePointListSchemaFactory() {
  }

  @Override public Schema create(SchemaPlus parentSchema, String name,
      Map<String, Object> operand) {
    String siteUrl = (String) operand.get("siteUrl");

    if (siteUrl == null) {
      throw new RuntimeException("siteUrl is required");
    }

    // Pass the entire operand map to support all auth patterns
    // This includes Phase 1-3 auth options and API selection flags
    Map<String, Object> config = new java.util.HashMap<>(operand);

    // Ensure siteUrl is in the config
    config.put("siteUrl", siteUrl);

    // Create the main SharePoint schema with full config
    SharePointListSchema sharePointSchema = new SharePointListSchema(siteUrl, config);

    // Add the metadata schemas as top-level schemas (not sub-schemas)
    SharePointMetadataSchema metadataSchema = new SharePointMetadataSchema(sharePointSchema, null, name);
    parentSchema.add("pg_catalog", metadataSchema);
    parentSchema.add("information_schema", metadataSchema);

    return sharePointSchema;
  }
}
