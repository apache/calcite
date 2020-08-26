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
package org.apache.calcite.adapter.arrow;

import org.apache.calcite.schema.SchemaPlus;
import org.apache.calcite.schema.Table;
import org.apache.calcite.schema.impl.AbstractSchema;

import com.google.common.collect.ImmutableList;

import java.io.File;
import java.util.List;
import java.util.Map;

/**
 * Schema mapped onto a set of Arrow files.
 */
class ArrowSchema extends AbstractSchema {
  private final ImmutableList<Map<String, Object>> tables;
  private final File baseDirectory;

  /**
   * Creates an Arrow schema.
   *
   * @param parentSchema  Parent schema
   * @param name          Schema name
   * @param baseDirectory Base directory to look for relative files, or null
   */
  ArrowSchema(SchemaPlus parentSchema, String name, File baseDirectory,
      List<Map<String, Object>> tables) {
    this.tables = tables == null ? ImmutableList.of()
        : ImmutableList.copyOf(tables);
    this.baseDirectory = baseDirectory;
  }

  @Override protected Map<String, Table> getTableMap() {
    return null;
  }
}
