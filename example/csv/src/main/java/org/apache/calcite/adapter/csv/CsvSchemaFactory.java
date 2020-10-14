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
package org.apache.calcite.adapter.csv;

import org.apache.calcite.model.ModelHandler;
import org.apache.calcite.schema.Schema;
import org.apache.calcite.schema.SchemaFactory;
import org.apache.calcite.schema.SchemaPlus;

import java.io.File;
import java.util.Locale;
import java.util.Map;

/**
 * Factory that creates a {@link CsvSchema}.
 * fixme
 *      创建 CsvSchema 的工厂类，允许自定义包含 model 和 json 文件的schema。
 *
 *
 * <p>Allows a custom schema to be included in a <code><i>model</i>.json</code> file.
 */
@SuppressWarnings("UnusedDeclaration")
public class CsvSchemaFactory implements SchemaFactory {

  /**
   * Public singleton, per factory contract.
   * 单例模式，final。
   */
  public static final CsvSchemaFactory INSTANCE = new CsvSchemaFactory();

  private CsvSchemaFactory() {
  }

  @Override
  public Schema create(SchemaPlus parentSchema,
                       String name,
                       Map<String, Object> operand) {

    // directory：目录
    final String directory = (String) operand.get("directory");

    // operand： 操作数
    final File base = (File) operand.get(ModelHandler.ExtraOperand.BASE_DIRECTORY.camelName);

    // 使用指定目录创建 File 对象
    File directoryFile = new File(directory);
    if (base != null && !directoryFile.isAbsolute()) {
      directoryFile = new File(base, directory);
    }

    // "特点"名称
    String flavorName = (String) operand.get("flavor");

    // 默认 "可扫描的"
    CsvTable.Flavor flavor;
    if (flavorName == null) {
      flavor = CsvTable.Flavor.SCANNABLE;
    } else {
      flavor = CsvTable.Flavor.valueOf(flavorName.toUpperCase(Locale.ROOT));
    }

    //
    return new CsvSchema(directoryFile, flavor);
  }
}
