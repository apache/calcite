/*
// Licensed to Julian Hyde under one or more contributor license
// agreements. See the NOTICE file distributed with this work for
// additional information regarding copyright ownership.
//
// Julian Hyde licenses this file to you under the Apache License,
// Version 2.0 (the "License"); you may not use this file except in
// compliance with the License. You may obtain a copy of the License at:
//
// http://www.apache.org/licenses/LICENSE-2.0
//
// Unless required by applicable law or agreed to in writing, software
// distributed under the License is distributed on an "AS IS" BASIS,
// WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
// See the License for the specific language governing permissions and
// limitations under the License.
*/
package net.hydromatic.optiq.impl.csv;

import net.hydromatic.optiq.*;

import org.eigenbase.reltype.RelDataType;

import java.io.File;
import java.util.ArrayList;
import java.util.List;
import java.util.Map;

/**
 * Factory that creates a {@link CsvTable}.
 *
 * <p>Allows a CSV table to be included in a model.json file, even in a
 * schema that is not based upon {@link CsvSchema}.</p>
 */
@SuppressWarnings("UnusedDeclaration")
public class CsvTableFactory implements TableFactory<CsvTable> {
  // public constructor, per factory contract
  public CsvTableFactory() {
  }

  public CsvTable create(Schema schema, String name,
      Map<String, Object> map, RelDataType rowType) {
    String fileName = (String) map.get("file");
    Boolean smart = (Boolean) map.get("smart");
    final File file = new File(fileName);
    final List<CsvFieldType> list = new ArrayList<CsvFieldType>();
    final RelDataType rowType2 =
        CsvTable.deduceRowType(schema.getTypeFactory(), file, list);
    final RelDataType rowType3 = rowType != null ? rowType : rowType2;
    return new CsvTable(schema, name, file, rowType3, list);
  }
}

// End CsvTableFactory.java
