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
package org.apache.calcite.adapter.sec;

import org.apache.calcite.schema.Schema;
import org.apache.calcite.schema.SchemaFactory;
import org.apache.calcite.schema.SchemaPlus;

import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.util.Map;

/**
 * SEC Schema Factory - Backward compatibility wrapper.
 *
 * @deprecated Use {@link org.apache.calcite.adapter.govdata.GovDataSchemaFactory}
 * with dataSource="sec" instead. This class will be removed in a future version.
 */
@Deprecated
public class SecSchemaFactory implements SchemaFactory {
  private static final Logger LOGGER = LoggerFactory.getLogger(SecSchemaFactory.class);

  private static boolean deprecationWarningShown = false;

  @Override public Schema create(SchemaPlus parentSchema, String name, 
      Map<String, Object> operand) {
    
    if (!deprecationWarningShown) {
      LOGGER.warn("SecSchemaFactory is deprecated. Please use " +
          "org.apache.calcite.adapter.govdata.GovDataSchemaFactory with dataSource='sec' " +
          "instead. This class will be removed in a future version.");
      deprecationWarningShown = true;
    }
    
    // Ensure dataSource is set to "sec" for backward compatibility
    operand.put("dataSource", "sec");
    
    // Delegate to the new SEC schema factory
    return new org.apache.calcite.adapter.govdata.sec.SecSchemaFactory()
        .create(parentSchema, name, operand);
  }
}