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

import org.apache.calcite.adapter.govdata.GovDataDriver;

import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.sql.Connection;
import java.sql.SQLException;
import java.util.Properties;

/**
 * SEC JDBC Driver - Backward compatibility wrapper.
 *
 * @deprecated Use {@link org.apache.calcite.adapter.govdata.GovDataDriver} 
 * with "jdbc:govdata:source=sec" URLs instead. This class will be removed 
 * in a future version.
 */
@Deprecated
public class SecDriver extends GovDataDriver {
  private static final Logger LOGGER = LoggerFactory.getLogger(SecDriver.class);

  private static boolean deprecationWarningShown = false;

  static {
    new SecDriver().register();
  }

  @Override protected String getConnectStringPrefix() {
    return "jdbc:sec:";
  }

  @Override public Connection connect(String url, Properties info) throws SQLException {
    if (!acceptsURL(url)) {
      return null;
    }

    if (!deprecationWarningShown) {
      LOGGER.warn("SecDriver and 'jdbc:sec:' URLs are deprecated. Please use " +
          "GovDataDriver with 'jdbc:govdata:source=sec' URLs instead. " +
          "This class will be removed in a future version.");
      deprecationWarningShown = true;
    }

    // Convert jdbc:sec: URL to jdbc:govdata:source=sec URL
    String paramString = url.substring("jdbc:sec:".length());
    String govDataUrl;
    
    if (paramString.isEmpty()) {
      govDataUrl = "jdbc:govdata:source=sec";
    } else {
      govDataUrl = "jdbc:govdata:source=sec&" + paramString;
    }
    
    LOGGER.debug("Converting SEC URL '{}' to GovData URL '{}'", url, govDataUrl);
    
    // Delegate to parent GovDataDriver
    return super.connect(govDataUrl, info);
  }
}