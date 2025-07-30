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

/**
 * Represents a column in a SharePoint list.
 */
public class SharePointColumn {
  private final String internalName;
  private final String name;
  private final String type;
  private final boolean required;

  public SharePointColumn(String internalName, String name, String type, boolean required) {
    this.internalName = internalName;
    this.name = name;
    this.type = type;
    this.required = required;
  }

  public String getInternalName() {
    return internalName;
  }

  public String getName() {
    return name;
  }

  public String getType() {
    return type;
  }

  public boolean isRequired() {
    return required;
  }
}
