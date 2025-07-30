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

import java.util.List;

/**
 * Metadata for a SharePoint list.
 */
public class SharePointListMetadata {
  private final String listId;
  private final String listName;
  private final String entityTypeName;
  private final List<SharePointColumn> columns;

  public SharePointListMetadata(String listId, String listName, String entityTypeName,
      List<SharePointColumn> columns) {
    this.listId = listId;
    this.listName = listName;
    this.entityTypeName = entityTypeName;
    this.columns = columns;
  }

  public String getListId() {
    return listId;
  }

  public String getListName() {
    return listName;
  }

  public String getEntityTypeName() {
    return entityTypeName;
  }

  public List<SharePointColumn> getColumns() {
    return columns;
  }
}
