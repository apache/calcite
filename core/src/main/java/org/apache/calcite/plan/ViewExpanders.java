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
package org.apache.calcite.plan;

import org.apache.calcite.rel.RelRoot;
import org.apache.calcite.rel.type.RelDataType;
import org.apache.calcite.schema.SchemaPlus;

import java.util.List;
import javax.annotation.Nonnull;

/**
 * Utilities for {@link RelOptTable.ViewExpander} and
 * {@link RelOptTable.ToRelContext}.
 */
@Nonnull
public abstract class ViewExpanders {
  private ViewExpanders() {}

  /** Converts a {@code ViewExpander} to a {@code ToRelContext}. */
  public static RelOptTable.ToRelContext toRelContext(
      RelOptTable.ViewExpander viewExpander, RelOptCluster cluster) {
    if (viewExpander instanceof RelOptTable.ToRelContext) {
      return (RelOptTable.ToRelContext) viewExpander;
    }
    return new RelOptTable.ToRelContext() {
      public RelOptCluster getCluster() {
        return cluster;
      }

      public RelRoot expandView(RelDataType rowType, String queryString,
          List<String> schemaPath, List<String> viewPath) {
        return viewExpander.expandView(rowType, queryString, schemaPath,
            viewPath);
      }

      public RelRoot expandView(RelDataType rowType, String queryString,
                                SchemaPlus rootSchema, List<String> schemaPath) {
        return viewExpander.expandView(rowType, queryString,
            rootSchema, schemaPath);
      }
    };
  }

  /** Creates a simple {@code ToRelContext} that cannot expand views. */
  public static RelOptTable.ToRelContext simpleContext(RelOptCluster cluster) {
    return new RelOptTable.ToRelContext() {
      public RelOptCluster getCluster() {
        return cluster;
      }

      public RelRoot expandView(RelDataType rowType, String queryString,
          List<String> schemaPath, List<String> viewPath) {
        throw new UnsupportedOperationException();
      }
    };
  }
}

// End ViewExpanders.java
