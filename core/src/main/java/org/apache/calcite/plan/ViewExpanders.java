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
import org.apache.calcite.rel.hint.RelHint;
import org.apache.calcite.rel.type.RelDataType;

import com.google.common.collect.ImmutableList;

import org.checkerframework.checker.nullness.qual.NonNull;
import org.checkerframework.checker.nullness.qual.Nullable;

import java.util.List;

/**
 * Utilities for {@link RelOptTable.ViewExpander} and
 * {@link RelOptTable.ToRelContext}.
 */
@NonNull
public abstract class ViewExpanders {
  private ViewExpanders() {}

  /** Converts a {@code ViewExpander} to a {@code ToRelContext}. */
  public static RelOptTable.ToRelContext toRelContext(
      RelOptTable.ViewExpander viewExpander,
      RelOptCluster cluster,
      List<RelHint> hints) {
    return new RelOptTable.ToRelContext() {
      @Override public RelOptCluster getCluster() {
        return cluster;
      }

      @Override public List<RelHint> getTableHints() {
        return hints;
      }

      @Override public RelRoot expandView(RelDataType rowType, String queryString,
          List<String> schemaPath, @Nullable List<String> viewPath) {
        return viewExpander.expandView(rowType, queryString, schemaPath,
            viewPath);
      }
    };
  }

  /** Converts a {@code ViewExpander} to a {@code ToRelContext}. */
  public static RelOptTable.ToRelContext toRelContext(
      RelOptTable.ViewExpander viewExpander,
      RelOptCluster cluster) {
    return toRelContext(viewExpander, cluster, ImmutableList.of());
  }

  /** Creates a simple {@code ToRelContext} that cannot expand views. */
  public static RelOptTable.ToRelContext simpleContext(RelOptCluster cluster) {
    return simpleContext(cluster, ImmutableList.of());
  }

  /** Creates a simple {@code ToRelContext} that cannot expand views. */
  public static RelOptTable.ToRelContext simpleContext(
      RelOptCluster cluster,
      List<RelHint> hints) {
    return new RelOptTable.ToRelContext() {
      @Override public RelOptCluster getCluster() {
        return cluster;
      }

      @Override public RelRoot expandView(RelDataType rowType, String queryString,
          List<String> schemaPath, @Nullable List<String> viewPath) {
        throw new UnsupportedOperationException();
      }

      @Override public List<RelHint> getTableHints() {
        return hints;
      }
    };
  }
}
