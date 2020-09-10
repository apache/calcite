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

import org.apache.arrow.vector.UInt4Vector;
import org.apache.arrow.vector.VectorSchemaRoot;

import org.apache.calcite.DataContext;
import org.apache.calcite.adapter.java.AbstractQueryableTable;
import org.apache.calcite.adapter.java.JavaTypeFactory;
import org.apache.calcite.linq4j.AbstractEnumerable;
import org.apache.calcite.linq4j.Enumerable;
import org.apache.calcite.linq4j.Enumerator;
import org.apache.calcite.plan.RelOptTable;
import org.apache.calcite.rel.RelNode;
import org.apache.calcite.rel.type.RelDataType;
import org.apache.calcite.rel.type.RelDataTypeFactory;
import org.apache.calcite.rel.type.RelProtoDataType;
import org.apache.calcite.schema.TranslatableTable;
import org.apache.calcite.schema.impl.AbstractTable;
import org.apache.calcite.util.ImmutableIntList;
import org.apache.calcite.util.Pair;
import org.apache.calcite.util.Source;

import javax.lang.model.util.Elements;
import java.util.*;
import java.util.concurrent.atomic.AtomicBoolean;
import java.util.stream.Collectors;

public class ArrowTable extends AbstractTable
{

  private final RelProtoDataType protoRowType;
  private VectorSchemaRoot[] vectorSchemaRoots;
  private UInt4Vector selectionVector;

  public ArrowTable(VectorSchemaRoot[] vectorSchemaRoots, UInt4Vector intVector, RelProtoDataType protoRowType) {
    this.vectorSchemaRoots = vectorSchemaRoots;
    this.selectionVector = intVector;
    this.protoRowType = protoRowType;
  }

  public RelDataType getRowType(RelDataTypeFactory typeFactory) {
    if (this.protoRowType != null) {
      return this.protoRowType.apply(typeFactory);
    }
    return deduceRowType(this.vectorSchemaRoots[0], (JavaTypeFactory) typeFactory);
  }

  public Enumerable<Object> project(final DataContext root, final int[] fields) {
    final AtomicBoolean cancelFlag = DataContext.Variable.CANCEL_FLAG.get(root);
    return null;
  }

  private RelDataType deduceRowType(VectorSchemaRoot vectorSchemaRoot, JavaTypeFactory typeFactory) {
    List<Pair<String, RelDataType>> ret = vectorSchemaRoot.getFieldVectors().stream().map(fieldVector -> {
      RelDataType relDataType = ArrowFieldType.of(fieldVector.getField().getType()).toType(typeFactory);
      return new Pair<>(fieldVector.getField().getName(), relDataType);
    }).collect(Collectors.toList());
    System.out.println(typeFactory.createStructType(ret));
    return typeFactory.createStructType(ret);
  }

   public RelNode toRel(RelOptTable.ToRelContext context, RelOptTable relOptTable) {
     final int fieldCount = relOptTable.getRowType().getFieldCount();
     final int[] fields = EnumerableUtils.identityList(fieldCount);
     return new ArrowTableScan(context.getCluster(), relOptTable, this, fields);
   }
}
