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

import org.apache.calcite.DataContext;
import org.apache.calcite.adapter.java.JavaTypeFactory;
import org.apache.calcite.linq4j.Enumerable;
import org.apache.calcite.linq4j.QueryProvider;
import org.apache.calcite.linq4j.Queryable;
import org.apache.calcite.linq4j.tree.Expression;
import org.apache.calcite.plan.RelOptCluster;
import org.apache.calcite.plan.RelOptTable;
import org.apache.calcite.rel.RelNode;
import org.apache.calcite.rel.type.RelDataType;
import org.apache.calcite.rel.type.RelDataTypeFactory;
import org.apache.calcite.rel.type.RelProtoDataType;
import org.apache.calcite.schema.QueryableTable;
import org.apache.calcite.schema.SchemaPlus;
import org.apache.calcite.schema.Schemas;
import org.apache.calcite.schema.TranslatableTable;
import org.apache.calcite.schema.impl.AbstractTable;
import org.apache.calcite.util.ImmutableIntList;
import org.apache.calcite.util.Util;

import org.apache.arrow.gandiva.evaluator.Filter;
import org.apache.arrow.gandiva.evaluator.Projector;
import org.apache.arrow.gandiva.exceptions.GandivaException;
import org.apache.arrow.gandiva.expression.Condition;
import org.apache.arrow.gandiva.expression.ExpressionTree;
import org.apache.arrow.gandiva.expression.TreeBuilder;
import org.apache.arrow.gandiva.expression.TreeNode;
import org.apache.arrow.memory.RootAllocator;
import org.apache.arrow.vector.ipc.ArrowFileReader;
import org.apache.arrow.vector.ipc.SeekableReadChannel;
import org.apache.arrow.vector.types.pojo.ArrowType;
import org.apache.arrow.vector.types.pojo.Field;
import org.apache.arrow.vector.types.pojo.Schema;

import org.checkerframework.checker.nullness.qual.Nullable;

import java.io.File;
import java.io.FileInputStream;
import java.io.FileNotFoundException;
import java.io.IOException;
import java.lang.reflect.Type;
import java.nio.ByteBuffer;
import java.nio.channels.FileChannel;
import java.nio.channels.SeekableByteChannel;
import java.util.ArrayList;
import java.util.List;
import java.util.Queue;
import java.util.concurrent.ConcurrentLinkedQueue;

import static java.lang.Double.parseDouble;
import static java.lang.Float.parseFloat;
import static java.lang.Integer.parseInt;
import static java.lang.Long.parseLong;
import static java.util.Objects.requireNonNull;

/**
 * Arrow Table.
 */
public class ArrowTable extends AbstractTable
    implements TranslatableTable, QueryableTable {
  private final @Nullable RelProtoDataType protoRowType;
  /** Arrow schema. (In Calcite terminology, more like a row type than a Schema.) */
  private final Schema schema;
  private final File arrowFile;
  private final Queue<FileChannel> channelPool = new ConcurrentLinkedQueue<>();

  ArrowTable(@Nullable RelProtoDataType protoRowType, File arrowFile,
      ArrowFileReader arrowFileReader) {
    try {
      this.schema = arrowFileReader.getVectorSchemaRoot().getSchema();
    } catch (IOException e) {
      throw Util.toUnchecked(e);
    }
    this.protoRowType = protoRowType;
    this.arrowFile = arrowFile;
  }

  @Override public RelDataType getRowType(RelDataTypeFactory typeFactory) {
    if (this.protoRowType != null) {
      return this.protoRowType.apply(typeFactory);
    }
    return deduceRowType(this.schema, (JavaTypeFactory) typeFactory);
  }

  @Override public Expression getExpression(SchemaPlus schema, String tableName,
      Class clazz) {
    return Schemas.tableExpression(schema, getElementType(), tableName, clazz);
  }

  /** Called via code generation; see uses of
   * {@link org.apache.calcite.adapter.arrow.ArrowMethod#ARROW_QUERY}. */
  @SuppressWarnings("unused")
  public Enumerable<Object> query(DataContext root, ImmutableIntList fields,
      List<List<List<String>>> conditions) {
    requireNonNull(fields, "fields");
    final Projector projector;
    final Filter filter;

    if (conditions.isEmpty()) {
      filter = null;

      final List<ExpressionTree> expressionTrees = new ArrayList<>();
      for (int fieldOrdinal : fields) {
        Field field = schema.getFields().get(fieldOrdinal);
        TreeNode node = TreeBuilder.makeField(field);
        expressionTrees.add(TreeBuilder.makeExpression(node, field));
      }
      try {
        projector = Projector.make(schema, expressionTrees);
      } catch (GandivaException e) {
        throw Util.toUnchecked(e);
      }
    } else {
      projector = null;

      final List<TreeNode> conjuncts = new ArrayList<>(conditions.size());
      for (List<List<String>> orGroup : conditions) {
        final List<TreeNode> disjuncts = new ArrayList<>(orGroup.size());
        for (List<String> conditionParts : orGroup) {
          disjuncts.add(
              convertConditionToGandiva(
                  ConditionToken.fromTokenList(conditionParts)));
        }
        if (disjuncts.size() == 1) {
          conjuncts.add(disjuncts.get(0));
        } else {
          conjuncts.add(TreeBuilder.makeOr(disjuncts));
        }
      }
      final Condition filterCondition;
      if (conjuncts.size() == 1) {
        filterCondition = TreeBuilder.makeCondition(conjuncts.get(0));
      } else {
        filterCondition =
            TreeBuilder.makeCondition(TreeBuilder.makeAnd(conjuncts));
      }

      try {
        filter = Filter.make(schema, filterCondition);
      } catch (GandivaException e) {
        throw Util.toUnchecked(e);
      }
    }

    final FileChannel channel = borrowChannel();
    final ArrowFileReader reader = createReader(channel);
    final Runnable onClose = () -> returnChannel(channel);
    return new ArrowEnumerable(reader, fields, projector, filter, onClose);
  }

  @Override public <T> Queryable<T> asQueryable(QueryProvider queryProvider,
      SchemaPlus schema, String tableName) {
    throw new UnsupportedOperationException();
  }

  @Override public Type getElementType() {
    return Object[].class;
  }

  @Override public RelNode toRel(RelOptTable.ToRelContext context,
      RelOptTable relOptTable) {
    final int fieldCount = relOptTable.getRowType().getFieldCount();
    final ImmutableIntList fields =
        ImmutableIntList.copyOf(Util.range(fieldCount));
    final RelOptCluster cluster = context.getCluster();
    return new ArrowTableScan(cluster, cluster.traitSetOf(ArrowRel.CONVENTION),
        relOptTable, this, fields);
  }

  private static RelDataType deduceRowType(Schema schema,
      JavaTypeFactory typeFactory) {
    final RelDataTypeFactory.Builder builder = typeFactory.builder();
    for (Field field : schema.getFields()) {
      builder.add(field.getName(),
          ArrowFieldTypeFactory.toType(field.getType(), typeFactory));
    }
    return builder.build();
  }

  /** Converts a single {@link ConditionToken} into a Gandiva {@link TreeNode}. */
  private TreeNode convertConditionToGandiva(ConditionToken token) {
    final List<TreeNode> treeNodes = new ArrayList<>(2);
    treeNodes.add(
        TreeBuilder.makeField(schema.getFields()
            .get(
                schema.getFields().indexOf(
                schema.findField(token.fieldName)))));

    if (token.isBinary()) {
      treeNodes.add(
          makeLiteralNode(
              requireNonNull(token.value, "value"),
              requireNonNull(token.valueType, "valueType")));
    }

    return TreeBuilder.makeFunction(
        token.operator, treeNodes, new ArrowType.Bool());
  }

  /** Borrows a {@link FileChannel} from the pool, or opens a new one
   * if the pool is empty. */
  private FileChannel borrowChannel() {
    FileChannel channel = channelPool.poll();
    if (channel != null) {
      try {
        channel.position(0);
        return channel;
      } catch (IOException e) {
        // Channel is broken, fall through to create a new one.
      }
    }
    try {
      return new FileInputStream(arrowFile).getChannel();
    } catch (FileNotFoundException e) {
      throw Util.toUnchecked(e);
    }
  }

  /** Returns a {@link FileChannel} to the pool for reuse. */
  private void returnChannel(FileChannel channel) {
    channelPool.offer(channel);
  }

  /** Creates a new {@link ArrowFileReader} that wraps the given channel
   * without owning it (i.e. closing the reader will not close the channel). */
  private static ArrowFileReader createReader(FileChannel channel) {
    return new ArrowFileReader(
        new SeekableReadChannel(new NonClosingByteChannel(channel)),
        new RootAllocator());
  }

  /** A {@link SeekableByteChannel} wrapper that delegates all operations
   * to the underlying channel but makes {@link #close()} a no-op,
   * so the pooled {@link FileChannel} survives reader disposal. */
  private static class NonClosingByteChannel implements SeekableByteChannel {
    private final FileChannel delegate;

    NonClosingByteChannel(FileChannel delegate) {
      this.delegate = delegate;
    }

    @Override public int read(ByteBuffer dst) throws IOException {
      return delegate.read(dst);
    }

    @Override public int write(ByteBuffer src) throws IOException {
      return delegate.write(src);
    }

    @Override public long position() throws IOException {
      return delegate.position();
    }

    @Override public SeekableByteChannel position(long newPosition)
        throws IOException {
      delegate.position(newPosition);
      return this;
    }

    @Override public long size() throws IOException {
      return delegate.size();
    }

    @Override public SeekableByteChannel truncate(long size)
        throws IOException {
      delegate.truncate(size);
      return this;
    }

    @Override public boolean isOpen() {
      return delegate.isOpen();
    }

    @Override public void close() {
      // No-op: the underlying channel is managed by the pool.
    }
  }

  private static TreeNode makeLiteralNode(String literal, String type) {
    if (type.startsWith("decimal")) {
      String[] typeParts =
          type.substring(type.indexOf('(') + 1, type.indexOf(')')).split(",");
      int precision = parseInt(typeParts[0]);
      int scale = parseInt(typeParts[1]);
      return TreeBuilder.makeDecimalLiteral(literal, precision, scale);
    } else if (type.equals("integer")) {
      return TreeBuilder.makeLiteral(parseInt(literal));
    } else if (type.equals("long")) {
      return TreeBuilder.makeLiteral(parseLong(literal));
    } else if (type.equals("float")) {
      return TreeBuilder.makeLiteral(parseFloat(literal));
    } else if (type.equals("double")) {
      return TreeBuilder.makeLiteral(parseDouble(literal));
    } else if (type.equals("string")) {
      return TreeBuilder.makeStringLiteral(literal.substring(1, literal.length() - 1));
    } else {
      throw new IllegalArgumentException("Invalid literal " + literal
          + ", type " + type);
    }
  }
}
