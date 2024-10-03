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
import org.apache.parquet.avro.AvroSchemaConverter;
import org.apache.avro.Schema;
import org.apache.calcite.linq4j.Linq4j;
import org.apache.calcite.linq4j.Enumerable;

import org.apache.parquet.hadoop.ParquetFileReader;
import org.apache.parquet.hadoop.util.HadoopInputFile;
import org.apache.parquet.schema.MessageType;
import org.apache.parquet.schema.Type;
import org.apache.parquet.schema.PrimitiveType;
import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.fs.Path;
import org.apache.parquet.hadoop.ParquetReader;
//import org.apache.parquet.hadoop.ParquetReadOptions;
import org.apache.parquet.hadoop.ParquetFileReader;
import org.apache.arrow.gandiva.evaluator.Filter;
import org.apache.arrow.gandiva.expression.TreeBuilder;
import org.apache.parquet.hadoop.metadata.ParquetMetadata;
import org.apache.avro.generic.GenericRecord;
import org.apache.arrow.gandiva.expression.Condition;
import org.apache.parquet.io.InputFile;
import org.apache.arrow.gandiva.expression.TreeNode;
import static java.util.Objects.requireNonNull;
import org.apache.arrow.gandiva.evaluator.Projector;
import org.apache.arrow.gandiva.expression.ExpressionTree;
import java.util.ArrayList;
import org.apache.arrow.vector.types.pojo.Field;
import org.apache.arrow.vector.types.pojo.ArrowType;
import org.apache.parquet.avro.AvroParquetReader;
import org.apache.parquet.avro.AvroReadSupport;

import org.checkerframework.checker.nullness.qual.Nullable;
import org.apache.arrow.gandiva.exceptions.GandivaException;
import org.apache.calcite.util.Util;
import java.util.List;
import java.util.Arrays;

import java.io.IOException;


public class ParquetTable extends AbstractTable
    implements TranslatableTable, QueryableTable {

  private final String filePath;
  private final AvroParquetReader.Builder<GenericRecord> readerBuilder;
  private final @Nullable RelProtoDataType protoRowType;
  private MessageType schema;

  ParquetTable(String filePath, AvroParquetReader.Builder<GenericRecord> readerBuilder,
          @Nullable RelProtoDataType protoRowType) {
    this.filePath = filePath;
    this.readerBuilder = readerBuilder;
    this.protoRowType = protoRowType;
    try {
      this.schema = getSchemaFromFile(filePath);
    } catch (IOException e) {
      throw new RuntimeException("Error initializing Parquet schema", e);
    }
  }

  private MessageType createMessageTypeFromOrdinals(ImmutableIntList ordinals) {
    List<Type> selectedTypes = new ArrayList<>();

    for (Integer ordinal : ordinals) {
      selectedTypes.add(schema.getType(ordinal));
    }

    return new MessageType(schema.getName(), selectedTypes);
  }

  private MessageType getSchemaFromFile(String filePath) throws IOException {
    Configuration conf = new Configuration();
    Path path = new Path(filePath);
    InputFile inputFile = HadoopInputFile.fromPath(path, new Configuration());
    try (ParquetFileReader reader = ParquetFileReader.open(inputFile)) {
      ParquetMetadata metadata = reader.getFooter();
      MessageType schema = metadata.getFileMetaData().getSchema();
      System.out.println(schema);
      return schema;
    }
  }

  @Override
  public RelDataType getRowType(RelDataTypeFactory typeFactory) {
    if (this.protoRowType != null) {
      return this.protoRowType.apply(typeFactory);
    }
    return deduceRowType(this.schema, (JavaTypeFactory) typeFactory);
  }

  @Override
  public Expression getExpression(SchemaPlus schema, String tableName,
      Class clazz) {
    return Schemas.tableExpression(schema, Object[].class, tableName, clazz);
  }

  @Override
  public <T> Queryable<T> asQueryable(QueryProvider queryProvider,
      SchemaPlus schema, String tableName) {
    throw new UnsupportedOperationException();
  }

  @Override
  public java.lang.reflect.Type getElementType() {
    return Object[].class;
  }

  @Override
  public RelNode toRel(RelOptTable.ToRelContext context,
      RelOptTable relOptTable) {
    final int fieldCount = relOptTable.getRowType().getFieldCount();
    final ImmutableIntList fields = ImmutableIntList.identity(fieldCount);
    final RelOptCluster cluster = context.getCluster();
    return new ParquetTableScan(cluster, cluster.traitSetOf(ArrowRel.CONVENTION),
        relOptTable, this, fields);
  }

  @SuppressWarnings("unused")
  public Enumerable<Object> query(DataContext root, ImmutableIntList fields,
      List<String> conditions) {
    requireNonNull(fields, "fields");
    final Projector projector;
    final Filter filter;

    AvroSchemaConverter converter = new AvroSchemaConverter();
    MessageType m = createMessageTypeFromOrdinals(fields);
    Schema avroSchema = converter.convert(m);
    Configuration conf = new Configuration();
    AvroReadSupport.setAvroReadSchema(conf, avroSchema);

    List<Object> records = new ArrayList<>();
    try {
      ParquetReader<GenericRecord> reader = readerBuilder.withConf(conf).build();
      try {

        GenericRecord record;

        // loop will run as long as there are records
        while ((record = reader.read()) != null) {
          // add each record to the list
          Object[] recordArray = new Object[record.getSchema().getFields().size()];
          for (int i = 0; i < recordArray.length; i++) {
            recordArray[i] = record.get(i);
          }
          records.add(recordArray);
        }

      } catch (IOException e) {
        throw new RuntimeException("Unable to read from Parquet file", e);
      } finally {
        try {
          reader.close();
        } catch (IOException e) {
          // handle failure to close reader
        }
      }
    } catch (IOException e) {
      e.printStackTrace();
    }
    return Linq4j.asEnumerable(records);
  }

  private static RelDataType deduceRowType(MessageType schema,
      JavaTypeFactory typeFactory) {
    final RelDataTypeFactory.Builder builder = typeFactory.builder();
    for (Type field : schema.getFields()) {
      builder.add(field.getName(), convertParquetType(field, typeFactory));
    }
    return builder.build();
  }

  private static RelDataType convertParquetType(Type parquetType, JavaTypeFactory typeFactory) {
    if (parquetType.isPrimitive()) {
      PrimitiveType primitiveType = parquetType.asPrimitiveType();
      switch (primitiveType.getPrimitiveTypeName()) {
      case BOOLEAN:
        return typeFactory.createJavaType(Boolean.class);
      case INT32:
        return typeFactory.createJavaType(Integer.class);
      case INT64:
        return typeFactory.createJavaType(Long.class);
      case FLOAT:
        return typeFactory.createJavaType(Float.class);
      case DOUBLE:
        return typeFactory.createJavaType(Double.class);
      case BINARY:
      case FIXED_LEN_BYTE_ARRAY:
        return typeFactory.createJavaType(String.class);
      default:
        throw new UnsupportedOperationException("Unsupported Parquet type: " + parquetType);
      }
    } else {
      throw new UnsupportedOperationException("Complex Parquet types not supported: " + parquetType);
    }
  }
}
