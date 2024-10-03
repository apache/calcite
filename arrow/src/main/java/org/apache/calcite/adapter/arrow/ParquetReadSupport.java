package org.apache.calcite.adapter.arrow;

import org.apache.hadoop.conf.Configuration;
import org.apache.parquet.hadoop.api.InitContext;
import org.apache.parquet.hadoop.api.ReadSupport;
import org.apache.parquet.io.api.Binary;
import org.apache.parquet.io.api.Converter;
import org.apache.parquet.io.api.GroupConverter;
import org.apache.parquet.io.api.PrimitiveConverter;
import org.apache.parquet.io.api.RecordMaterializer;
import org.apache.parquet.schema.MessageType;

import java.util.Map;
import java.util.HashMap;

public class ParquetReadSupport extends ReadSupport<Object> {

  @Override
  public ReadContext init(InitContext context) {
    return new ReadContext(context.getFileSchema());
  }

  @Override
  @SuppressWarnings("deprecation")
  public RecordMaterializer<Object> prepareForRead(Configuration configuration,
      Map<String, String> keyValueMetaData,
      MessageType fileSchema,
      ReadContext readContext) {
    return new CustomRecordMaterializer(fileSchema);
  }

  // This method is for future compatibility
  public RecordMaterializer<Object> prepareForReadWithContext(Configuration configuration,
      Map<String, String> keyValueMetaData,
      MessageType fileSchema,
      ReadContext readContext,
      InitContext initContext) {
    return prepareForRead(configuration, keyValueMetaData, fileSchema, readContext);
  }

  private static class CustomRecordMaterializer extends RecordMaterializer<Object> {
    private final MessageType schema;
    private Map<String, Object> currentRecord;

    CustomRecordMaterializer(MessageType schema) {
      this.schema = schema;
    }

    @Override
    public Object getCurrentRecord() {
      return currentRecord;
    }

    @Override
    public void skipCurrentRecord() {
      this.currentRecord = null;
    }

    public void startMessage() {
      this.currentRecord = new HashMap<>();
    }

    public void endMessage() {
      // Finalize the record if needed
    }

    @Override
    public GroupConverter getRootConverter() {
      return new GroupConverter() {
        @Override
        public Converter getConverter(int fieldIndex) {
          return new CustomPrimitiveConverter(schema.getFieldName(fieldIndex));
        }

        @Override
        public void start() {
          startMessage();
        }

        @Override
        public void end() {
          endMessage();
        }
      };
    }

    private class CustomPrimitiveConverter extends PrimitiveConverter {
      private final String fieldName;

      CustomPrimitiveConverter(String fieldName) {
        this.fieldName = fieldName;
      }

      @Override
      public void addBinary(Binary value) {
        currentRecord.put(fieldName, value.toStringUsingUTF8());
      }

      @Override
      public void addBoolean(boolean value) {
        currentRecord.put(fieldName, value);
      }

      @Override
      public void addDouble(double value) {
        currentRecord.put(fieldName, value);
      }

      @Override
      public void addFloat(float value) {
        currentRecord.put(fieldName, value);
      }

      @Override
      public void addInt(int value) {
        currentRecord.put(fieldName, value);
      }

      @Override
      public void addLong(long value) {
        currentRecord.put(fieldName, value);
      }
    }
  }
}
