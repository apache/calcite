import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.fs.Path;
import org.apache.parquet.column.page.PageReadStore;
import org.apache.parquet.hadoop.ParquetFileReader;
import org.apache.parquet.hadoop.metadata.ParquetMetadata;
import org.apache.parquet.io.ColumnIOFactory;
import org.apache.parquet.io.MessageColumnIO;
import org.apache.parquet.io.RecordReader;
import org.apache.parquet.io.api.Binary;
import org.apache.parquet.io.api.Converter;
import org.apache.parquet.io.api.GroupConverter;
import org.apache.parquet.io.api.PrimitiveConverter;
import org.apache.parquet.io.api.RecordMaterializer;
import org.apache.parquet.schema.MessageType;

import java.io.File;
import java.util.ArrayList;
import java.util.List;

public class TestDirectParquetRead {
  static class SimpleGroup {
    Object[] values;
    SimpleGroup(int size) {
      values = new Object[size];
    }
  }
  
  public static void main(String[] args) throws Exception {
    // Find the parquet file
    File dir = new File("build/resources/test/csv-type-inference/.parquet_cache_csv_blank_preserved");
    if (!dir.exists()) {
      System.out.println("Cache directory doesn't exist. Run the test first.");
      return;
    }
    
    File parquetFile = new File(dir, "blank_strings.parquet");
    if (!parquetFile.exists()) {
      System.out.println("blank_strings.parquet doesn't exist");
      return;
    }
    
    Configuration conf = new Configuration();
    Path path = new Path(parquetFile.getAbsolutePath());
    
    try (ParquetFileReader reader = ParquetFileReader.open(conf, path)) {
      ParquetMetadata metadata = reader.getFooter();
      MessageType schema = metadata.getFileMetaData().getSchema();
      
      System.out.println("Schema: " + schema);
      System.out.println("\nReading data:");
      
      PageReadStore pages;
      while ((pages = reader.readNextRowGroup()) != null) {
        long rows = pages.getRowCount();
        System.out.println("Row group has " + rows + " rows");
        
        MessageColumnIO columnIO = new ColumnIOFactory().getColumnIO(schema);
        
        RecordMaterializer<SimpleGroup> materializer = new RecordMaterializer<SimpleGroup>() {
          SimpleGroup current;
          
          GroupConverter converter = new GroupConverter() {
            public void start() {
              current = new SimpleGroup(schema.getFieldCount());
            }
            
            public void end() {}
            
            public Converter getConverter(int fieldIndex) {
              return new PrimitiveConverter() {
                public void addBinary(Binary value) {
                  String str = value.toStringUsingUTF8();
                  System.out.println("Field " + fieldIndex + " (" + schema.getFields().get(fieldIndex).getName() + 
                                   "): Binary value='" + str + "', length=" + str.length() + 
                                   ", bytes=" + value.length());
                  current.values[fieldIndex] = str;
                }
                
                public void addInt(int value) {
                  current.values[fieldIndex] = value;
                }
                
                public void addLong(long value) {
                  current.values[fieldIndex] = value;
                }
                
                public void addBoolean(boolean value) {
                  current.values[fieldIndex] = value;
                }
                
                public void addDouble(double value) {
                  current.values[fieldIndex] = value;
                }
                
                public void addFloat(float value) {
                  current.values[fieldIndex] = value;
                }
              };
            }
          };
          
          public SimpleGroup getCurrentRecord() {
            return current;
          }
          
          public GroupConverter getRootConverter() {
            return converter;
          }
        };
        
        RecordReader<SimpleGroup> recordReader = columnIO.getRecordReader(pages, materializer);
        
        for (int i = 0; i < rows; i++) {
          SimpleGroup group = recordReader.read();
          System.out.println("\nRow " + i + ":");
          for (int j = 0; j < group.values.length; j++) {
            Object val = group.values[j];
            if (val == null) {
              System.out.println("  " + schema.getFields().get(j).getName() + ": NULL");
            } else if (val instanceof String) {
              String str = (String) val;
              System.out.println("  " + schema.getFields().get(j).getName() + ": '" + str + 
                               "' (length=" + str.length() + ", isEmpty=" + str.isEmpty() + ")");
            } else {
              System.out.println("  " + schema.getFields().get(j).getName() + ": " + val);
            }
          }
        }
      }
    }
  }
}