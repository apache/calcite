package org.apache.calcite.adapter.file;
import com.fasterxml.jackson.databind.ObjectMapper;
import java.util.LinkedHashMap;
import java.io.IOException;

public class ComparableLinkedHashMap<K, V> extends LinkedHashMap<K, V> implements Comparable<ComparableLinkedHashMap<K, V>> {
  @Override
  public int compareTo(ComparableLinkedHashMap<K, V> o) {
    ObjectMapper mapper = new ObjectMapper();

    try {
      // Convert this map to a JSON string
      String thisJson = mapper.writeValueAsString(this);

      // Convert the other map to a JSON string
      String otherJson = mapper.writeValueAsString(o);

      // Return the comparison result
      return thisJson.compareTo(otherJson);

    } catch (IOException e) {
      // Handle the exception
      throw new RuntimeException("Failed to serialize LinkedHashMap to JSON", e);
    }
  }
}
