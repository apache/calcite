package org.apache.calcite.adapter.file;
import com.fasterxml.jackson.databind.ObjectMapper;
import java.util.ArrayList;
import java.io.IOException;

public class ComparableArrayList<T> extends ArrayList<T> implements Comparable<ComparableArrayList<T>> {
  @Override
  public int compareTo(ComparableArrayList<T> o) {
    ObjectMapper mapper = new ObjectMapper();

    try {
      // Convert this list to a JSON string
      String thisJson = mapper.writeValueAsString(this);

      // Convert the other list to a JSON string
      String otherJson = mapper.writeValueAsString(o);

      // Return the comparison result
      return thisJson.compareTo(otherJson);
    } catch (IOException e) {
      // Handle the exception
      throw new RuntimeException("Failed to serialize ArrayList to JSON", e);
    }
  }
}
