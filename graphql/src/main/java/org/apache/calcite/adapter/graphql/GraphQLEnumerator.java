package org.apache.calcite.adapter.graphql;

import org.apache.calcite.linq4j.Enumerator;
import org.apache.calcite.linq4j.Linq4j;
import org.checkerframework.checker.nullness.qual.Nullable;
import java.sql.SQLException;
import java.util.*;

public class GraphQLEnumerator implements Enumerator<Object> {
  private final Enumerator<@Nullable Object> enumerator;
  private final List<Object> cachedRows;
  private int currentRowIndex;

  public GraphQLEnumerator(List<? extends @Nullable Object> list) {
    List<@Nullable Object> objs = new ArrayList<>();
    for (Object obj : list) {
      if (obj instanceof Collection) {
        List<Object> tmp = (List<Object>) obj;
        objs.add(tmp.toArray());
      } else if (obj instanceof Map) {
        objs.add(((LinkedHashMap) obj).values().toArray());
      } else {
        objs.add(new Object[]{obj});
      }
    }
    this.enumerator = Linq4j.enumerator(objs);
    this.cachedRows = new ArrayList<>(objs);
    this.currentRowIndex = -1;
  }

  @Override
  public Object current() {
    Object row = enumerator.current();
    assert row != null;
    if (row.getClass().isArray()) {
      Object[] rowArray = (Object[]) row;
      if (rowArray.length == 1) {
        return rowArray[0];
      }
    }
    return row;
  }

  @Override
  public boolean moveNext() {
    if (currentRowIndex + 1 < cachedRows.size()) {
      currentRowIndex++;
      return enumerator.moveNext();
    }
    return false;
  }

  @Override
  public void reset() {
    currentRowIndex = -1;
    enumerator.reset();
  }

  @Override
  public void close() {
    enumerator.close();
  }

  public boolean previous() throws SQLException {
    if (currentRowIndex > 0) {
      currentRowIndex--;
      enumerator.reset();
      for (int i = 0; i < currentRowIndex; i++) {
        enumerator.moveNext();
      }
      return true;
    }
    return false;
  }

  public boolean absolute(int row) throws SQLException {
    if (row >= 0 && row < cachedRows.size()) {
      currentRowIndex = row;
      enumerator.reset();
      for (int i = 0; i < row; i++) {
        enumerator.moveNext();
      }
      return true;
    }
    return false;
  }

  public boolean relative(int rows) throws SQLException {
    int newRow = currentRowIndex + rows;
    return absolute(newRow);
  }

  public boolean first() throws SQLException {
    return absolute(0);
  }

  public boolean last() throws SQLException {
    return absolute(cachedRows.size() - 1);
  }

  public boolean isBeforeFirst() throws SQLException {
    return currentRowIndex == -1 && !cachedRows.isEmpty();
  }

  public boolean isAfterLast() throws SQLException {
    return currentRowIndex >= cachedRows.size() && !cachedRows.isEmpty();
  }

  public boolean isFirst() throws SQLException {
    return currentRowIndex == 0;
  }

  public boolean isLast() throws SQLException {
    return currentRowIndex == cachedRows.size() - 1;
  }

  public int getRow() throws SQLException {
    return currentRowIndex + 1;
  }
}
