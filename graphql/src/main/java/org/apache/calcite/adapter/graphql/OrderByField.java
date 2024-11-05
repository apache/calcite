package org.apache.calcite.adapter.graphql;

import org.apache.calcite.rel.RelFieldCollation;

public class OrderByField {
  private final Integer field;
  private final RelFieldCollation.Direction direction;

  public OrderByField(Integer field, RelFieldCollation.Direction direction) {
    this.field = field;
    this.direction = direction;
  }

  /**
   * Converts to Hasura order_by format
   */
  public String toHasuraFormat() {
    return String.format("%s: %s", field, direction == RelFieldCollation.Direction.ASCENDING ? "Asc" : "Desc");
  }
}
