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
package org.apache.calcite.adapter.elasticsearch;

import com.fasterxml.jackson.annotation.JsonCreator;
import com.fasterxml.jackson.annotation.JsonIgnoreProperties;
import com.fasterxml.jackson.annotation.JsonProperty;

import java.time.Duration;
import java.util.Iterator;
import java.util.List;
import java.util.Locale;
import java.util.Map;
import java.util.Objects;

/**
 * Internal object used to parse elastic search result. Similar to {@code SearchHit}.
 * Since we're using row-level rest client the response has to be processed manually.
 */
@JsonIgnoreProperties(ignoreUnknown = true)
public class ElasticsearchSearchResult {

  private final SearchHits hits;
  private final long took;

  /**
   * Constructor for this instance.
   * @param hits list of matched documents
   * @param took time taken (in took) for this query to execute
   */
  @JsonCreator
  ElasticsearchSearchResult(@JsonProperty("hits") SearchHits hits,
                            @JsonProperty("took") long took) {
    this.hits = Objects.requireNonNull(hits, "hits");
    this.took = took;
  }

  public SearchHits searchHits() {
    return hits;
  }

  public Duration took() {
    return Duration.ofMillis(took);
  }

  /**
   * Similar to {@code SearchHits} in ES. Container for {@link SearchHit}
   */
  @JsonIgnoreProperties(ignoreUnknown = true)
  public static class SearchHits {

    private final long total;
    private final List<SearchHit> hits;

    @JsonCreator
    SearchHits(@JsonProperty("total")final long total,
               @JsonProperty("hits") final List<SearchHit> hits) {
      this.total = total;
      this.hits = Objects.requireNonNull(hits, "hits");
    }

    public List<SearchHit> hits() {
      return this.hits;
    }

    public long total() {
      return total;
    }

  }

  /**
   * Concrete result record which matched the query. Similar to {@code SearchHit} in ES.
   */
  @JsonIgnoreProperties(ignoreUnknown = true)
  public static class SearchHit {
    private final String id;
    private final Map<String, Object> source;
    private final Map<String, Object> fields;

    @JsonCreator
    private SearchHit(@JsonProperty("_id") final String id,
                      @JsonProperty("_source") final Map<String, Object> source,
                      @JsonProperty("fields") final Map<String, Object> fields) {
      this.id = Objects.requireNonNull(id, "id");

      // both can't be null
      if (source == null && fields == null) {
        final String message = String.format(Locale.ROOT,
            "Both '_source' and 'fields' are missing for %s", id);
        throw new IllegalArgumentException(message);
      }

      // both can't be non-null
      if (source != null && fields != null) {
        final String message = String.format(Locale.ROOT,
            "Both '_source' and 'fields' are populated (non-null) for %s", id);
        throw new IllegalArgumentException(message);
      }

      this.source = source;
      this.fields = fields;
    }

    /**
     * Returns id of this hit (usually document id)
     * @return unique id
     */
    public String id() {
      return id;
    }

    /**
     * Finds specific attribute from ES search result
     * @param name attribute name
     * @return value from result (_source or fields)
     */
    Object value(String name) {
      Objects.requireNonNull(name, "name");

      if (!sourceOrFields().containsKey(name)) {
        final String message = String.format(Locale.ROOT,
            "Attribute %s not found in search result %s", name, id);
        throw new IllegalArgumentException(message);
      }

      if (source != null) {
        return source.get(name);
      } else if (fields != null) {
        Object field = fields.get(name);
        if (field instanceof Iterable) {
          // return first element (or null)
          Iterator<?> iter = ((Iterable<?>) field).iterator();
          return iter.hasNext() ? iter.next() : null;
        }

        return field;
      }

      throw new AssertionError("Shouldn't get here: " + id);

    }

    public Map<String, Object> source() {
      return source;
    }

    public Map<String, Object> fields() {
      return fields;
    }

    public Map<String, Object> sourceOrFields() {
      return source != null ? source : fields;
    }
  }

}

// End ElasticsearchSearchResult.java
