/*
// Licensed to Julian Hyde under one or more contributor license
// agreements. See the NOTICE file distributed with this work for
// additional information regarding copyright ownership.
//
// Julian Hyde licenses this file to you under the Apache License,
// Version 2.0 (the "License"); you may not use this file except in
// compliance with the License. You may obtain a copy of the License at:
//
// http://www.apache.org/licenses/LICENSE-2.0
//
// Unless required by applicable law or agreed to in writing, software
// distributed under the License is distributed on an "AS IS" BASIS,
// WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
// See the License for the specific language governing permissions and
// limitations under the License.
*/
package org.eigenbase.rel.metadata;

import java.util.*;

import org.eigenbase.rel.*;
import org.eigenbase.relopt.*;
import org.eigenbase.util.*;

/**
 * CachingRelMetadataProvider implements the {@link RelMetadataProvider}
 * interface by caching results from an underlying provider.
 */
public class CachingRelMetadataProvider implements RelMetadataProvider {
  //~ Instance fields --------------------------------------------------------

  private final Map<List, CacheEntry> cache;

  private final RelMetadataProvider underlyingProvider;

  private final RelOptPlanner planner;

  //~ Constructors -----------------------------------------------------------

  public CachingRelMetadataProvider(
      RelMetadataProvider underlyingProvider,
      RelOptPlanner planner) {
    this.underlyingProvider = underlyingProvider;
    this.planner = planner;

    cache = new HashMap<List, CacheEntry>();
  }

  //~ Methods ----------------------------------------------------------------

  // implement RelMetadataProvider
  public Object getRelMetadata(
      RelNode rel,
      String metadataQueryName,
      Object[] args) {
    // TODO jvs 30-Mar-2006: Use meta-metadata to decide which metadata
    // query results can stay fresh until the next Ice Age.

    // Compute hash key.
    List<Object> hashKey;
    if (args != null) {
      hashKey = new ArrayList<Object>(args.length + 2);
      hashKey.add(rel);
      hashKey.add(metadataQueryName);
      hashKey.addAll(Arrays.asList(args));
    } else {
      hashKey = Arrays.asList(rel, metadataQueryName);
    }

    long timestamp = planner.getRelMetadataTimestamp(rel);

    // Perform cache lookup.
    CacheEntry entry = cache.get(hashKey);
    if (entry != null) {
      if (timestamp == entry.timestamp) {
        return entry.result;
      } else {
        // Cache results are stale.
      }
    }

    // Cache miss or stale.
    Object result =
        underlyingProvider.getRelMetadata(
            rel,
            metadataQueryName,
            args);
    if (result != null) {
      entry = new CacheEntry();
      entry.timestamp = timestamp;
      entry.result = result;
      cache.put(hashKey, entry);
    }
    return result;
  }

  //~ Inner Classes ----------------------------------------------------------

  private static class CacheEntry {
    long timestamp;

    Object result;
  }
}

// End CachingRelMetadataProvider.java
