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
package org.apache.calcite.tools.visualizer;

import java.util.Collection;
import java.util.Map;
import java.util.Objects;

public class VisualizerTvrInfo {

  public String tvrId;
  // Map<TvrSemantics, RelSet>
  public Map<String, Collection<String>> tvrSets;
  // Map<TvrProperty, TvrMetaSet>
  public Map<String, Collection<String>> tvrPropertyLinks;

  public VisualizerTvrInfo() {}

  public VisualizerTvrInfo(String tvrId,
      Map<String, Collection<String>> tvrSets,
      Map<String, Collection<String>> tvrPropertyLinks) {
    this.tvrId = tvrId;
    this.tvrSets = tvrSets;
    this.tvrPropertyLinks = tvrPropertyLinks;
  }

  @Override public boolean equals(Object o) {
    if (this == o) {
      return true;
    }
    if (!(o instanceof VisualizerTvrInfo)) {
      return false;
    }
    VisualizerTvrInfo that = (VisualizerTvrInfo) o;
    return tvrId == that.tvrId && Objects.equals(tvrSets, that.tvrSets)
        && Objects.equals(tvrPropertyLinks, that.tvrPropertyLinks);
  }

  @Override public int hashCode() {
    return Objects.hash(tvrId, tvrSets, tvrPropertyLinks);
  }
}
