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
package org.apache.calcite.test.schemata.hr;

import org.checkerframework.checker.nullness.qual.Nullable;

import java.sql.Timestamp;
import java.util.Objects;

/**
 * Event.
 */
public class Event {
  public final int eventid;
  public final @Nullable Timestamp ts;

  public Event(int eventid, @Nullable Timestamp ts) {
    this.eventid = eventid;
    this.ts = ts;
  }

  @Override public String toString() {
    return "Event [eventid: " + eventid + ", ts: " + ts + "]";
  }

  @Override public boolean equals(Object obj) {
    return obj == this
        || obj instanceof Event
        && eventid == ((Event) obj).eventid;
  }

  @Override public int hashCode() {
    return Objects.hash(eventid);
  }
}
