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
package org.apache.calcite.adapter.govdata.sec;

import org.junit.jupiter.api.Disabled;
import org.junit.jupiter.api.Tag;
import org.junit.jupiter.api.Test;

import java.util.HashSet;
import java.util.List;
import java.util.Set;

import static org.junit.jupiter.api.Assertions.*;

/**
 * Tests for market cap-based CIK markers.
 */
@Tag("unit")
public class MarketCapMarkersTest {

  @Test @Disabled("Requires API access")
  public void testGlobalMegaCap() {
    List<String> megaCap = CikRegistry.resolveCiks("_GLOBAL_MEGA_CAP");
    assertNotNull(megaCap);
    assertFalse(megaCap.isEmpty(), "Global mega-cap should have results");
    System.out.println("Global Mega-Cap (>$200B): " + megaCap.size() + " companies");
  }

  @Test @Disabled("Requires API access")
  public void testUSLargeCap() {
    List<String> largeCap = CikRegistry.resolveCiks("_US_LARGE_CAP");
    assertNotNull(largeCap);
    assertFalse(largeCap.isEmpty(), "US large-cap should have results");
    System.out.println("US Large-Cap (>$10B): " + largeCap.size() + " companies");
  }

  @Test @Disabled("Requires API access")
  public void testUSMidCap() {
    List<String> midCap = CikRegistry.resolveCiks("_US_MID_CAP");
    assertNotNull(midCap);
    assertFalse(midCap.isEmpty(), "US mid-cap should have results");
    System.out.println("US Mid-Cap ($2B-$10B): " + midCap.size() + " companies");
  }

  @Test @Disabled("Requires API access")
  public void testUSSmallCap() {
    List<String> smallCap = CikRegistry.resolveCiks("_US_SMALL_CAP");
    assertNotNull(smallCap);
    assertFalse(smallCap.isEmpty(), "US small-cap should have results");
    System.out.println("US Small-Cap ($300M-$2B): " + smallCap.size() + " companies");
  }

  @Test @Disabled("Requires API access")
  public void testUSMicroCap() {
    List<String> microCap = CikRegistry.resolveCiks("_US_MICRO_CAP");
    assertNotNull(microCap);
    assertFalse(microCap.isEmpty(), "US micro-cap should have results");
    System.out.println("US Micro-Cap (<$300M): " + microCap.size() + " companies");
  }

  @Test @Disabled("Requires API access")
  public void testNoOverlapBetweenTiers() {
    List<String> midCap = CikRegistry.resolveCiks("_US_MID_CAP");
    List<String> smallCap = CikRegistry.resolveCiks("_US_SMALL_CAP");
    List<String> microCap = CikRegistry.resolveCiks("_US_MICRO_CAP");

    Set<String> midCapSet = new HashSet<>(midCap);
    Set<String> smallCapSet = new HashSet<>(smallCap);
    Set<String> microCapSet = new HashSet<>(microCap);

    // Check no overlap between mid and small
    Set<String> midSmallOverlap = new HashSet<>(midCapSet);
    midSmallOverlap.retainAll(smallCapSet);
    assertTrue(midSmallOverlap.isEmpty(), "Mid-cap and small-cap should not overlap");

    // Check no overlap between small and micro
    Set<String> smallMicroOverlap = new HashSet<>(smallCapSet);
    smallMicroOverlap.retainAll(microCapSet);
    assertTrue(smallMicroOverlap.isEmpty(), "Small-cap and micro-cap should not overlap");
  }

  @Test @Disabled("Requires API access")
  public void testMegaCapIsSubsetOfLargeCap() {
    List<String> megaCap = CikRegistry.resolveCiks("_GLOBAL_MEGA_CAP");
    List<String> largeCap = CikRegistry.resolveCiks("_US_LARGE_CAP");

    Set<String> largeCapSet = new HashSet<>(largeCap);

    int megaInLarge = 0;
    for (String cik : megaCap) {
      if (largeCapSet.contains(cik)) {
        megaInLarge++;
      }
    }

    // Most mega-cap companies should be in large-cap
    double coverage = (megaInLarge * 100.0) / megaCap.size();
    assertTrue(coverage > 80, "Most mega-cap companies should be in large-cap");
    System.out.println("Mega-cap coverage in large-cap: " +
                       String.format("%.1f%%", coverage));
  }
}
