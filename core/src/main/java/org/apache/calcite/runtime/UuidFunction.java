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
package org.apache.calcite.runtime;

import org.apache.calcite.linq4j.function.Deterministic;

import java.security.SecureRandom;
import java.util.UUID;
import java.util.concurrent.atomic.AtomicLong;

/**
 * Function object for {@code UUIDV4} and {@code UUIDV7}.
 *
 * <p>{@code UUIDV4} generates a random (version 4) UUID as per RFC 4122.
 * {@code UUIDV7} generates a time-ordered (version 7) UUID as per RFC 9562,
 * using the <em>Monotonic Random</em> method (Section 6.2, Method 2):
 * <ul>
 *   <li>A 48-bit Unix timestamp (millisecond precision) occupies the high field.
 *   <li>Version = 7 in the 4-bit ver field.
 *   <li>The 12-bit rand_a field is seeded freshly (random) at the start of each
 *       millisecond, then acts as a monotonically incrementing counter within
 *       the same millisecond, guaranteeing strict ordering.
 *   <li>Variant = 0b10 (IETF RFC 4122 / RFC 9562).
 *   <li>62 fresh random bits occupy rand_b.
 * </ul>
 *
 * <p>Monotonicity and thread-safety are achieved without a lock: the timestamp
 * and rand_a counter are packed into a single {@link AtomicLong} and updated
 * via a compare-and-swap (CAS) retry loop.
 *
 * <p>State packing layout (64 bits):
 * <pre>
 *  [63:12]  last unix_ts_ms  (52 bits, enough for ~142 million years)
 *  [11: 0]  rand_a counter   (12 bits, 0–4095)
 * </pre>
 *
 * <p>Marked {@link Deterministic} so that the code generator instantiates one
 * instance per query, not once per row.
 */
@SuppressWarnings("unused")
public class UuidFunction {

  /** Source of random bits.
   *
   * <p>RFC 9562 section 6.9 recommends that implementations use a
   * cryptographically secure pseudorandom number generator, so that generated
   * values are unguessable. {@link SecureRandom} is thread-safe, and is the
   * same source that {@link UUID#randomUUID()} uses for {@code UUIDV4}. */
  private static final SecureRandom RANDOM = new SecureRandom();

  /** Bitmask for the 12-bit rand_a field (bits [11:0]). */
  private static final long RAND_A_MASK = 0x0FFFL;

  /** Bitmask for the 62-bit rand_b field (bits [61:0]). */
  private static final long RAND_B_MASK = 0x3FFF_FFFF_FFFF_FFFFL;

  /** Bit pattern that encodes UUID version 7 in bits [15:12] of the MSB. */
  private static final long VERSION_7 = 0x7000L;

  /** Bit pattern that encodes the IETF variant (0b10) in bits [63:62] of the LSB. */
  private static final long VARIANT_IETF = 0x8000_0000_0000_0000L;

  /**
   * Packed monotonic state: high 52 bits = last timestamp (ms), low 12 bits = seqA.
   * Initial value 0 ensures the first call's real timestamp always exceeds it,
   * triggering a fresh random seed for rand_a.
   */
  private final AtomicLong state = new AtomicLong(0L);

  /** Creates a UuidFunction.
   *
   * <p>Marked deterministic so that the code generator instantiates one once
   * per query, not once per row. */
  @Deterministic public UuidFunction() {
  }

  /** Implements the {@code UUIDV4()} SQL function.
   * Returns a random (version 4) UUID as per RFC 4122. */
  public UUID uuidv4() {
    return UUID.randomUUID();
  }

  /**
   * Implements the {@code UUIDV7()} SQL function.
   * Returns a time-ordered (version 7) UUID as per RFC 9562.
   *
   * <p>128-bit layout (MSB → LSB):
   * <pre>
   * |&lt;--- unix_ts_ms (48) ---&gt;| ver(4)=7 |&lt;rand_a(12)&gt;|
   * | var(2)=10 |&lt;------------ rand_b (62) ------------&gt;|
   * </pre>
   *
   * <p>Thread-safety is achieved lock-free via a CAS loop on {@link #state}.
   * In the common case (no contention) the loop executes exactly once.
   */
  public UUID uuidv7() {
    long ms;
    long seqA;
    long current;
    long next;
    do {
      current = state.get();
      final long lastMs = current >>> 12;     // high 52 bits
      final long lastSeq = current & RAND_A_MASK; // low 12 bits
      ms = System.currentTimeMillis();
      if (ms > lastMs) {
        // New millisecond: advance the clock and seed rand_a randomly so that
        // the initial value of rand_a for this ms is unpredictable.
        seqA = RANDOM.nextLong() & RAND_A_MASK;
      } else {
        // Same millisecond (or a clock regression): increment the counter to
        // preserve monotonic order.
        ms = lastMs;
        seqA = lastSeq + 1;
        if (seqA > RAND_A_MASK) {
          // rand_a overflowed 12 bits; bump the logical clock by 1 ms.
          ms = lastMs + 1;
          seqA = 0;
        }
      }
      next = (ms << 12) | seqA;
      // CAS: if another thread has already updated state, retry with fresh reads.
    } while (!state.compareAndSet(current, next));

    // MSB: unix_ts_ms (48) | version 7 (4) | rand_a (12)
    final long msb = (ms << 16) | VERSION_7 | seqA;
    // LSB: IETF variant (2) | rand_b (62) — always fresh random bits
    final long lsb = VARIANT_IETF | (RANDOM.nextLong() & RAND_B_MASK);
    return new UUID(msb, lsb);
  }
}
