/*
// Licensed to DynamoBI Corporation (DynamoBI) under one
// or more contributor license agreements.  See the NOTICE file
// distributed with this work for additional information
// regarding copyright ownership.  DynamoBI licenses this file
// to you under the Apache License, Version 2.0 (the
// "License"); you may not use this file except in compliance
// with the License.  You may obtain a copy of the License at

//   http://www.apache.org/licenses/LICENSE-2.0

// Unless required by applicable law or agreed to in writing,
// software distributed under the License is distributed on an
// "AS IS" BASIS, WITHOUT WARRANTIES OR CONDITIONS OF ANY
// KIND, either express or implied.  See the License for the
// specific language governing permissions and limitations
// under the License.
*/
package org.eigenbase.runtime;

/**
 * A synchronization primitive which allows a producer and a consumer to use the
 * same resources without treading on each other's feet.
 *
 * <p>At most one of the producer and consumer has access at a time. The
 * synchronization ensures that the call sequence is as follows:
 *
 * <ul><li{@link #beginWriting()} (called by producer) <li{@link #endWriting()}
 * (called by producer) <li{@link #beginReading()} (called by consumer) <li
 * {@link #endReading()} (called by consumer)
 * </ul>
 *
 * <p>{@link ExclusivePipe} is a simple extension to this class containing a
 * {@link java.nio.ByteBuffer} as the shared resource.
 *
 * @author jhyde
 * @version $Id$
 */
public class Interlock
{
    //~ Instance fields --------------------------------------------------------

    /**
     * The producer notifies <code>empty</code> every time it finishes writing.
     * The consumer waits for it.
     */
    private final Semaphore empty = new Semaphore(0);

    /**
     * The consumer notifies <code>full</code> every time it finishes reading.
     * The producer waits for it, then starts work.
     */
    private final Semaphore full = new Semaphore(1);

    //~ Constructors -----------------------------------------------------------

    public Interlock()
    {
    }

    //~ Methods ----------------------------------------------------------------

    /**
     * Acquires the buffer, in preparation for writing.
     *
     * <p>The producer should call this method. After this call completes, the
     * consumer's call to {@link #beginReading()} will block until the producer
     * has called {@link #endWriting()}.
     */
    public void beginWriting()
    {
        full.acquire(); // wait for consumer thread to use previous
    }

    /**
     * Releases the buffer after writing.
     *
     * <p>The producer should call this method. After this call completes, the
     * producers's call to {@link #beginWriting()} will block until the consumer
     * has called {@link #beginReading()} followed by {@link #endReading()}.
     */
    public void endWriting()
    {
        empty.release(); // wake up consumer
    }

    /**
     * Acquires the buffer, in preparation for reading.
     *
     * <p>After this call completes, the producer's call to {@link
     * #beginWriting()} will block until the consumer has called {@link
     * #endReading()}.
     */
    public void beginReading()
    {
        empty.acquire(); // wait for producer to produce one
    }

    /**
     * Releases the buffer after reading its contents.
     *
     * <p>The consumer should call this method. After this call completes, the
     * consumer's call to {@link #beginReading()} will block until the producer
     * has called {@link #beginWriting()} followed by {@link #endWriting()}.
     */
    public void endReading()
    {
        full.release(); // wake up producer
    }
}

// End Interlock.java
