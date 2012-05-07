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

import java.util.*;


/**
 * A counting semaphore. Conceptually, a semaphore maintains a set of permits.
 * Each {@link #acquire()} blocks if necessary until a permit is available, and
 * then takes it. Each {@link #release()} adds a permit, potentially releasing a
 * blocking acquirer. However, no actual permit objects are used; the Semaphore
 * just keeps a count of the number available and acts accordingly.
 *
 * <p>Semaphores are often used to restrict the number of threads than can
 * access some (physical or logical) resource.
 *
 * <p>Note that JDK 1.5 contains <a
 * href="http://java.sun.com/j2se/1.5.0/docs/api/java/util/concurrent/Semaphore.html">
 * a Semaphore class</a>. We should obsolete this class when we upgrade.
 *
 * @author jhyde
 * @version $Id$
 */
public class Semaphore
{
    //~ Static fields/initializers ---------------------------------------------

    private static final boolean verbose = false;

    //~ Instance fields --------------------------------------------------------

    private int count;

    //~ Constructors -----------------------------------------------------------

    /**
     * Creates a Semaphore with the given number of permits.
     */
    public Semaphore(int count)
    {
        this.count = count;
    }

    //~ Methods ----------------------------------------------------------------

    /**
     * Acquires a permit from this semaphore, blocking until one is available.
     */
    public synchronized void acquire()
    {
        // REVIEW (jhyde, 2004/7/23): the JDK 1.5 Semaphore class throws
        //   InterruptedException; maybe we should too.
        while (count <= 0) {
            try {
                wait();
            } catch (InterruptedException e) {
            }
        }

        // we have control, decrement the count
        count--;
    }

    /**
     * Acquires a permit from this semaphore, if one becomes available within
     * the given waiting time.
     *
     * <p>If timeoutMillisec is less than or equal to zero, does not wait at
     * all.
     */
    public synchronized boolean tryAcquire(long timeoutMillisec)
    {
        long enterTime = System.currentTimeMillis();
        long endTime = enterTime + timeoutMillisec;
        long currentTime = enterTime;
        if (verbose) {
            System.out.println(
                "tryAcquire: enter=" + (enterTime % 100000)
                + ", timeout=" + timeoutMillisec + ", count=" + count
                + ", this=" + this + ", date=" + new Date());
        }

        while ((count <= 0) && (currentTime < endTime)) {
            // REVIEW (jhyde, 2004/7/23): the equivalent method in the JDK 1.5
            //   Semaphore class throws InterruptedException; maybe we should
            //   too.
            try {
                // Note that wait(0) means no timeout (wait forever), whereas
                // tryAcquire(0) means don't wait
                assert (endTime - currentTime) > 0
                    : "wait(0) means no timeout!";
                wait(endTime - currentTime);
            } catch (InterruptedException e) {
            }
            currentTime = System.currentTimeMillis();
        }

        if (verbose) {
            System.out.println(
                "enter=" + (enterTime % 100000) + ", now="
                + (currentTime % 100000) + ", end=" + (endTime % 100000)
                + ", timeout=" + timeoutMillisec + ", remain="
                + (endTime - currentTime) + ", count=" + count + ", this="
                + this + ", date=" + new Date());
        }

        // we may have either been timed out or notified
        // let's check which is the case
        if (count <= 0) {
            if (verbose) {
                System.out.println("false");
            }

            // lock still not released - we were timed out!
            return false;
        } else {
            if (verbose) {
                System.out.println("true");
            }

            // we have control, decrement the count
            count--;
            return true;
        }
    }

    /**
     * Releases a permit, returning it to the semaphore.
     */
    public synchronized void release()
    {
        count++;
        notify();
    }
}

// End Semaphore.java
