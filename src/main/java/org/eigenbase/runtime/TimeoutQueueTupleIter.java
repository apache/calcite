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

import org.eigenbase.util.*;


/**
 * Adapter which allows you to iterate over an {@link TupleIter} with a timeout.
 *
 * <p>The interface is similar to an {@link TupleIter}: the {@link #fetchNext}
 * method retrieves rows and indicates when there are no more rows. It has a
 * timeout parameter, and throws a {@link QueueIterator.TimeoutException} if the
 * timeout is exceeded. There is also a {@link #closeAllocation} method, which
 * you must call.
 *
 * <p>The class is implemented using a thread which reads from the underlying
 * TupleIter and places the results into a {@link QueueIterator}. If a method
 * call times out, the underlying thread will wait for the result of the call
 * until it completes.
 *
 * <p>There is no facility to cancel the fetch from the underlying iterator.
 *
 * <p><b>Reader/writer synchronization and the {@link #FENCEPOST}.</b> The
 * thread within this class that reads row objects from the underlying
 * TupleIter(s) must be careful not to read a subsequent row until the reading
 * thread (e.g., the driver) is finished with the row. This is because the same
 * row object may be re-used for subsequent rows. To achieve this, this class's
 * thread always inserts {@link #FENCEPOST} after every row object and the
 * {@link #fetchNext} method detects and discards the fencepost. The nature of
 * the underlying {@link QueueIterator}'s SynchronousQueue prevents the writing
 * thread from completing the put operation of the fencepost until the reading
 * thread is prepared to read the value. In this way we guarantee that the row
 * object is not modified until the reader has requested the next row object, at
 * which point we assume it's safe to modify the row object.
 *
 * @author Stephan Zuecher (based on tleung's TimeoutQueueIterator)
 * @version $Id$
 */
public class TimeoutQueueTupleIter
{
    //~ Static fields/initializers ---------------------------------------------

    /**
     * Prevents reader's row object from being clobbered by the next row. See
     * class description for how this works.
     */
    private static final Fencepost FENCEPOST = new Fencepost();

    //~ Instance fields --------------------------------------------------------

    private final QueueIterator queueIterator;
    private final TupleIter producer;
    private Thread thread;

    //~ Constructors -----------------------------------------------------------

    public TimeoutQueueTupleIter(TupleIter producer)
    {
        this.producer = producer;
        this.queueIterator = new QueueIterator();
    }

    //~ Methods ----------------------------------------------------------------

    /**
     * Retrieve the next row from the underlying TupleIter, with the given
     * timeout, in milliseconds.
     *
     * <p>See class description re: {@link #FENCEPOST}.
     *
     * @param timeoutMillis number of milliseconds to wait for the next row;
     * less than or equal to 0 means do not wait
     *
     * @return next row
     *
     * @throws org.eigenbase.runtime.QueueIterator.TimeoutException on timeout
     */
    public Object fetchNext(long timeoutMillis)
        throws QueueIterator.TimeoutException
    {
        // REVIEW: SWZ: 7/13/2006: A particularly timeout particularly
        // close to the amount of time it takes to fetch a row may
        // cause problems due to the fencepost objects.  Perhaps we
        // should reset the timeout when we find a fencepost object?
        // Then again, fetch time is in no way guaranteed constant, so
        // the timeout is probably to close for comfort even if we
        // reset.

        long endTime = System.currentTimeMillis() + timeoutMillis;
        while (queueIterator.hasNext(timeoutMillis)) {
            long remainingTimeout = endTime - System.currentTimeMillis();
            if (remainingTimeout <= 0) {
                // hasNext() took too long
                throw new QueueIterator.TimeoutException();
            }

            Object result = queueIterator.next(remainingTimeout);

            if (result != FENCEPOST) {
                return result;
            }
        }

        return TupleIter.NoDataReason.END_OF_DATA;
    }

    /**
     * Starts the thread which reads from the consumer.
     *
     * @pre thread == null // not previously started
     */
    public synchronized void start()
    {
        Util.pre(thread == null, "thread == null");
        thread =
            new Thread() {
                public void run()
                {
                    doWork();
                }
            };
        thread.setName("TimeoutQueueTupleIter" + thread.getName());
        thread.start();
    }

    /**
     * Releases the resources used by this iterator, including killing the
     * underlying thread.
     *
     * @param timeoutMillis Timeout while waiting for the underlying thread to
     * die. Zero means wait forever.
     */
    public synchronized void closeAllocation(long timeoutMillis)
    {
        if (thread != null) {
            try {
                // Empty the queue -- the thread will wait for us to consume
                // all items in the queue, hanging the join call.
                while (queueIterator.hasNext()) {
                    queueIterator.next();
                }
                thread.join(timeoutMillis);
            } catch (InterruptedException e) {
                // ignore } catch (QueueIterator.TimeoutException e) { not
                // actually possible - because hasNext(timeout=0) means to poll
            }
            thread = null;
        }
    }

    /**
     * Reads objects from the producer and writes them into the QueueIterator.
     * This is the method called by the thread when you call {@link #start}.
     * Never throws an exception.
     *
     * <p>See class description re: {@link #FENCEPOST}.
     */
    private void doWork()
    {
        try {
            while (true) {
                Object next = producer.fetchNext();

                if (next == TupleIter.NoDataReason.END_OF_DATA) {
                    break;
                } else if (next instanceof TupleIter.NoDataReason) {
                    // TODO: SWZ: 2/23/2006: Better exception
                    throw new RuntimeException();
                }

                // Insert the object and then a fencepost.
                queueIterator.put(next);
                queueIterator.put(FENCEPOST);
            }

            // Signal that the stream ended without error.
            queueIterator.done(null);
        } catch (Throwable e) {
            // Signal that the stream ended with an error.
            queueIterator.done(e);
        }
    }

    //~ Inner Classes ----------------------------------------------------------

    private static class Fencepost
    {
        public String toString()
        {
            return "FENCEPOST_DUMMY";
        }
    }
}

// End TimeoutQueueTupleIter.java
