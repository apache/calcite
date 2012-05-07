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
package org.eigenbase.runtime;

import java.util.*;
import java.util.concurrent.*;
import java.util.logging.*;

import org.eigenbase.trace.*;
import org.eigenbase.util.*;


/**
 * Adapter that exposes a 'push' producer as an {@link Iterator}. Supports one
 * or more producers feeding into a single consumer. The consumer and the
 * producers must each run in its own thread. When there are several producers
 * the data is merged as it arrives: no sorting.
 *
 * <p>By default, the queue contains at most one object (implemented via {@link
 * SynchronousQueue}), but this can be customized by supplying an alternate
 * implementation (e.g. {@link ArrayBlockingQueue}) to the constructor. If you
 * call {@link #next}, your thread will wait until a producer thread calls
 * {@link #put} or {@link #done}. Nulls are allowed. If a producer has an error,
 * it can pass it to the consumer via {@link #done}.</p>
 *
 * @author jhyde
 * @version $Id$
 * @since Oct 20, 2003
 */
public class QueueIterator
    implements Iterator
{
    //~ Static fields/initializers ---------------------------------------------

    private static final WrappedNull WRAPPED_NULL = new WrappedNull();

    //~ Instance fields --------------------------------------------------------

    // NOTE: numProducers is the only state variable requiring synchronization.
    // All others are accessed only from the consumer end, which does not
    // support consumption from multiple threads.  (The queue itself provides
    // its own synchronization.)

    private int numProducers;

    // a wrapping class can provide its tracer, which is used here to trace
    // synchronization events
    private final EigenbaseLogger tracer;

    /**
     * next Iterator value (nulls are represented via #WRAPPED_NULL)
     */
    protected Object next;

    /**
     * false when Iterator is finished
     */
    protected boolean hasNext;
    protected Throwable throwable;

    protected BlockingQueue queue;

    //~ Constructors -----------------------------------------------------------

    /**
     * default constructor (one producer, no tracer, SynchronousQueue)
     */
    public QueueIterator()
    {
        this(1, null);
    }

    /**
     * @param n number of producers
     * @param tracer trace to this Logger, or null.
     */
    public QueueIterator(int n, Logger tracer)
    {
        this(n, tracer, null);
    }

    /**
     * @param n number of producers
     * @param tracer trace to this Logger, or null.
     * @param queue {@link BlockingQueue}  implementation, or null for default
     */
    public QueueIterator(int n, Logger tracer, BlockingQueue queue)
    {
        numProducers = n;
        this.tracer = (tracer == null) ? null : new EigenbaseLogger(tracer);

        if (queue == null) {
            this.queue = new SynchronousQueue();
        } else {
            this.queue = queue;
        }

        if (n == 0) {
            hasNext = false; // done now
            return;
        }
        hasNext = true;
    }

    //~ Methods ---------------------------------------------------------------

    public StringBuilder printStatus(StringBuilder b)
    {
        b.append(this);
        if (throwable != null) {
            b.append(" error: ").append(throwable);
        }
        if (hasNext) {
            if (queue instanceof ArrayBlockingQueue) {
                // Kludge - expect an ArrayBlockingQueue
                ArrayBlockingQueue abq = (ArrayBlockingQueue) queue;
                b.append(" size: ").append(abq.size());
            } else {
                b.append("size ?");
            }
        } else {
            b.append(" done");
        }
        return b;
    }

    protected void reset(int n)
    {
        hasNext = true;
        next = null;
        throwable = null;
        numProducers = n;
    }

    /**
     * Producer calls <code>done</code> to say that there are no more objects,
     * setting <code>throwable</code> if there was an error.
     */
    public void done(Throwable throwable)
    {
        EndOfQueue eoq = null;

        // NOTE:  synchronized can't be around queue.put or we'll deadlock
        synchronized (this) {
            numProducers--;
            if ((numProducers == 0) || (throwable != null)) {
                // shut down the iterator
                eoq = new EndOfQueue(throwable);
            }
        }

        if (eoq != null) {
            // put a dummy null to wake up the consumer, who will check
            // for
            try {
                queue.put(eoq);
            } catch (InterruptedException ex) {
                throw Util.newInternal(ex);
            }
        }
    }

    // implement Iterator
    public boolean hasNext()
    {
        if (!hasNext) {
            return false;
        }
        if (next == null) {
            try {
                next = queue.take();
            } catch (InterruptedException ex) {
                throw Util.newInternal(ex);
            }
        }
        checkTermination();
        return hasNext;
    }

    /**
     * As {@link #hasNext}, but throws {@link TimeoutException} if no row is
     * available within the timeout.
     *
     * @param timeoutMillis Milliseconds to wait; less than or equal to zero
     * means don't wait
     */
    public boolean hasNext(long timeoutMillis)
        throws TimeoutException
    {
        if (!hasNext) {
            return false;
        }
        if (next == null) {
            try {
                if (timeoutMillis <= 0) {
                    next = queue.poll();
                } else {
                    next = queue.poll(timeoutMillis, TimeUnit.MILLISECONDS);
                }
            } catch (InterruptedException ex) {
                throw Util.newInternal(ex);
            }
            if (next == null) {
                throw new TimeoutException();
            }
        }
        checkTermination();
        return hasNext;
    }

    // implement Iterator
    public Object next()
    {
        if (!hasNext()) {
            // It is illegal to call next when there are no more objects.
            throw new NoSuchElementException();
        }
        Object o = next;
        next = null;
        if (o == WRAPPED_NULL) {
            return null;
        } else {
            return o;
        }
    }

    /**
     * As {@link #next}, but throws {@link TimeoutException} if no row is
     * available within the timeout.
     *
     * @param timeoutMillis Milliseconds to wait; less than or equal to zero
     * means don't wait
     */
    public Object next(long timeoutMillis)
        throws TimeoutException
    {
        if (!hasNext(timeoutMillis)) {
            // It is illegal to call next when there are no more objects.
            throw new NoSuchElementException();
        }
        return next();
    }

    /**
     * Producer calls <code>put</code> to add another object (which may be
     * null).
     *
     * @param o object to put
     *
     * @throws IllegalStateException if this method is called after {@link
     * #done}
     */
    public void put(Object o)
    {
        if (!hasNext) {
            // It is illegal to add a new object after done() has been called.
            throw new IllegalStateException();
        }
        if (o == null) {
            o = WRAPPED_NULL;
        }
        try {
            queue.put(o);
        } catch (InterruptedException ex) {
            throw Util.newInternal(ex);
        }
    }

    /**
     * Producer calls <code>offer</code> to attempt to add another object (which
     * may be null) with a timeout.
     *
     * @param o object to offer
     * @param timeoutMillis Milliseconds to wait; less than or equal to zero
     * means don't wait
     *
     * @return true if offer accepted
     *
     * @throws IllegalStateException if this method is called after {@link
     * #done}
     */
    public boolean offer(Object o, long timeoutMillis)
    {
        if (!hasNext) {
            // It is illegal to add a new object after done() has been called.
            throw new IllegalStateException();
        }
        if (o == null) {
            o = WRAPPED_NULL;
        }
        try {
            if (timeoutMillis <= 0) {
                return queue.offer(o);
            } else {
                return queue.offer(o, timeoutMillis, TimeUnit.MILLISECONDS);
            }
        } catch (InterruptedException ex) {
            throw Util.newInternal(ex);
        }
    }

    // implement Iterator
    public void remove()
    {
        throw new UnsupportedOperationException();
    }

    /**
     * Checks for end-of-queue, and throws an error if one has been set via
     * {@link #done(Throwable)}.
     */
    protected void checkTermination()
    {
        if (!(next instanceof EndOfQueue)) {
            return;
        }
        EndOfQueue eoq = (EndOfQueue) next;
        next = null;
        hasNext = false;
        throwable = eoq.throwable;
        onEndOfQueue();
        if (throwable == null) {
            ;
        } else if (throwable instanceof RuntimeException) {
            throw (RuntimeException) throwable;
        } else if (throwable instanceof Error) {
            throw (Error) throwable;
        } else {
            throw new Error("error: " + throwable);
        }
    }

    /**
     * Called (from the consumer thread context) just before the iterator
     * returns false for hasNext(). Default implementation does nothing, but
     * subclasses can use this for cleanup actions.
     */
    protected void onEndOfQueue()
    {
    }

    //~ Inner Classes ----------------------------------------------------------

    /**
     * Thrown by {@link QueueIterator#hasNext(long)} and {@link
     * QueueIterator#next(long)} to indicate that operation timed out before
     * rows were available.
     */
    public static class TimeoutException
        extends Exception
    {
    }

    /**
     * Sentinel object.
     */
    private static class EndOfQueue
    {
        Throwable throwable;

        EndOfQueue(Throwable throwable)
        {
            this.throwable = throwable;
        }
    }

    /**
     * A null masquerading as a real object.
     */
    private static class WrappedNull
    {
    }
}

// End QueueIterator.java
