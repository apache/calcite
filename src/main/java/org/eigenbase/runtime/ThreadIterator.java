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
import java.util.concurrent.*;


/**
 * <code>ThreadIterator</code> converts 'push' code to 'pull'. You implement
 * {@link #doWork} to call {@link #put} with each row, and this class invokes it
 * in a separate thread. Then the results come out via the familiar {@link
 * Iterator} interface. For example,
 *
 * <blockquote>
 * <pre>class ArrayIterator extends ThreadIterator {
 *   Object[] a;
 *   ArrayIterator(Object[] a) {
 *     this.a = a;
 *     start();
 *   }
 *   protected void doWork() {
 *     for (int i = 0; i < a.length; i++) {
 *       put(a[i]);
 *     }
 *   }
 * }</pre>
 * </blockquote>
 *
 * Or, more typically, using an anonymous class:
 *
 * <blockquote>
 * <pre>Iterator i = new ThreadIterator() {
 *   int limit;
 *   public ThreadIterator start(int limit) {
 *     this.limit = limit;
 *     return super.start();
 *   }
 *   protected void doWork() {
 *     for (int i = 0; i < limit; i++) {
 *       put(new Integer(i));
 *     }
 *   }
 * }.start(100);
 * while (i.hasNext()) {
 *   <em>etc.</em>
 * }</pre>
 * </blockquote>
 */
public abstract class ThreadIterator
    extends QueueIterator
    implements Iterator,
        Runnable,
        Iterable
{
    //~ Instance fields --------------------------------------------------------

    private Thread thread;
    private String threadName;

    //~ Constructors -----------------------------------------------------------

    public ThreadIterator()
    {
    }

    public ThreadIterator(BlockingQueue queue)
    {
        super(1, null, queue);
    }

    //~ Methods ----------------------------------------------------------------

    public void setThreadName(String s)
    {
        threadName = s;
        if (thread != null) {
            thread.setName(threadName);
        }
    }

    // implement Iterable
    public Iterator iterator()
    {
        return start();
    }

    // implement Runnable
    public void run()
    {
        boolean calledDone = false;
        try {
            doWork();
        } catch (Throwable e) {
            done(e);
            calledDone = true;
        } finally {
            if (!calledDone) {
                done(null);
            }
        }
    }

    /**
     * The implementation should call {@link #put} with each row.
     */
    protected abstract void doWork();

    protected ThreadIterator start()
    {
        assert (thread == null);
        thread = new Thread(this);
        if (threadName != null) {
            thread.setName(threadName);
        }

        // Make the thread a daemon so that we don't have to worry
        // about cleaning it up.  This is important since we can't
        // be guaranteed that onClose will get called (someone
        // may create an iterator and then forget about it), so
        // requiring a join() call would be a bad idea.  Of course,
        // if someone does forget about it, and the producer
        // thread gets stuck on a full queue, it will never exit,
        // and will become a resource leak.
        thread.setDaemon(true);
        thread.start();
        return this;
    }

    // implement QueueIterator
    protected void onEndOfQueue()
    {
        thread = null;
    }
}

// End ThreadIterator.java
