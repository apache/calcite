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
package org.eigenbase.util;

import java.util.*;

/**
 * ArrayQueue is a queue implementation backed by an array. Grows by doubling
 * the existing size, but never shrinks. Queue entries are allowed to wrap
 * around array boundaries. ArrayQueue does not allow <code>null</code> entries.
 *
 * <p>Contains the necessary methods to implement JDK 1.5's Queue interface.
 * Also, some methods can be removed by extending JDK 1.5's AbstractQueue class.
 *
 * <p>{@link #offer(Object) Offering} (adding) items to the queue, {@link
 * #poll() polling} (removing) items from the queue, and {@link #peek() peeking}
 * at the head of the queue are normally constant time operations. The exception
 * is that growing the queue is an O(N) operation and can occur when offering an
 * item to the queue.
 *
 * <p>The {@link Iterator} returned by {@link #iterator()} behaves somewhat
 * inconsistently with the general contract of Iterator. Read the documentation
 * for that method carefully.
 */
public class ArrayQueue<E>
    extends AbstractCollection<E>
    implements Collection<E>
{
    //~ Static fields/initializers ---------------------------------------------

    private static final int DEFAULT_CAPACITY = 10;

    //~ Instance fields --------------------------------------------------------

    /**
     * The current capacity (not size) of this queue. Equal to <code>{link
     * #queue}.length</code>.
     */
    private int capacity;

    /**
     * The queue contents. Treated as a circular buffer.
     */
    private E [] queue;

    /**
     * The current position of the head element of the queue.
     */
    private int start;

    /**
     * The current position for the next element added to the queue.
     */
    private int end;

    //~ Constructors -----------------------------------------------------------

    /**
     * Constructs an empty ArrayQueue with the specified initial capacity.
     *
     * @param capacity the initial capacity of this queue
     */
    public ArrayQueue(int capacity)
    {
        if (capacity <= 0) {
            throw new IllegalArgumentException();
        }
        this.capacity = capacity;
        this.queue = (E []) new Object[capacity];
        this.start = 0;
        this.end = 0;
    }

    /**
     * Constructs an empty ArrayQueue with the default initial capacity of
     * DEFAULT_CAPACITY.
     */
    public ArrayQueue()
    {
        this(DEFAULT_CAPACITY);
    }

    /**
     * Constructs an ArrayQueue with the given contents. The initial capacity of
     * the queue is {@link #DEFAULT_CAPACITY 10} or {@link Collection#size()
     * c.size()} whichever is larger. The queue is populated with the elements
     * of <code>c</code> in the order in which <code>c</code>'s iterator returns
     * them.
     *
     * @param c a collection to use as the default contents of the queue
     *
     * @throws NullPointerException if c or any of its elements are null
     */
    public ArrayQueue(Collection<? extends E> c)
    {
        this(Math.max(DEFAULT_CAPACITY, c.size()));

        addAll(c);
    }

    /**
     * Constructs an ArrayQueue with the given contents and initial capacity. If
     * <code>capacity</code> is smaller than {@link Collection#size() c.size()},
     * the initial capacity will be <code>c.size()</code>. The queue is
     * populated with the elements of <code>c</code> in the order in which
     * <code>c</code>'s iterator returns them.
     *
     * @param capacity the initial capacity of this queue
     * @param c a collection to use as the default contents of the queue
     *
     * @throws NullPointerException if c or any of its elements are null
     */
    public ArrayQueue(int capacity, Collection<? extends E> c)
    {
        this(Math.max(capacity, c.size()));

        addAll(c);
    }

    //~ Methods ----------------------------------------------------------------

    /**
     * Inserts the specified element into this queue. The queue's capacity may
     * grow as a result of this call.
     *
     * @param o the element to insert
     *
     * @return <code>false</code> if o is <code>null</code>, otherwise <code>
     * true</code> since it's always possible to add an element to this queue.
     */
    public boolean offer(E o)
    {
        if (o == null) {
            return false;
        }

        int newEnd = increment(end);
        if (newEnd == start) {
            // queue is full, allocate more space
            grow();
            queue[end] = o;
            end = increment(end); // cannot assume end is the same after grow()
        } else {
            queue[end] = o;
            end = newEnd;
        }

        return true;
    }

    /**
     * Retrieves, but does not remove the head of this queue, returning <code>
     * null</code> if this queue is empty.
     *
     * @return the head of the queue or <code>null</code> if the queue is empty
     */
    public E peek()
    {
        if (start == end) {
            return null;
        }

        return queue[start];
    }

    /**
     * Retrieves and removes the head of this queue, returning <code>null</code>
     * if this queue is empty.
     *
     * @return the head of the queue or <code>null</code> if the queue is empty
     */
    public E poll()
    {
        if (start == end) {
            return null;
        }

        E result = queue[start];
        queue[start] = null; // Let the "result" be GCed as soon as possible
        start = increment(start);
        return result;
    }

    /**
     * Returns the number of elements currently in the queue.
     *
     * @return the number of elements currently in the queue
     */
    public int size()
    {
        if (end < start) {
            return (capacity - start) + end;
        } else {
            return end - start;
        }
    }

    /**
     * Returns an iterator over the elements in the queue in proper sequence.
     * The returned <code>Iterator</code> is a "weakly consistent" iterator. It
     * will never throw <code>ConcurrentModificationException</code> and
     * guarantees to traverse elements as they existed upon construction of the
     * iterator, but will never reflect any modifications subsequent to
     * construction.
     *
     * @return an iterator over the elements in this queue in proper order
     */
    public Iterator<E> iterator()
    {
        E [] contents = (E []) new Object[size()];

        copyQueueToArray(contents);

        return Arrays.asList(contents).iterator();
    }

    /**
     * Unsupported operation.
     */
    public boolean remove(Object o)
    {
        throw new UnsupportedOperationException();
    }

    /**
     * Unsupported operation.
     */
    public boolean removeAll(Collection c)
    {
        throw new UnsupportedOperationException();
    }

    /**
     * Unsupported operation.
     */
    public boolean retainAll(Collection<?> c)
    {
        throw new UnsupportedOperationException();
    }

    /**
     * Grows the queue to twice the current capacity.
     */
    private void grow()
    {
        int size = size();

        int largerCapacity = capacity * 2;
        E [] largerQueue = (E []) new Object[largerCapacity];
        copyQueueToArray(largerQueue);

        queue = largerQueue;
        start = 0;
        end = size;
        capacity = largerCapacity;
    }

    /**
     * Compares two queues for equality. The queues are not modified by this
     * method. Concurrent modification of either this queue or the one being
     * compared to has undefined results. Each element, in the proper order,
     * must match in the two queues using the elements' <code>equals</code>
     * method.
     *
     * @param o the queue to compare this queue to
     *
     * @return true if the queues have the same elements in the same order,
     * false otherwise
     *
     * @throws ClassCastException if <code>o</code> is not an ArrayQueue.
     */
    public boolean equals(Object o)
    {
        if (!(o instanceof ArrayQueue)) {
            return false;
        }
        ArrayQueue<E> oq = (ArrayQueue<E>) o;

        if (size() != oq.size()) {
            return false;
        }

        int s = start;
        int os = oq.start;
        while ((s != end) && (os != oq.end)) {
            if (!queue[s].equals(oq.queue[os])) {
                return false;
            }

            s = increment(s);
            os = oq.increment(os);
        }

        return true;
    }

    /**
     * Copies the contents of the queue into an array. The elements are copied
     * such that the first element of the queue ends up in <code>
     * otherQueue[0]</code>. Elements are copied in order.
     *
     * @param otherQueue the array to copy data into, <code>
     * otherQueue.length</code> must be greater than or equal to {@link
     * #size()}.
     */
    private void copyQueueToArray(E [] otherQueue)
    {
        assert (otherQueue.length >= size());

        if (end < start) {
            System.arraycopy(queue, start, otherQueue, 0, capacity - start);
            if (end > 0) {
                System.arraycopy(queue, 0, otherQueue, capacity - start, end);
            }
        } else {
            System.arraycopy(queue, start, otherQueue, 0, end - start);
        }
    }

    /**
     * Increments the given index by one modulo the queue's capacity.
     *
     * @param index the index value to increment
     *
     * @return the index mod {@link #capacity}
     */
    private int increment(int index)
    {
        index++;
        if (index >= capacity) {
            index = 0;
        }

        return index;
    }

    /**
     * Adds the specified element to this queue. This implementation returns
     * true if offer succeeds, else throws an IllegalStateException.
     *
     * @param o the element
     *
     * @return true (as per the general contract of {@link
     * Collection#add(Object)}).
     *
     * @throws NullPointerException if o is <code>null</code>
     * @throws IllegalStateException if the call to {@link #offer(Object)} fails
     */
    public boolean add(E o)
    {
        if (o == null) {
            throw new NullPointerException();
        }

        if (offer(o)) {
            return true;
        }

        throw new IllegalStateException();
    }

    /**
     * Adds all of the elements in the specified collection to this queue.
     * Attempts to addAll of a queue to itself result in
     * IllegalArgumentException. Further, the behavior of this operation is
     * undefined if the specified collection is modified while the operation is
     * in progress.
     *
     * <p>This implementation iterates over the specified collection, and adds
     * each element returned by the iterator to this collection, in turn. A
     * runtime exception encountered while trying to add an element (including,
     * in particular, a <code>null</code> element) may result in only some of
     * the elements having been successfully added when the associated exception
     * is thrown.
     *
     * @param c collection to add to the queue
     *
     * @return true if this queue changed as a result of the call
     *
     * @throws IllegalArgumentException if <code>this == c</code>
     * @throws NullPointerException if <code>c</code> or any of its elements are
     * <code>null</code>.
     * @throws IllegalStateException if the call to {@link #add(Object)} does
     */
    public boolean addAll(Collection<? extends E> c)
    {
        if (c == this) {
            throw new IllegalArgumentException();
        }

        boolean result = false;
        for (Iterator<? extends E> i = c.iterator(); i.hasNext();) {
            result = add(i.next());
        }

        return result;
    }

    /**
     * Removes all elements from the queue. The queue will be empty and contain
     * no references to its previous contents after this call returns. This
     * method calls {@link #poll()} repeatedly until it returns <code>
     * null</code>.
     */
    public void clear()
    {
        while (poll() != null) {
            ;
        }
    }

    /**
     * Retrieves, but does not remove, the head of the queue. Returns the result
     * of {@link #peek()} unless the queue is empty.
     *
     * @return the head of this queue
     *
     * @throws NoSuchElementException if the queue is empty
     */
    public Object element()
    {
        if (isEmpty()) {
            throw new NoSuchElementException();
        }

        return peek();
    }

    /**
     * Retrieves and removes the head of the queue. Returns the result of {@link
     * #poll()} unless the queue is empty.
     *
     * @return the head of the queue
     *
     * @throws NoSuchElementException if the queue is empty
     */
    public E remove()
    {
        if (isEmpty()) {
            throw new NoSuchElementException();
        }

        return poll();
    }
}

// End ArrayQueue.java
