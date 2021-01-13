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
package org.apache.calcite.util;

import org.checkerframework.checker.nullness.qual.Nullable;

import java.util.AbstractSequentialList;
import java.util.Arrays;
import java.util.Collection;
import java.util.LinkedList;
import java.util.ListIterator;
import java.util.NoSuchElementException;

import static org.apache.calcite.linq4j.Nullness.castNonNull;

import static java.util.Objects.requireNonNull;

/**
 * Implementation of list similar to {@link LinkedList}, but stores elements
 * in chunks of 32 elements.
 *
 * <p>ArrayList has O(n) insertion and deletion into the middle of the list.
 * ChunkList insertion and deletion are O(1).</p>
 *
 * @param <E> element type
 */
public class ChunkList<E> extends AbstractSequentialList<E> {
  private static final int HEADER_SIZE = 3;
  private static final int CHUNK_SIZE = 64;
  private static final Integer[] INTEGERS = new Integer[CHUNK_SIZE + 3];

  static {
    for (int i = 0; i < INTEGERS.length; i++) {
      INTEGERS[i] = i;
    }
  }

  private int size;
  private E @Nullable [] first;
  private E @Nullable [] last;

  /**
   * Creates an empty ChunkList.
   */
  public ChunkList() {
  }

  /**
   * Creates a ChunkList whose contents are a given Collection.
   */
  public ChunkList(Collection<E> collection) {
    @SuppressWarnings({"method.invocation.invalid", "unused"})
    boolean ignore = addAll(collection);
  }

  /**
   * For debugging and testing.
   */
  boolean isValid(boolean fail) {
    if ((first == null) != (last == null)) {
      assert !fail;
      return false;
    }
    if ((first == null) != (size == 0)) {
      assert !fail;
      return false;
    }
    int n = 0;
    for (@SuppressWarnings("unused") E e : this) {
      if (n++ > size) {
        assert !fail;
        return false;
      }
    }
    if (n != size) {
      assert !fail;
      return false;
    }
    E[] prev = null;
    for (E[] chunk = first; chunk != null; chunk = next(chunk)) {
      if (prev(chunk) != prev) {
        assert !fail;
        return false;
      }
      prev = chunk;
      if (occupied(chunk) == 0) {
        assert !fail;
        return false;
      }
    }
    return true;
  }

  @Override public ListIterator<E> listIterator(int index) {
    return locate(index);
  }

  @Override public int size() {
    return size;
  }

  @Override public void clear() {
    // base class method works, but let's optimize
    size = 0;
    first = last = null;
  }

  @Override public boolean add(E element) {
    E[] chunk = last;
    int occupied;
    if (chunk == null) {
      //noinspection unchecked
      chunk = first = last = (E[]) new Object[CHUNK_SIZE + HEADER_SIZE];
      occupied = 0;
    } else {
      occupied = occupied(chunk);
      if (occupied == CHUNK_SIZE) {
        //noinspection unchecked
        chunk = (E[]) new Object[CHUNK_SIZE + HEADER_SIZE];
        setNext(requireNonNull(last, "last"), chunk);
        setPrev(chunk, last);
        occupied = 0;
        last = chunk;
      }
    }
    setOccupied(chunk, occupied + 1);
    setElement(chunk, HEADER_SIZE + occupied, element);
    ++size;
    return true;
  }

  @Override public void add(int index, E element) {
    if (index == size) {
      add(element);
    } else {
      super.add(index, element);
    }
  }

  private static <E> E @Nullable [] prev(E[] chunk) {
    //noinspection unchecked
    return (E @Nullable []) chunk[0];
  }

  private static <E> void setPrev(E[] chunk, E @Nullable [] prev) {
    //noinspection unchecked
    chunk[0] = (E) prev;
  }

  private static <E> E @Nullable [] next(E[] chunk) {
    //noinspection unchecked
    return (E @Nullable []) chunk[1];
  }

  private static <E> void setNext(E[] chunk, E @Nullable [] next) {
    assert chunk != next;
    //noinspection unchecked
    chunk[1] = (E) next;
  }

  private static <E> int occupied(E[] chunk) {
    return (Integer) requireNonNull(chunk[2], "chunk[2] (number of occupied entries)");
  }

  @SuppressWarnings("unchecked")
  private static <E> void setOccupied(E[] chunk, int size) {
    chunk[2] = (E) INTEGERS[size];
  }

  private static <E> E element(E[] chunk, int index) {
    return chunk[index];
  }

  private static <E> void setElement(E[] chunk, int index, @Nullable E element) {
    chunk[index] = castNonNull(element);
  }

  private ChunkListIterator locate(int index) {
    if (index < 0 || index > size) {
      throw new IndexOutOfBoundsException();
    }
    if (first == null) {
      // Create an iterator positioned before the first element.
      return new ChunkListIterator(null, 0, 0, -1, 0);
    }
    int n = 0;
    for (E[] chunk = first;;) {
      final int occupied = occupied(chunk);
      final int nextN = n + occupied;
      final E[] next = next(chunk);
      if (nextN >= index || next == null) {
        return new ChunkListIterator(chunk, n, index, -1, n + occupied);
      }
      n = nextN;
      chunk = next;
    }
  }

  /** Iterator over a {@link ChunkList}. */
  private class ChunkListIterator implements ListIterator<E> {
    private E @Nullable [] chunk;
    /** Offset in the list of the first element of this chunk. */
    private int start;
    /** Offset within current chunk of the next element to return. */
    private int cursor;
    /** Offset within the current chunk of the last element returned. -1 if
     * {@link #next} or {@link #previous()} has not been called. */
    private int lastRet;
    /** Offset of the first unoccupied location in the current chunk. */
    private int end;

    ChunkListIterator(E @Nullable [] chunk, int start, int cursor, int lastRet,
        int end) {
      this.chunk = chunk;
      this.start = start;
      this.cursor = cursor;
      this.lastRet = lastRet;
      this.end = end;
    }

    private E[] currentChunk() {
      return castNonNull(chunk);
    }

    @Override public boolean hasNext() {
      return cursor < size;
    }

    @Override public E next() {
      if (cursor >= size) {
        throw new NoSuchElementException();
      }
      if (cursor == end) {
        if (chunk == null) {
          chunk = first;
        } else {
          chunk = ChunkList.next(chunk);
        }
        start = end;
        if (chunk == null) {
          end = start;
        } else {
          end = start + occupied(chunk);
        }
      }
      @SuppressWarnings("unchecked")
      final E element = (E) element(currentChunk(),
          HEADER_SIZE + (lastRet = cursor++) - start);
      return element;
    }

    @Override public boolean hasPrevious() {
      return cursor > 0;
    }

    @Override public E previous() {
      lastRet = cursor--;
      if (cursor < start) {
        chunk = chunk == null ? last : ChunkList.prev(chunk);
        if (chunk == null) {
          throw new NoSuchElementException();
        }
        final int o = occupied(chunk);
        end = start;
        start -= o;
        assert cursor == end - 1;
      }
      //noinspection unchecked
      return (E) element(currentChunk(), cursor - start);
    }

    @Override public int nextIndex() {
      return cursor;
    }

    @Override public int previousIndex() {
      return cursor - 1;
    }

    @Override public void remove() {
      if (lastRet < 0) {
        throw new IllegalStateException();
      }
      --size;
      --cursor;
      if (end == start + 1) {
        // Chunk is now empty.
        final E[] prev = prev(currentChunk());
        final E[] next = ChunkList.next(currentChunk());
        if (next == null) {
          last = prev;
          if (prev == null) {
            first = null;
          } else {
            setNext(prev, null);
          }
          chunk = null;
          end = HEADER_SIZE;
        } else {
          if (prev == null) {
            chunk = first = next;
            setPrev(next, null);
            end = occupied(requireNonNull(chunk, "chunk"));
          } else {
            setNext(prev, next);
            setPrev(next, prev);
            chunk = prev;
            end = start;
            start -= occupied(requireNonNull(chunk, "chunk"));
          }
        }
        lastRet = -1;
        return;
      }
      final int r = lastRet;
      lastRet = -1;
      if (r < start) {
        // Element we wish to eliminate is the last element in the previous
        // block.
        E[] c = chunk;
        if (c == null) {
          c = last;
        }
        int o = occupied(castNonNull(c));
        if (o == 1) {
          // Block is now empty; remove it
          final E[] prev = prev(c);
          if (prev == null) {
            if (chunk == null) {
              first = last = null;
            } else {
              first = chunk;
              setPrev(requireNonNull(chunk, "chunk"), null);
            }
          } else {
            setNext(requireNonNull(prev, "prev"), chunk);
            setPrev(requireNonNull(chunk, "chunk"), prev);
          }
        } else {
          --o;
          setElement(c, HEADER_SIZE + o, null); // allow gc
          setOccupied(c, o);
        }
      } else {
        // Move existing contents down one.
        System.arraycopy(currentChunk(), HEADER_SIZE + r - start + 1,
            currentChunk(), HEADER_SIZE + r - start, end - r - 1);
        --end;
        final int o = end - start;
        setElement(currentChunk(), HEADER_SIZE + o, null); // allow gc
        setOccupied(currentChunk(), o);
      }
    }

    @Override public void set(E e) {
      if (lastRet < 0) {
        throw new IllegalStateException();
      }
      E[] c = currentChunk();
      int p = lastRet;
      int s = start;
      if (p < start) {
        // The element is at the end of the previous chunk
        c = prev(c);
        s -= occupied(castNonNull(c));
      }
      setElement(c, HEADER_SIZE + p - s, e);
    }

    @Override public void add(E e) {
      if (chunk == null) {
        //noinspection unchecked
        E[] newChunk = (E[]) new Object[CHUNK_SIZE + HEADER_SIZE];
        if (first != null) {
          setNext(newChunk, first);
          setPrev(requireNonNull(first, "first"), newChunk);
        }
        first = newChunk;
        if (last == null) {
          last = newChunk;
        }
        chunk = newChunk;
        end = start;
      } else if (end == start + CHUNK_SIZE) {
        // FIXME We create a new chunk, but the next chunk might be
        // less than half full. We should consider using it.
        //noinspection unchecked
        E[] newChunk = (E[]) new Object[CHUNK_SIZE + HEADER_SIZE];
        final E[] next = ChunkList.next(chunk);
        setPrev(newChunk, chunk);
        setNext(requireNonNull(chunk, "chunk"), newChunk);

        if (next == null) {
          last = newChunk;
        } else {
          setPrev(next, newChunk);
          setNext(newChunk, next);
        }

        setOccupied(requireNonNull(chunk, "chunk"), CHUNK_SIZE / 2);
        setOccupied(newChunk, CHUNK_SIZE / 2);
        System.arraycopy(requireNonNull(chunk, "chunk"), HEADER_SIZE + CHUNK_SIZE / 2,
            newChunk, HEADER_SIZE, CHUNK_SIZE / 2);
        Arrays.fill(chunk, HEADER_SIZE + CHUNK_SIZE / 2,
            HEADER_SIZE + CHUNK_SIZE, null);

        if (cursor - start < CHUNK_SIZE / 2) {
          end -= CHUNK_SIZE / 2;
        } else {
          start += CHUNK_SIZE / 2;
          chunk = newChunk;
        }
      }
      // Move existing contents up one.
      System.arraycopy(chunk, HEADER_SIZE + cursor - start,
          chunk, HEADER_SIZE + cursor - start + 1, end - cursor);
      ++end;
      setElement(chunk, HEADER_SIZE + cursor - start, e);
      setOccupied(chunk, end - start);
      ++size;
    }
  }
}
