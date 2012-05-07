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

import java.nio.*;


/**
 * A synchronization primitive which allows producer and a consumer to use the
 * same {@link java.nio.Buffer} without treading on each other's feet.
 *
 * <p>This class is <em>synchronous</em>: only one of the producer and consumer
 * has access at a time. There is only one buffer, and data is not copied.
 *
 * <p>The byte buffer is fixed in size. The producer writes up to the maximum
 * number of bytes into the buffer, then yields. The consumer must read all of
 * the data in the buffer before yielding back to the producer.
 *
 * @author jhyde
 * @version $Id$
 * @testcase
 */
public class ExclusivePipe
    extends Interlock
{
    //~ Instance fields --------------------------------------------------------

    private final ByteBuffer buf;

    //~ Constructors -----------------------------------------------------------

    public ExclusivePipe(ByteBuffer buf)
    {
        this.buf = buf;
    }

    //~ Methods ----------------------------------------------------------------

    /**
     * Returns the buffer.
     */
    public ByteBuffer getBuffer()
    {
        return buf;
    }

    public void beginWriting()
    {
        super.beginWriting();
        buf.clear(); // don't need to synchronize -- we hold the semaphore
    }

    public void beginReading()
    {
        super.beginReading();
        buf.flip(); // don't need to synchronize -- we hold the semaphore
    }
}

// End ExclusivePipe.java
