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
package org.eigenbase.test;

import java.nio.*;

import junit.framework.*;

import org.eigenbase.runtime.*;


/**
 * Testcase for {@link org.eigenbase.runtime.ExclusivePipe}.
 */
public class ExclusivePipeTest
    extends TestCase
{
    //~ Static fields/initializers ---------------------------------------------

    private static final int BUF_BYTES = 10;
    private static final int timeoutMillis = Integer.MAX_VALUE;

    private static final String [] words =
    {
        "the", "quick", "brown", "fox", "jumped", "over", "the", "lazy", "dog",
    };

    //~ Methods ----------------------------------------------------------------

    public void test()
    {
        ByteBuffer buf = ByteBuffer.allocateDirect(BUF_BYTES);
        ExclusivePipe pipe = new ExclusivePipe(buf);
        Producer producer = new Producer(pipe);
        Consumer consumer = new Consumer(pipe);
        producer.start();
        consumer.start();
        try {
            producer.join(timeoutMillis);
        } catch (InterruptedException e) {
            fail("producer interrupted");
        }
        try {
            consumer.join(timeoutMillis);
        } catch (InterruptedException e) {
            fail("consumer interrupted");
        }
        if (producer.thrown != null) {
            fail("producer had error: " + producer.thrown);
        }
        assertTrue("producer blocked", producer.succeeded);
        if (consumer.thrown != null) {
            fail("producer had error: " + consumer.thrown);
        }
        assertTrue("consumer blocked", consumer.succeeded);
    }

    //~ Inner Classes ----------------------------------------------------------

    /**
     * Producer thread writes a list of words into a pipe.
     */
    private static class Producer
        extends Thread
    {
        private final ExclusivePipe pipe;
        private boolean succeeded;
        private Throwable thrown;

        Producer(ExclusivePipe pipe)
        {
            this.pipe = pipe;
        }

        public void run()
        {
            try {
                ByteBuffer buf = pipe.getBuffer();
                for (int i = 0; i < words.length; i++) {
                    String word = words[i];
                    byte [] bytes = word.getBytes();
                    pipe.beginWriting();

                    // Store the string as a 1-byte length followed by n bytes.
                    // Can't handle strings longer than 255 but hey, this is
                    // only a test!
                    buf.put((byte) bytes.length);
                    buf.put(bytes, 0, bytes.length);
                    pipe.endWriting();
                }
                succeeded = true;
            } catch (Exception e) {
                thrown = e;
                e.printStackTrace();
            }
        }
    }

    /**
     * Consumer thread reads words from a pipe, comparing with the list of
     * expected words, until it has read all of the words it expects to see.
     */
    private static class Consumer
        extends Thread
    {
        private final ExclusivePipe pipe;
        private final byte [] bytes = new byte[BUF_BYTES];
        private boolean succeeded;
        private Throwable thrown;

        Consumer(ExclusivePipe pipe)
        {
            this.pipe = pipe;
        }

        public void run()
        {
            try {
                ByteBuffer buf = pipe.getBuffer();
                for (int i = 0; i < words.length; i++) {
                    String word = words[i];
                    pipe.beginReading();
                    int length = buf.get();
                    buf.get(bytes, 0, length);
                    String actualWord = new String(bytes, 0, length);
                    assertEquals(word, actualWord);
                    pipe.endReading();
                }
                succeeded = true;
            } catch (Exception e) {
                thrown = e;
                e.printStackTrace();
            }
        }
    }
}

// End ExclusivePipeTest.java
