/**
 * Copyright 2016 Confluent Inc.
 *
 * Licensed under the Apache License, Version 2.0 (the "License");
 * you may not use this file except in compliance with the License.
 * You may obtain a copy of the License at
 *
 * http://www.apache.org/licenses/LICENSE-2.0
 *
 * Unless required by applicable law or agreed to in writing, software
 * distributed under the License is distributed on an "AS IS" BASIS,
 * WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
 * See the License for the specific language governing permissions and
 * limitations under the License.
 **/

package io.confluent.connect.jdbc.source.dialect;

import org.junit.Before;
import org.junit.Test;

import java.sql.Timestamp;


import static org.junit.Assert.assertEquals;
import static org.junit.Assert.assertNotNull;

public class GenericTimestampIncrementingOffsetTest {
    private final Timestamp ts = new Timestamp(100L);
    private final long id = 1000L;
    private final GenericTimestampIncrementingOffset unset = new GenericTimestampIncrementingOffset(null, null);
    private final GenericTimestampIncrementingOffset tsOnly = new GenericTimestampIncrementingOffset(ts, null);
    private final GenericTimestampIncrementingOffset incOnly = new GenericTimestampIncrementingOffset(null, id);
    private final GenericTimestampIncrementingOffset tsInc = new GenericTimestampIncrementingOffset(ts, id);
    private Timestamp nanos;
    private GenericTimestampIncrementingOffset nanosOffset;

    @Before
    public void setUp() {
        long millis = System.currentTimeMillis();
        nanos = new Timestamp(millis);
        nanos.setNanos((int)(millis % 1000) * 1000000 + 123456);
        assertEquals(millis, nanos.getTime());
        nanosOffset = new GenericTimestampIncrementingOffset(nanos, null);
    }

    @Test
    public void testDefaults() {
        assertEquals(-1, unset.getIncrementingOffset());
        assertNotNull(unset.getTimestampOffset());
        assertEquals(0, unset.getTimestampOffset().getTime());
        assertEquals(0, unset.getTimestampOffset().getNanos());
    }

    @Test
    public void testToMap() {
        assertEquals(0, unset.toMap().size());
        assertEquals(2, tsOnly.toMap().size());
        assertEquals(1, incOnly.toMap().size());
        assertEquals(3, tsInc.toMap().size());
        assertEquals(2, nanosOffset.toMap().size());
    }

    @Test
    public void testGetIncrementingOffset() {
        assertEquals(-1, unset.getIncrementingOffset());
        assertEquals(-1, tsOnly.getIncrementingOffset());
        assertEquals(id, incOnly.getIncrementingOffset());
        assertEquals(id, tsInc.getIncrementingOffset());
        assertEquals(-1, nanosOffset.getIncrementingOffset());
    }

    @Test
    public void testGetTimestampOffset() {
        assertNotNull(unset.getTimestampOffset());
        Timestamp zero = new Timestamp(0);
        assertEquals(zero, unset.getTimestampOffset());
        assertEquals(ts, tsOnly.getTimestampOffset());
        assertEquals(zero, incOnly.getTimestampOffset());
        assertEquals(ts, tsInc.getTimestampOffset());
        assertEquals(nanos, nanosOffset.getTimestampOffset());
    }

    @Test
    public void testFromMap() {
        assertEquals(unset, GenericTimestampIncrementingOffset.fromMap(unset.toMap()));
        assertEquals(tsOnly, GenericTimestampIncrementingOffset.fromMap(tsOnly.toMap()));
        assertEquals(incOnly, GenericTimestampIncrementingOffset.fromMap(incOnly.toMap()));
        assertEquals(tsInc, GenericTimestampIncrementingOffset.fromMap(tsInc.toMap()));
        assertEquals(nanosOffset, GenericTimestampIncrementingOffset.fromMap(nanosOffset.toMap()));
    }

    @Test
    public void testEquals() {
        assertEquals(nanosOffset, nanosOffset);
        assertEquals(new GenericTimestampIncrementingOffset(null, null), new GenericTimestampIncrementingOffset(null, null));
        assertEquals(unset, new GenericTimestampIncrementingOffset(null, null));

        GenericTimestampIncrementingOffset x = new GenericTimestampIncrementingOffset(null, id);
        assertEquals(x, incOnly);

        x = new GenericTimestampIncrementingOffset(ts, null);
        assertEquals(x, tsOnly);

        x = new GenericTimestampIncrementingOffset(ts, id);
        assertEquals(x, tsInc);

        x = new GenericTimestampIncrementingOffset(nanos, null);
        assertEquals(x, nanosOffset);
    }

}
