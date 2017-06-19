package io.confluent.connect.jdbc.source.dialect;

import org.junit.Before;
import org.junit.Test;


import static org.junit.Assert.assertEquals;
import static org.junit.Assert.assertNotNull;

public class SqlServerTimestampIncrementingOffsetTest {
    private final Long ts = 163L;
    private final long id = 1000L;
    private final SqlServerTimestampIncrementingOffset unset = new SqlServerTimestampIncrementingOffset(null, null);
    private final SqlServerTimestampIncrementingOffset tsOnly = new SqlServerTimestampIncrementingOffset(ts, null);
    private final SqlServerTimestampIncrementingOffset incOnly = new SqlServerTimestampIncrementingOffset(null, id);
    private final SqlServerTimestampIncrementingOffset tsInc = new SqlServerTimestampIncrementingOffset(ts, id);

    @Before
    public void setUp() {

    }

    @Test
    public void testDefaults() {
        assertEquals(-1, unset.getIncrementingOffset());
        assertNotNull(unset.getTimestampOffset());
        assertEquals(0L, (long) unset.getTimestampOffset());
    }

    @Test
    public void testToMap() {
        assertEquals(0, unset.toMap().size());
        assertEquals(2, tsOnly.toMap().size());
        assertEquals(1, incOnly.toMap().size());
        assertEquals(3, tsInc.toMap().size());
    }

    @Test
    public void testGetIncrementingOffset() {
        assertEquals(-1, unset.getIncrementingOffset());
        assertEquals(-1, tsOnly.getIncrementingOffset());
        assertEquals(id, incOnly.getIncrementingOffset());
        assertEquals(id, tsInc.getIncrementingOffset());
    }

    @Test
    public void testGetTimestampOffset() {
        assertNotNull(unset.getTimestampOffset());
        Long zero = 0L;
        assertEquals(zero, unset.getTimestampOffset());
        assertEquals(ts, tsOnly.getTimestampOffset());
        assertEquals(zero, incOnly.getTimestampOffset());
        assertEquals(ts, tsInc.getTimestampOffset());
    }

    @Test
    public void testFromMap() {
        // todo won't these always be different instances?
        assertEquals(unset, SqlServerTimestampIncrementingOffset.fromMap(unset.toMap()));
        assertEquals(tsOnly, SqlServerTimestampIncrementingOffset.fromMap(tsOnly.toMap()));
        assertEquals(incOnly, SqlServerTimestampIncrementingOffset.fromMap(incOnly.toMap()));
        assertEquals(tsInc, SqlServerTimestampIncrementingOffset.fromMap(tsInc.toMap()));
    }

    @Test
    public void testEquals() {
        assertEquals(new SqlServerTimestampIncrementingOffset(null, null), new SqlServerTimestampIncrementingOffset(null, null));
        assertEquals(unset, new SqlServerTimestampIncrementingOffset(null, null));

        SqlServerTimestampIncrementingOffset x = new SqlServerTimestampIncrementingOffset(null, id);
        assertEquals(x, incOnly);

        x = new SqlServerTimestampIncrementingOffset(ts, null);
        assertEquals(x, tsOnly);

        x = new SqlServerTimestampIncrementingOffset(ts, id);
        assertEquals(x, tsInc);
    }
}