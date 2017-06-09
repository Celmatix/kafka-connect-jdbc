package io.confluent.connect.jdbc.source.dialect;

import java.sql.Timestamp;
import java.util.HashMap;
import java.util.Map;

/**
 * Copyright 2016 Confluent Inc.
 * <p>
 * Licensed under the Apache License, Version 2.0 (the "License");
 * you may not use this file except in compliance with the License.
 * You may obtain a copy of the License at
 * <p>
 * http://www.apache.org/licenses/LICENSE-2.0
 * <p>
 * Unless required by applicable law or agreed to in writing, software
 * distributed under the License is distributed on an "AS IS" BASIS,
 * WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
 * See the License for the specific language governing permissions and
 * limitations under the License.
 **/
public class GenericTimestampIncrementingOffset extends DbDialectTimestampIncrementingOffset {
    private static final String INCREMENTING_FIELD = "incrementing";
    private static final String TIMESTAMP_FIELD = "timestamp";
    private static final String TIMESTAMP_NANOS_FIELD = "timestamp_nanos";

    private final Long incrementingOffset;
    private final Timestamp timestampOffset;

    /**
     * @param timestampOffset    the timestamp offset.
     *                           If null, {@link #getTimestampOffset()} will return {@code new Timestamp(0)}.
     * @param incrementingOffset the incrementing offset.
     *                           If null, {@link #getIncrementingOffset()} will return -1.
     */
    public GenericTimestampIncrementingOffset(Timestamp timestampOffset, Long incrementingOffset) {
        this.timestampOffset = timestampOffset;
        this.incrementingOffset = incrementingOffset;
    }

    @Override
    public long getIncrementingOffset() {
        return incrementingOffset == null ? -1 : incrementingOffset;
    }

    @Override
    public Timestamp getTimestampOffset() {
        return timestampOffset == null ? new Timestamp(0) : timestampOffset;
    }

    @Override
    public Map<String, Object> toMap() {
        Map<String, Object> map = new HashMap<>(3);
        if (incrementingOffset != null) {
            map.put(INCREMENTING_FIELD, incrementingOffset);
        }
        if (timestampOffset != null) {
            map.put(TIMESTAMP_FIELD, timestampOffset.getTime());
            map.put(TIMESTAMP_NANOS_FIELD, (long) timestampOffset.getNanos());
        }
        return map;
    }

    public static DbDialectTimestampIncrementingOffset fromMap(Map<String, ?> map) {
        if (map == null || map.isEmpty()) {
            return new GenericTimestampIncrementingOffset(null, null);
        }

        Long incr = (Long) map.get(INCREMENTING_FIELD);
        Long millis = (Long) map.get(TIMESTAMP_FIELD);
        Timestamp ts = null;
        if (millis != null) {
            ts = new Timestamp(millis);
            Long nanos = (Long) map.get(TIMESTAMP_NANOS_FIELD);
            if (nanos != null) {
                ts.setNanos(nanos.intValue());
            }
        }
        return new GenericTimestampIncrementingOffset(ts, incr);
    }

    @Override
    public boolean equals(Object o) {
        if (this == o) {
            return true;
        }
        if (o == null || getClass() != o.getClass()) {
            return false;
        }

        GenericTimestampIncrementingOffset that = (GenericTimestampIncrementingOffset) o;

        if (incrementingOffset != null ? !incrementingOffset.equals(that.incrementingOffset) : that.incrementingOffset != null) {
            return false;
        }
        return timestampOffset != null ? timestampOffset.equals(that.timestampOffset) : that.timestampOffset == null;

    }

    @Override
    public int hashCode() {
        int result = incrementingOffset != null ? incrementingOffset.hashCode() : 0;
        result = 31 * result + (timestampOffset != null ? timestampOffset.hashCode() : 0);
        return result;
    }
}

