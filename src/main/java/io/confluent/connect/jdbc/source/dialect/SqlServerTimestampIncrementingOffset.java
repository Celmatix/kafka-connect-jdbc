package io.confluent.connect.jdbc.source.dialect;

import java.math.BigInteger;
import java.util.Arrays;
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
public class SqlServerTimestampIncrementingOffset extends DbDialectTimestampIncrementingOffset {
    private static final String INCREMENTING_FIELD = "incrementing";
    private static final String TIMESTAMP_FIELD = "timestamp";
    private static final String TIMESTAMP_NANOS_FIELD = "timestamp_nanos";

    private final Long incrementingOffset;
    private final byte[] timestampOffset;

    /**
     * @param timestampOffset    the timestamp offset.
     *                           If null, {@link #getTimestampOffset()} will return {@code new Timestamp(0)}.
     * @param incrementingOffset the incrementing offset.
     *                           If null, {@link #getIncrementingOffset()} will return -1.
     */
    public SqlServerTimestampIncrementingOffset(byte[] timestampOffset, Long incrementingOffset) {
        this.timestampOffset = timestampOffset;
        this.incrementingOffset = incrementingOffset;
    }


    @Override
    public long getIncrementingOffset() {
        return incrementingOffset == null ? -1 : incrementingOffset;
    }

    @Override
    public Long getTimestampOffset() {
        return timestampOffset == null ? 0L : new BigInteger(timestampOffset).longValue();
    }

    @Override
    public Map<String, Object> toMap() {
        Map<String, Object> map = new HashMap<>(3);
        if (incrementingOffset != null) {
            map.put(INCREMENTING_FIELD, incrementingOffset);
        }
        if (timestampOffset != null) {
            map.put(TIMESTAMP_FIELD, new BigInteger(timestampOffset).longValue());
            map.put(TIMESTAMP_NANOS_FIELD, null);
        }
        return map;
    }

    public static DbDialectTimestampIncrementingOffset fromMap(Map<String, ?> map) {
        if (map == null || map.isEmpty()) {
            return new SqlServerTimestampIncrementingOffset(null, null);
        }

        Long incr = (Long) map.get(INCREMENTING_FIELD);

        byte[] tf = null;

        if (map.get(TIMESTAMP_FIELD) instanceof byte[]) {
            tf = (byte[]) map.get(TIMESTAMP_FIELD);
        } else if (map.get(TIMESTAMP_FIELD) instanceof Long) {
            tf = BigInteger.valueOf((Long) map.get(TIMESTAMP_FIELD)).toByteArray();
        } else if (map.get(TIMESTAMP_FIELD) == null) {
            tf = null;
        } else {
            // todo how do we proceed if it is not in one of the allowed types
        }

        return new SqlServerTimestampIncrementingOffset(tf, incr);
    }

    @Override
    public boolean equals(Object o) {
        if (this == o) {
            return true;
        }
        if (o == null || getClass() != o.getClass()) {
            return false;
        }

        SqlServerTimestampIncrementingOffset that = (SqlServerTimestampIncrementingOffset) o;

        if (incrementingOffset != null ? !incrementingOffset.equals(that.incrementingOffset) : that.incrementingOffset != null) {
            return false;
        }
        return timestampOffset != null ? Arrays.equals(timestampOffset, that.timestampOffset) : that.timestampOffset == null;

    }

    @Override
    public int hashCode() {
        int result = incrementingOffset != null ? incrementingOffset.hashCode() : 0;
        result = 31 * result + (timestampOffset != null ? timestampOffset.hashCode() : 0);
        return result;
    }
}

