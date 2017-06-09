package io.confluent.connect.jdbc.source.dialect;

import io.confluent.connect.jdbc.util.DateTimeUtils;
import io.confluent.connect.jdbc.util.JdbcUtils;
import org.apache.kafka.connect.data.Struct;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.math.BigInteger;
import java.sql.PreparedStatement;
import java.sql.SQLException;
import java.sql.Timestamp;

public class TimestampStatementAdapter {
    private static final Logger log = LoggerFactory.getLogger(TimestampStatementAdapter.class);

    public static  PreparedStatement getTimeStampAndIncrementingOffsetPreparedStatement(DbDialectTimestampIncrementingOffset offset, long timestampDelay, PreparedStatement stmt) throws SQLException {
        if (offset instanceof SqlServerTimestampIncrementingOffset) {
            Long incOffset = offset.getIncrementingOffset();
            Long tsOffset = (Long) offset.getTimestampOffset();
            Long endTime = new BigInteger(JdbcUtils.getRowVersionFromMsSql(stmt.getConnection())).longValue();
            stmt.setLong(1, endTime);
            stmt.setLong(2, tsOffset);
            stmt.setLong(3, incOffset);
            stmt.setLong(4, tsOffset);
            log.debug("Executing prepared statement with start time value = {} end time = {} and incrementing value = {}",
                    tsOffset,
                    endTime,
                    incOffset);
        } else {
            Timestamp tsOffset = (Timestamp) offset.getTimestampOffset();
            Long incOffset = offset.getIncrementingOffset();
            Timestamp endTime = new Timestamp(JdbcUtils.getCurrentTimeOnDB(stmt.getConnection(), DateTimeUtils.UTC_CALENDAR.get()).getTime() - timestampDelay);
            stmt.setTimestamp(1, endTime, DateTimeUtils.UTC_CALENDAR.get());
            stmt.setTimestamp(2, tsOffset, DateTimeUtils.UTC_CALENDAR.get());
            stmt.setLong(3, incOffset);
            stmt.setTimestamp(4, tsOffset, DateTimeUtils.UTC_CALENDAR.get());
            log.debug("Executing prepared statement with start time value = {} end time = {} and incrementing value = {}",
                    DateTimeUtils.formatUtcTimestamp(tsOffset),
                    DateTimeUtils.formatUtcTimestamp(endTime),
                    incOffset);
        }

        return stmt;
    }

    public static PreparedStatement getTimeStampPreparedStatement(DbDialectTimestampIncrementingOffset offset, long timestampDelay, PreparedStatement stmt) throws SQLException {
        if (offset instanceof SqlServerTimestampIncrementingOffset) {
            Long tsOffset = (Long) offset.getTimestampOffset();
            Long endTime = new BigInteger(JdbcUtils.getRowVersionFromMsSql(stmt.getConnection())).longValue();
            stmt.setLong(1, tsOffset);
            stmt.setLong(2, endTime);
            log.debug("Executing prepared statement with timestamp value = {} end time = {}",
                    tsOffset,
                    endTime);
        } else {
            Timestamp tsOffset = (Timestamp) offset.getTimestampOffset();
            Timestamp endTime = new Timestamp(JdbcUtils.getCurrentTimeOnDB(stmt.getConnection(), DateTimeUtils.UTC_CALENDAR.get()).getTime() - timestampDelay);
            stmt.setTimestamp(1, tsOffset, DateTimeUtils.UTC_CALENDAR.get());
            stmt.setTimestamp(2, endTime, DateTimeUtils.UTC_CALENDAR.get());
            log.debug("Executing prepared statement with timestamp value = {} end time = {}",
                    DateTimeUtils.formatUtcTimestamp(tsOffset),
                    DateTimeUtils.formatUtcTimestamp(endTime));
        }

        return stmt;
    }

    public static Object extractTimeStampRecordAssertion(DbDialectTimestampIncrementingOffset offset, Struct record, String timestampColumn) {
        if (offset instanceof SqlServerTimestampIncrementingOffset) {
            long extractedTimestamp = new BigInteger((byte[]) record.get(timestampColumn)).longValue();
            Long timestampOffset = (Long) offset.getTimestampOffset();

            assert timestampOffset != null && timestampOffset.compareTo(extractedTimestamp) <= 0;

            return extractedTimestamp;
        } else {
            Timestamp extractedTimestamp = (Timestamp) record.get(timestampColumn);
            Timestamp timestampOffset = (Timestamp) offset.getTimestampOffset();

            assert timestampOffset != null && timestampOffset.compareTo(extractedTimestamp) <= 0;

            return extractedTimestamp;
        }
    }
}
