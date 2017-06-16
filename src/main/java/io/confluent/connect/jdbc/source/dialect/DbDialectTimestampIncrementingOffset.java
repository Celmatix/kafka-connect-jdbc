/**
 * Copyright 2015 Confluent Inc.
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

import org.apache.kafka.connect.errors.ConnectException;

import java.sql.Timestamp;
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
public abstract class DbDialectTimestampIncrementingOffset {
  public abstract long getIncrementingOffset();

  public abstract Object getTimestampOffset();

  public abstract Map<String, Object> toMap();

  public static DbDialectTimestampIncrementingOffset fromConnectionStringAndMap(final String url, Map<String, ?> offsetMap, boolean rowVersion) {
    if (!url.startsWith("jdbc:")) {
      throw new ConnectException(String.format("Not a valid JDBC URL: %s", url));
    }

    final String protocol = extractProtocolFromUrl(url).toLowerCase();
    switch (protocol) {
      case "microsoft:sqlserver":
      case "sqlserver":
      case "jtds:sqlserver":
        if (rowVersion) {
          return SqlServerTimestampIncrementingOffset.fromMap(offsetMap);
        } else {
          return GenericTimestampIncrementingOffset.fromMap(offsetMap);
        }
      default:
        return GenericTimestampIncrementingOffset.fromMap(offsetMap);
    }
  }

  public static DbDialectTimestampIncrementingOffset fromConnectionString(final String url, Object timestampOffset, Long incrementingOffset, boolean rowVersion) {
    if (!url.startsWith("jdbc:")) {
      throw new ConnectException(String.format("Not a valid JDBC URL: %s", url));
    }

    final String protocol = extractProtocolFromUrl(url).toLowerCase();
    // todo technically we should check that the column type is timestamp or rowversion since other timestamp types like smalltimestamp
    // todo in mssql is still a real timestamp
    switch (protocol) {
      case "microsoft:sqlserver":
      case "sqlserver":
      case "jtds:sqlserver":
        return new SqlServerTimestampIncrementingOffset((byte[]) timestampOffset, incrementingOffset);
      default:
        return new GenericTimestampIncrementingOffset((Timestamp) timestampOffset, incrementingOffset);
    }
  }

  private static String extractProtocolFromUrl(final String url) {
    if (!url.startsWith("jdbc:")) {
      throw new ConnectException(String.format("Not a valid JDBC URL: %s", url));
    }
    int index = url.indexOf("://", "jdbc:".length());
    if (index < 0) {
      index = url.indexOf(":", "jdbc:".length());
      if (index < 0) {
        throw new ConnectException(String.format("Not a valid JDBC URL: %s", url));
      }
    }
    return url.substring("jdbc:".length(), index);
  }
}
