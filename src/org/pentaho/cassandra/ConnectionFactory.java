package org.pentaho.cassandra;

import org.pentaho.cassandra.legacy.CassandraConnection;
import org.pentaho.cassandra.spi.Connection;

public class ConnectionFactory {
  private static ConnectionFactory s_singleton = new ConnectionFactory();

  public static enum Driver {
    LEGACY_THRIFT, ASTYANAX, BINARY_CQL3_PROTOCOL;
  }

  private ConnectionFactory() {
  }

  public static ConnectionFactory getFactory() {
    return s_singleton;
  }

  public Connection getConnection(Driver d) {
    switch (d) {
    case LEGACY_THRIFT:
      return new CassandraConnection();
    }

    return null;
  }
}
