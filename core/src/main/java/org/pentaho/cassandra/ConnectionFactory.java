/*! ******************************************************************************
 *
 * Pentaho
 *
 * Copyright (C) 2002 - 2026 by Pentaho Canada Inc. : http://www.pentaho.com
 *
 * Use of this software is governed by the Business Source License included
 * in the LICENSE.TXT file.
 *
 * Change Date: 2030-06-15
 ******************************************************************************/


package org.pentaho.cassandra;

import org.pentaho.cassandra.driver.datastax.DriverConnection;
import org.pentaho.cassandra.spi.Connection;

public class ConnectionFactory {
  private static ConnectionFactory s_singleton = new ConnectionFactory();

  public static enum Driver {
    ASTYANAX, BINARY_CQL3_PROTOCOL;
  }

  private ConnectionFactory() {
  }

  public static ConnectionFactory getFactory() {
    return s_singleton;
  }

  public Connection getConnection( Driver d ) {
    switch ( d ) {
      case BINARY_CQL3_PROTOCOL:
        return new DriverConnection();
      default:
        return null;
    }
  }

}
