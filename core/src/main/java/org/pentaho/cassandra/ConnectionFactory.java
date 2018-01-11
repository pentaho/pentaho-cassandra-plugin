/*******************************************************************************
*
* Pentaho Big Data
*
* Copyright (C) 2002-2017 by Hitachi Vantara : http://www.pentaho.com
*
*******************************************************************************
*
* Licensed under the Apache License, Version 2.0 (the "License");
* you may not use this file except in compliance with
* the License. You may obtain a copy of the License at
*
*    http://www.apache.org/licenses/LICENSE-2.0
*
* Unless required by applicable law or agreed to in writing, software
* distributed under the License is distributed on an "AS IS" BASIS,
* WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
* See the License for the specific language governing permissions and
* limitations under the License.
*
******************************************************************************/

package org.pentaho.cassandra;

import org.pentaho.cassandra.driverimpl.DriverConnection;
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

  public Connection getConnection( Driver d ) {
    switch ( d ) {
      case LEGACY_THRIFT:
        return new CassandraConnection();
      case BINARY_CQL3_PROTOCOL:
        return new DriverConnection();
      default:
        return null;
    }
  }

}
