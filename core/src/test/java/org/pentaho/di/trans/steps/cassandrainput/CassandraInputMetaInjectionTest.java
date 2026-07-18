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


package org.pentaho.di.trans.steps.cassandrainput;

import org.junit.Before;
import org.junit.Test;
import org.pentaho.di.core.injection.BaseMetadataInjectionTest;

public class CassandraInputMetaInjectionTest extends BaseMetadataInjectionTest<CassandraInputMeta> {

  @Before
  public void setup() {
    setup( new CassandraInputMeta() );
  }

  @Test
  public void test() throws Exception {
    check( "CASSANDRA_HOST", new StringGetter() {
      public String get() {
        return meta.getCassandraHost();
      }
    } );
    check( "CASSANDRA_PORT", new StringGetter() {
      public String get() {
        return meta.getCassandraPort();
      }
    } );
    check( "USER_NAME", new StringGetter() {
      public String get() {
        return meta.getUsername();
      }
    } );
    check( "PASSWORD", new StringGetter() {
      public String get() {
        return meta.getPassword();
      }
    } );
    check( "CASSANDRA_KEYSPACE", new StringGetter() {
      public String get() {
        return meta.getCassandraKeyspace();
      }
    } );
    check( "USE_QUERY_COMPRESSION", new BooleanGetter() {
      public boolean get() {
        return meta.getUseCompression();
      }
    } );
    check( "CQL_QUERY", new StringGetter() {
      public String get() {
        return meta.getCQLSelectQuery();
      }
    } );
    check( "EXECUTE_FOR_EACH_ROW", new BooleanGetter() {
      public boolean get() {
        return meta.getExecuteForEachIncomingRow();
      }
    } );
    check( "SOCKET_TIMEOUT", new StringGetter() {
      public String get() {
        return meta.getSocketTimeout();
      }
    } );
    check( "TRANSPORT_MAX_LENGTH", new StringGetter() {
      public String get() {
        return meta.getMaxLength();
      }
    } );
  }

}
