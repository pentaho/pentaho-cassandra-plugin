/*! ******************************************************************************
 *
 * Pentaho Data Integration
 *
 * Copyright (C) 2002-2020 by Hitachi Vantara : http://www.pentaho.com
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
    check( "SSL", new BooleanGetter() {
      @Override public boolean get() {
        return meta.isSsl();
      }
    } );
    check( "ROW_FETCH_SIZE", new StringGetter() {
      @Override
      public String get() {
        return meta.getRowFetchSize();
      }
    } );
    check( "READ_TIMEOUT", new StringGetter() {
      @Override
      public String get() {
        return meta.getReadTimeout();
      }
    } );
    check( "LOCAL_DATACENTER", new StringGetter() {
      @Override
      public String get() {
        return meta.getLocalDataCenter();
      }
    } );
    check( "APPLICATION_CONF", new StringGetter() {
      @Override
      public String get() {
        return meta.getApplicationConfFile();
      }
    } );
    check( "EXPAND_COLLECTION", new BooleanGetter() {
      @Override
      public boolean get() {
        return meta.isExpandComplex();
      }
    } );
    check( "NOT_EXPANDING_MAPS", new BooleanGetter() {
      @Override
      public boolean get() {
        return meta.isNotExpandingMaps();
      }
    } );
  }

}
