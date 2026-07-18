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


package org.pentaho.di.trans.steps.cassandrasstableoutput;

import org.junit.Before;
import org.junit.Test;
import org.pentaho.di.core.injection.BaseMetadataInjectionTest;

public class SSTableOutputMetaInjectionTest extends BaseMetadataInjectionTest<SSTableOutputMeta> {

  @Before
  public void setup() {
    setup( new SSTableOutputMeta() );
  }

  @Test
  public void test() throws Exception {
    check( "YAML_FILE_PATH", new StringGetter() {
      public String get() {
        return meta.getYamlPath();
      }
    } );
    check( "DIRECTORY", new StringGetter() {
      public String get() {
        return meta.getDirectory();
      }
    } );
    check( "CASSANDRA_KEYSPACE", new StringGetter() {
      public String get() {
        return meta.getCassandraKeyspace();
      }
    } );
    check( "TABLE", new StringGetter() {
      public String get() {
        return meta.getTableName();
      }
    } );
    check( "KEY_FIELD", new StringGetter() {
      public String get() {
        return meta.getKeyField();
      }
    } );
    check( "BUFFER_SIZE", new StringGetter() {
      public String get() {
        return meta.getBufferSize();
      }
    } );
  }

}
