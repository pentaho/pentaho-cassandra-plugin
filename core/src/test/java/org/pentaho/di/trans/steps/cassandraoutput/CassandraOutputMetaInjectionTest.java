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


package org.pentaho.di.trans.steps.cassandraoutput;

import org.junit.Before;
import org.junit.Test;
import org.pentaho.di.core.injection.BaseMetadataInjectionTest;

public class CassandraOutputMetaInjectionTest extends BaseMetadataInjectionTest<CassandraOutputMeta> {

  @Before
  public void setup() {
    setup( new CassandraOutputMeta() );
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
    check( "SCHEMA_HOST", new StringGetter() {
      public String get() {
        return meta.getSchemaHost();
      }
    } );
    check( "SCHEMA_PORT", new StringGetter() {
      public String get() {
        return meta.getSchemaPort();
      }
    } );
    check( "TABLE", new StringGetter() {
      public String get() {
        return meta.getTableName();
      }
    } );
    check( "CONSISTENCY_LEVEL", new StringGetter() {
      public String get() {
        return meta.getConsistency();
      }
    } );
    check( "BATCH_SIZE", new StringGetter() {
      public String get() {
        return meta.getBatchSize();
      }
    } );
    check( "USE_UNLOGGED_BATCH", new BooleanGetter() {
      public boolean get() {
        return meta.getUseUnloggedBatch();
      }
    } );
    check( "USE_QUERY_COMPRESSION", new BooleanGetter() {
      public boolean get() {
        return meta.getUseCompression();
      }
    } );
    check( "CREATE_TABLE", new BooleanGetter() {
      public boolean get() {
        return meta.getCreateTable();
      }
    } );
    check( "CREATE_TABLE_WITH_CLAUSE", new StringGetter() {
      public String get() {
        return meta.getCreateTableWithClause();
      }
    } );
    check( "KEY_FIELD", new StringGetter() {
      public String get() {
        return meta.getKeyField();
      }
    } );
    check( "BATCH_TIMEOUT", new StringGetter() {
      public String get() {
        return meta.getCQLBatchInsertTimeout();
      }
    } );
    check( "SUB_BATCH_SIZE", new StringGetter() {
      public String get() {
        return meta.getCQLSubBatchSize();
      }
    } );
    check( "INSERT_FIELDS_NOT_IN_META", new BooleanGetter() {
      public boolean get() {
        return meta.getInsertFieldsNotInMeta();
      }
    } );
    check( "UPDATE_CASSANDRA_META", new BooleanGetter() {
      public boolean get() {
        return meta.getUpdateCassandraMeta();
      }
    } );
    check( "TRUNCATE_TABLE", new BooleanGetter() {
      public boolean get() {
        return meta.getTruncateTable();
      }
    } );
    check( "APRIORI_CQL", new StringGetter() {
      public String get() {
        return meta.getAprioriCQL();
      }
    } );
    check( "DONT_COMPLAIN_IF_APRIORI_CQL_FAILS", new BooleanGetter() {
      public boolean get() {
        return meta.getDontComplainAboutAprioriCQLFailing();
      }
    } );
    check( "SOCKET_TIMEOUT", new StringGetter() {
      public String get() {
        return meta.getSocketTimeout();
      }
    } );
    check( "TTL", new StringGetter() {
      public String get() {
        return meta.getTTL();
      }
    } );
    check( "TTL_UNIT", new StringGetter() {
      public String get() {
        return meta.getTTLUnit();
      }
    } );
  }

}
