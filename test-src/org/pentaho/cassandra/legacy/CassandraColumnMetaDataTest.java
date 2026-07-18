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


package org.pentaho.cassandra.legacy;

import org.junit.Test;

import java.util.regex.Matcher;
import java.util.regex.Pattern;

import static org.junit.Assert.assertFalse;
import static org.mockito.Matchers.anyString;
import static org.mockito.Mockito.doCallRealMethod;
import static org.mockito.Mockito.mock;

/**
 * Created by Yury_Bakhmutski on 1/15/2018.
 */
public class CassandraColumnMetaDataTest {

  @Test
  public void testGenerateRefreshCQL3Query() throws Exception {
    CassandraColumnMetaData metaMock = mock( CassandraColumnMetaData.class );
    doCallRealMethod().when( metaMock ).generateRefreshCQL3Query( anyString() );

    String cql = metaMock.generateRefreshCQL3Query( "default" );

    //deprecated expressions
    assertFalse( cql.contains( CassandraColumnMetaData.CFMetaDataElements.POPULATE_IO_CACHE_ON_FLUSH.toString() ) );

    String readRepairChancePattern = ".*\\sread_repair_chance,.*";
    Pattern pattern = Pattern.compile( readRepairChancePattern );
    Matcher matcher = pattern.matcher( cql );
    assertFalse( matcher.matches() );

    assertFalse( cql.contains( CassandraColumnMetaData.CFMetaDataElements.REPLICATE_ON_WRITE.toString() ) );
  }

}
