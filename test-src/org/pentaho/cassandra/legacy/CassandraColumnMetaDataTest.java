/*******************************************************************************
 *
 * Pentaho Big Data
 *
 * Copyright (C) 2002-2018 by Hitachi Vantara : http://www.pentaho.com
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
