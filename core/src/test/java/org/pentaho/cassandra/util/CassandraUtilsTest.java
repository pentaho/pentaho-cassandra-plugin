/*!
 * Copyright 2014 - 2018 Hitachi Vantara.  All rights reserved.
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
 *
 */

package org.pentaho.cassandra.util;

import static org.junit.Assert.assertEquals;
import static org.junit.Assert.assertNull;
import static org.junit.Assert.assertTrue;
import static org.mockito.Matchers.any;
import static org.mockito.Mockito.mock;
import static org.mockito.Mockito.when;

import org.apache.cassandra.dht.ByteOrderedPartitioner;
import org.apache.cassandra.dht.Murmur3Partitioner;
import org.apache.cassandra.dht.OrderPreservingPartitioner;
import org.apache.cassandra.dht.RandomPartitioner;
import org.junit.Test;
import org.pentaho.di.core.row.ValueMetaInterface;
import org.pentaho.di.core.row.value.ValueMetaDate;
import org.pentaho.di.core.row.value.ValueMetaTimestamp;

import java.util.Date;

public class CassandraUtilsTest {

  @Test
  public void testRemoveQuotesCQL3() {
    String toTest = "\"AQuotedMixedCaseItentifier\"";

    String result = CassandraUtils.removeQuotes( toTest );

    assertEquals( result, "AQuotedMixedCaseItentifier" );
  }

  @Test
  public void testRemoveQuotesNoQuotesCQL3CaseInsensitive() {

    String toTest = "MixedCaseNoQuotes";
    String result = CassandraUtils.removeQuotes( toTest );

    // Without enclosing quotes Cassandra CQL3 is case insensitive
    assertEquals( result, "mixedcasenoquotes" );
  }

  @Test
  public void testAddQuotesCQL3MixedCase() {
    String toTest = "MixedCaseNoQuotes";

    String result = CassandraUtils.cql3MixedCaseQuote( toTest );

    assertEquals( result, "\"MixedCaseNoQuotes\"" );
  }

  @Test
  public void testAddQuotesCQL3LowerCase() {
    String toTest = "alreadylowercase_noquotesneeded";

    String result = CassandraUtils.cql3MixedCaseQuote( toTest );

    // all lower case does not require enclosing quotes
    assertEquals( result, toTest );
  }

  @Test
  public void testAddQuotesAlreadyQuoted() {
    String toTest = "\"AQuotedMixedCaseItentifier\"";

    String result = CassandraUtils.cql3MixedCaseQuote( toTest );

    // already quoted - should be no change
    assertEquals( result, toTest );
  }

  @Test
  public void testGetPartitionerClassInstance() {
    assertTrue( CassandraUtils.getPartitionerClassInstance( "org.apache.cassandra.dht.Murmur3Partitioner" ).getClass() == Murmur3Partitioner.class );
    assertTrue( CassandraUtils.getPartitionerClassInstance( "org.apache.cassandra.dht.ByteOrderedPartitioner" ).getClass() == ByteOrderedPartitioner.class );
    assertTrue( CassandraUtils.getPartitionerClassInstance( "org.apache.cassandra.dht.RandomPartitioner" ).getClass() == RandomPartitioner.class );
    assertTrue( CassandraUtils.getPartitionerClassInstance( "org.apache.cassandra.dht.OrderPreservingPartitioner" ).getClass() == OrderPreservingPartitioner.class );
  }

  @Test
  public void testGetPartitionKey() {
    String primaryKey = "test1";
    assertNull( CassandraUtils.getPartitionKey( null ) );
    assertNull( CassandraUtils.getPartitionKey( "" ) );
    assertEquals( "test1", CassandraUtils.getPartitionKey( primaryKey ) );

    primaryKey = "test1, test2";
    assertEquals( "test1", CassandraUtils.getPartitionKey( primaryKey ) );

    primaryKey = "(test1, test2), test3";
    assertEquals( "(test1, test2)", CassandraUtils.getPartitionKey( primaryKey ) );

    primaryKey = "(test1, (test2, test3), test4), test5";
    assertEquals( "(test1, (test2, test3), test4)", CassandraUtils.getPartitionKey( primaryKey ) );

    primaryKey = "((test1, test2), test3), test4";
    assertEquals( "((test1, test2), test3)", CassandraUtils.getPartitionKey( primaryKey ) );
  }

  @Test
  public void testKettleToCQLDateAndTimestamp() throws Exception {
    ValueMetaInterface vmDate = mock( ValueMetaDate.class );
    ValueMetaInterface vmTimestamp = mock( ValueMetaTimestamp.class );
    Date testTimestamp = new Date( 1520816523456L );

    when( vmDate.getType() ).thenReturn( ValueMetaInterface.TYPE_DATE );
    when( vmTimestamp.getType() ).thenReturn( ValueMetaInterface.TYPE_TIMESTAMP );
    when( vmDate.getDate( any() ) ).thenReturn( testTimestamp );
    when( vmTimestamp.getDate( any() ) ).thenReturn( testTimestamp );

    assertEquals( "'2018-03-12'", CassandraUtils.kettleValueToCQL( vmDate, testTimestamp, 3 ) );
    assertEquals( "'2018-03-12T01:02:03.456Z'", CassandraUtils.kettleValueToCQL( vmTimestamp, testTimestamp, 3 ) );
  }
}
