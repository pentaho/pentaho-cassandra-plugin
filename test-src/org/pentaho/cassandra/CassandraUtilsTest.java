/*!
 * Copyright 2014 Pentaho Corporation.  All rights reserved.
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

package org.pentaho.cassandra;

import static org.junit.Assert.assertEquals;

import org.junit.Test;

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
}
