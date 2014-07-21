/*! ******************************************************************************
 *
 * Pentaho Data Integration
 *
 * Copyright (C) 2002-2014 by Pentaho : http://www.pentaho.com
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

import java.util.HashMap;
import java.util.Map;

import static org.junit.Assert.assertEquals;

/**
 * User: Dzmitry Stsiapanau Date: 7/7/14 Time: 5:09 PM
 */
public class CassandraConnectionTest {

  @Test
  public void testSetAdditionalOptions() throws Exception {
    CassandraConnection cassandraConnection  = new CassandraConnection(  );
    Map<String, String> opts = new HashMap<String, String>(  );
    opts.put( "maxLength", "1000" );
    cassandraConnection.setAdditionalOptions( opts );
    assertEquals( 1000, cassandraConnection.m_maxlength );
  }
}
