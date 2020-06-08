/*! ******************************************************************************
 *
 * Pentaho Data Integration
 *
 * Copyright (C) 2018-2020 by Hitachi Vantara : http://www.pentaho.com
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
package org.pentaho.cassandra.driver.datastax;

import static org.junit.Assert.assertEquals;
import static org.junit.Assert.assertNotSame;
import static org.junit.Assert.assertSame;
import static org.mockito.Mockito.mock;
import static org.mockito.Mockito.times;
import static org.mockito.Mockito.verify;

import java.net.InetSocketAddress;
import java.util.ArrayList;

import org.junit.Test;

import com.datastax.oss.driver.api.core.CqlSession;
import com.datastax.oss.driver.api.core.session.Session;

public class DriverConnectionTest {

  @Test
  public void testSessions() throws Exception {
    final String host = "some.host";
    final int port = 42;
    final Session mockCluster = mock( Session.class );

    ArrayList<Session> sessionList = new ArrayList<>();

    try ( DriverConnection connection = new DriverConnection( host, port ) {
      @Override
      protected CqlSession getCluster( String keyspace ) {
        CqlSession mockSession = mock( CqlSession.class );
        sessionList.add( mockSession );
        return mockSession;
      }
    } ) {
      Session session1 = connection.getSession( "1" );
      Session session1b = connection.getSession( "1" );
      Session session2 = connection.getSession( "2" );

      assertSame( session1, session1b );
      assertNotSame( session1, session2 );
    }
    assertEquals( 2, sessionList.size() );
    for ( Session session : sessionList ) {
      verify( session, times( 1 ) ).close();
    }
  }

  @Test
  public void testMultipleHosts() throws Exception {
    try ( DriverConnection connection = new DriverConnection() ) {
      connection.setHosts( "localhost:1234,localhost:2345" );
      InetSocketAddress[] addresses = connection.getAddresses();
      assertEquals( "localhost", addresses[0].getHostName() );
      assertEquals( 1234, addresses[0].getPort() );
      assertEquals( "localhost", addresses[1].getHostName() );
      assertEquals( 2345, addresses[1].getPort() );
    }
  }
}