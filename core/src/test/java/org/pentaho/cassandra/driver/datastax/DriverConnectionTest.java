/*! ******************************************************************************
 *
 * Pentaho
 *
 * Copyright (C) 2018 - 2026 by Pentaho Canada Inc. : http://www.pentaho.com
 *
 * Use of this software is governed by the Business Source License included
 * in the LICENSE.TXT file.
 *
 * Change Date: 2030-06-15
 ******************************************************************************/

package org.pentaho.cassandra.driver.datastax;

import java.net.InetSocketAddress;
import java.util.ArrayList;
import java.util.HashMap;
import java.util.Map;

import org.junit.Test;
import org.mockito.invocation.InvocationOnMock;
import org.mockito.stubbing.Answer;
import org.pentaho.cassandra.util.CassandraUtils;

import com.datastax.driver.core.AuthProvider;
import com.datastax.driver.core.Cluster;
import com.datastax.driver.core.ProtocolOptions;
import com.datastax.driver.core.Session;

import static org.junit.Assert.assertEquals;
import static org.junit.Assert.assertNotNull;
import static org.junit.Assert.assertNotSame;
import static org.junit.Assert.assertSame;
import static org.mockito.ArgumentMatchers.anyString;
import static org.mockito.Mockito.mock;
import static org.mockito.Mockito.times;
import static org.mockito.Mockito.verify;
import static org.mockito.Mockito.when;

public class DriverConnectionTest {

  @Test
  public void testBasicConnection() throws Exception {
    final String host = "some.host";
    final int port = 42;
    try ( DriverConnection connection = new DriverConnection( host, port ) ) {
      Cluster cluster = connection.getCluster();
      assertEquals( AuthProvider.NONE, cluster.getConfiguration().getProtocolOptions().getAuthProvider() );
      assertEquals( ProtocolOptions.Compression.NONE,
          cluster.getConfiguration().getProtocolOptions().getCompression() );
    }
  }

  @Test
  public void testAuthConnection() throws Exception {
    // host, port
    final String host = "some.host";
    final int port = 42;
    try ( DriverConnection connection = new DriverConnection( host, port ) ) {
      connection.setUsername( "user" );
      connection.setPassword( "user" );
      Cluster cluster = connection.getCluster();
      assertNotNull( cluster.getConfiguration().getProtocolOptions().getAuthProvider() );
      assertNotSame( AuthProvider.NONE, cluster.getConfiguration().getProtocolOptions().getAuthProvider() );
    }
  }

  @Test
  public void testOptsConnection() throws Exception {
    // host, port
    final String host = "some.host";
    final int port = 42;
    try ( DriverConnection connection = new DriverConnection( host, port ) ) {
      connection.setUseCompression( true );
      Map<String, String> opts = new HashMap<>( 1 );
      opts.put( CassandraUtils.ConnectionOptions.SOCKET_TIMEOUT, "1000" );
      connection.setAdditionalOptions( opts );

      Cluster cluster = connection.getCluster();
      assertEquals( ProtocolOptions.Compression.LZ4, cluster.getConfiguration().getProtocolOptions().getCompression() );
      assertEquals( 1000, cluster.getConfiguration().getSocketOptions().getConnectTimeoutMillis() );
    }
  }

  @Test
  public void testSessions() throws Exception {
    final String host = "some.host";
    final int port = 42;
    final Cluster mockCluster = mock( Cluster.class );
    ArrayList<Session> sessions = new ArrayList<>();
    when( mockCluster.connect( anyString() ) ).then( new Answer<Session>() {
      @Override
      public Session answer( InvocationOnMock invocation ) throws Throwable {
        Session mockSession = mock( Session.class );
        sessions.add( mockSession );
        return mockSession;
      }
    } );

    try ( DriverConnection connection = new DriverConnection( host, port ) {
      @Override
      public Cluster getCluster() {
        return mockCluster;
      };
    } ) {
      Session session1 = connection.getSession( "1" );
      Session session1b = connection.getSession( "1" );
      Session session2 = connection.getSession( "2" );

      assertSame( session1, session1b );
      assertNotSame( session1, session2 );
    }
    assertEquals( 2, sessions.size() );
    for ( Session session : sessions ) {
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
