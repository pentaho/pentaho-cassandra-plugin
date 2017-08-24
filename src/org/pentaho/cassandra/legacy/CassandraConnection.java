/*******************************************************************************
 *
 * Pentaho Big Data
 *
 * Copyright (C) 2002-2017 by Pentaho : http://www.pentaho.com
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

import java.util.HashMap;
import java.util.Map;

import org.apache.cassandra.thrift.AuthenticationRequest;
import org.apache.cassandra.thrift.Cassandra;
import org.apache.cassandra.thrift.KsDef;
import org.apache.thrift.protocol.TBinaryProtocol;
import org.apache.thrift.protocol.TProtocol;
import org.apache.thrift.transport.TFramedTransport;
import org.apache.thrift.transport.TSocket;
import org.apache.thrift.transport.TTransport;
import org.pentaho.cassandra.CassandraUtils;
import org.pentaho.cassandra.spi.Connection;
import org.pentaho.cassandra.spi.Keyspace;
import org.pentaho.di.core.util.Utils;
import org.pentaho.di.i18n.BaseMessages;

/**
 * Class for establishing a connection with Cassandra. Encapsulates the transport and Cassandra client object.
 *
 * @author Mark Hall (mhall{[at]}pentaho{[dot]}com)
 * @version $Revision; $
 */
public class CassandraConnection implements Connection {

  protected static final Class<?> PKG = CassandraConnection.class;

  private TTransport m_transport;

  public Cassandra.Client m_client;
  public String m_keyspaceName;

  protected String m_host = "localhost"; //$NON-NLS-1$
  protected int m_port = 9160;
  protected String m_username = ""; //$NON-NLS-1$
  protected String m_password = ""; //$NON-NLS-1$
  protected int m_timeout = -1;
  protected int m_maxlength = -1;

  protected Map<String, String> m_options;

  public CassandraConnection() {

  }

  /**
   * Construct an CassandaraConnection with no authentication.
   *
   * @param host the host to connect to
   * @param port the port to use
   * @throws Exception if the connection fails
   */
  public CassandraConnection( String host, int port ) throws Exception {
    this( host, port, null, null, -1 );
  }

  /**
   * Construct a CassandraConnection with no authentication and the supplied socket timeout (milliseconds).
   *
   * @param host    the host to connect to
   * @param port    the port to use
   * @param timeout the socket timeout to use in milliseconds
   * @throws Exception if the connection fails
   */
  public CassandraConnection( String host, int port, int timeout )
    throws Exception {
    this( host, port, null, null, timeout );
  }

  /**
   * Construct an CassandaraConnection with optional authentication.
   *
   * @param host     the host to connect to
   * @param port     the port to use
   * @param username the username to authenticate with (may be null for no authentication)
   * @param password the password to authenticate with (may be null for no authentication)
   * @throws Exception if the connection fails
   */
  public CassandraConnection( String host, int port, String username,
                              String password, int timeout ) throws Exception {
    this( host, port, username, password, timeout, -1 );
  }

  /**
   * Construct an CassandaraConnection with optional authentication.
   *
   * @param host     the host to connect to
   * @param port     the port to use
   * @param username the username to authenticate with (may be null for no authentication)
   * @param password the password to authenticate with (may be null for no authentication)
   * @throws Exception if the connection fails
   */
  public CassandraConnection( String host, int port, String username,
                              String password, int timeout, int maxlength ) throws Exception {
    m_host = host;
    m_port = port;
    m_username = username;
    m_password = password;
    m_timeout = timeout;
    m_maxlength = maxlength;
    openConnection();
  }

  /**
   * Get the encapsulated Cassandra.Client object
   *
   * @return the encapsulated Cassandra.Client object
   */
  public Cassandra.Client getClient() {
    return m_client;
  }

  /**
   * Get a keyspace definition for the set keyspace
   *
   * @return a keyspace definition
   * @throws Exception if a problem occurs
   */
  public KsDef describeKeyspace() throws Exception {
    if ( m_keyspaceName == null || m_keyspaceName.length() == 0 ) {
      throw new Exception( BaseMessages.getString( PKG,
        "CassandraConnection.Error.NoKeyspaceHasBeenSet" ) ); //$NON-NLS-1$
    }

    return m_client.describe_keyspace( m_keyspaceName );
  }

  public void close() {
    try {
      closeConnection();
    } catch ( Exception ex ) {
      // swallow this exception since it will never get thrown
    }
  }

  private void checkOpen() throws Exception {
    if ( m_transport == null && m_client == null ) {
      openConnection();

      if ( m_options != null ) {
        for ( Map.Entry<String, String> e : m_options.entrySet() ) {
          if ( e.getKey().equals( CassandraUtils.CQLOptions.CQLVERSION_OPTION ) ) {
            if ( e.getValue().equals( CassandraUtils.CQLOptions.CQL3_STRING ) ) {
              // System.out.println("Connection setting CQL version to "
              // + e.getValue());
              m_client.set_cql_version( e.getValue() );
            }
          }
        }
      }
    }
  }

  /**
   * Set the Cassandra keyspace (database) to use.
   *
   * @param keySpace the name of the keyspace to use
   * @throws Exception if the keyspace doesn't exist
   */
  public void setKeyspace( String keySpace ) throws Exception {
    checkOpen();

    m_client.set_keyspace( keySpace );
    m_keyspaceName = keySpace;
  }

  public void setHosts( String hosts ) {
    String[] parts = hosts.split( "," ); //$NON-NLS-1$

    // we use only one host
    if ( parts.length > 0 ) {
      String[] parts2 = parts[ 0 ].split( ":" ); //$NON-NLS-1$
      if ( parts2.length > 0 ) {
        m_host = parts2[ 0 ].trim();
      }

      if ( parts2.length > 1 ) {
        try {
          m_port = Integer.parseInt( parts2[ 1 ].trim() );
        } catch ( NumberFormatException ex ) {
          //ignored
        }
      }
    }
  }

  public void setDefaultPort( int port ) {
    m_port = port;
  }

  public void setUsername( String username ) {
    m_username = username;
  }

  public void setPassword( String password ) {
    m_password = password;
  }

  public void setAdditionalOptions( Map<String, String> opts ) {
    m_options = opts;

    if ( m_options != null && m_options.size() > 0 ) {
      for ( Map.Entry<String, String> entry : m_options.entrySet() ) {
        if ( entry.getKey().equalsIgnoreCase(
          CassandraUtils.ConnectionOptions.SOCKET_TIMEOUT ) ) {
          String v = entry.getValue().trim();

          try {
            m_timeout = Integer.parseInt( v );
          } catch ( NumberFormatException ex ) {
            //ignored
          }
        } else if ( entry.getKey().equalsIgnoreCase(
          CassandraUtils.ConnectionOptions.MAX_LENGTH ) ) {
          String v = entry.getValue().trim();

          try {
            m_maxlength = Integer.parseInt( v );
          } catch ( NumberFormatException ex ) {
            //ignored
          }
        }
      }
    }
  }

  public Map<String, String> getAdditionalOptions() {
    return m_options;
  }

  public void openConnection() throws Exception {
    TSocket socket = new TSocket( m_host, m_port );
    if ( m_timeout > 0 ) {
      socket.setTimeout( m_timeout );
    }
    if ( m_maxlength > 0 ) {
      m_transport = new TFramedTransport( socket, m_maxlength );
    } else {
      m_transport = new TFramedTransport( socket );
    }

    TProtocol protocol = new TBinaryProtocol( m_transport );
    m_client = new Cassandra.Client( protocol );
    m_transport.open();

    if ( !Utils.isEmpty( m_username ) && !Utils.isEmpty( m_password ) ) {
      Map<String, String> creds = new HashMap<String, String>();
      creds.put( "username", m_username ); //$NON-NLS-1$
      creds.put( "password", m_password ); //$NON-NLS-1$
      m_client.login( new AuthenticationRequest( creds ) );
    }
  }

  public void closeConnection() throws Exception {
    if ( m_transport != null ) {
      m_transport.close();
      m_transport = null;
      m_client = null;
    }
  }

  public Object getUnderlyingConnection() {
    return this;
  }

  public boolean supportsCQL() {
    return true;
  }

  public boolean supportsNonCQL() {
    return true;
  }

  public Keyspace getKeyspace( String keyspacename ) throws Exception {

    LegacyKeyspace ks = new LegacyKeyspace();
    ks.setConnection( this );
    ks.setOptions( m_options );
    ks.setKeyspace( keyspacename );

    return ks;
  }

}
