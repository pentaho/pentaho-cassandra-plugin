/*******************************************************************************
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

package org.pentaho.cassandra.datastax;

import com.datastax.oss.driver.api.core.CqlSession;
import com.datastax.oss.driver.api.core.CqlSessionBuilder;
import com.datastax.oss.driver.api.core.config.DefaultDriverOption;
import com.datastax.oss.driver.api.core.config.DriverConfigLoader;
import com.datastax.oss.driver.api.core.config.ProgrammaticDriverConfigLoaderBuilder;
import com.datastax.oss.driver.api.core.metadata.schema.KeyspaceMetadata;
import org.apache.commons.lang.StringUtils;
import org.pentaho.cassandra.spi.Connection;
import org.pentaho.cassandra.spi.Keyspace;
import org.pentaho.cassandra.util.CassandraUtils;
import org.pentaho.di.core.util.Utils;

import javax.net.ssl.SSLContext;
import javax.net.ssl.TrustManager;
import javax.net.ssl.X509TrustManager;
import java.io.File;
import java.net.InetSocketAddress;
import java.security.KeyManagementException;
import java.security.NoSuchAlgorithmException;
import java.security.SecureRandom;
import java.security.cert.CertificateException;
import java.security.cert.X509Certificate;
import java.time.Duration;
import java.util.Arrays;
import java.util.HashMap;
import java.util.Map;

/**
 * connection using standard datastax driver<br>
 * not thread-safe
 */
public class DriverConnection implements Connection, AutoCloseable {

  private String host;
  private int port = 9042;
  private String username, password;
  private Map<String, String> opts = new HashMap<>();
  private boolean useCompression;
  private String localDataCenter = null;

  private CqlSession session;
  private Map<String, CqlSession> sessions = new HashMap<>();

  private boolean expandCollection = true;

  public DriverConnection() {
  }

  public DriverConnection( String host, int port ) {
    this.host = host;
    this.port = port;
  }

  public DriverConnection( String host, int port, String localDataCenter ) {
    this.host = host;
    this.port = port;
    this.localDataCenter = localDataCenter;
  }

  @Override
  public void setHosts( String hosts ) {
    this.host = hosts;
  }

  @Override
  public void setDefaultPort( int port ) {
    this.port = port;
  }

  @Override
  public void setUsername( String username ) {
    this.username = username;
  }

  @Override
  public void setPassword( String password ) {
    this.password = password;
  }

  public String getLocalDataCenter() {
    return localDataCenter;
  }

  public void setLocalDataCenter( String localDataCenter ) {
    this.localDataCenter = localDataCenter;
  }

  @Override
  public void setAdditionalOptions( Map<String, String> opts ) {
    this.opts = opts;
    if ( opts.containsKey( CassandraUtils.ConnectionOptions.COMPRESSION ) ) {
      setUseCompression( true );
    }
  }

  @Override
  public Map<String, String> getAdditionalOptions() {
    return opts;
  }

  @Override
  public void openConnection() throws Exception {
    session = getCluster();
  }

  public void openConnection( String keySpace ) throws Exception {
    session = getCluster( keySpace );
  }

  @Override
  public void closeConnection() throws Exception {

    Exception ex = null;

    if ( session != null ) {
      try {
        session.close();
      } catch ( Exception e ) {
        ex = e;
      }
    }
    try {
      sessions.forEach( ( name, session ) -> session.close() );
    } catch ( Exception e ) {
      ex = e;
    }
    sessions.clear();

    if ( ex != null ) {
      throw ex;
    }

  }

  @Override
  public CqlSession getUnderlyingConnection() {
    return session;
  }

  public void setUseCompression( boolean useCompression ) {
    this.useCompression = useCompression;
  }

  public CqlSession getCluster() {
    return getCluster( null );
  }

  public CqlSession getCluster( String keySpace ) {

    ClassLoader prevLoader = Thread.currentThread().getContextClassLoader();

    try {

      ClassLoader loader = CqlSession.class.getClassLoader();

      Thread.currentThread().setContextClassLoader( loader );

      CqlSessionBuilder builder = CqlSession.builder().withClassLoader( CqlSession.class.getClassLoader() );

      boolean usesApplicationConf = !Utils.isEmpty( opts.get( CassandraUtils.ConnectionOptions.APPLICATION_CONF ) );

      if ( usesApplicationConf ) {
        builder.withConfigLoader( DriverConfigLoader.fromFile( new File( opts.get( CassandraUtils.ConnectionOptions.APPLICATION_CONF ) ) ) );
      }

      if ( getAddresses() != null && getAddresses().length > 0 ) {
        builder.addContactPoints( Arrays.asList( getAddresses() ) );
      }

      if ( !Utils.isEmpty( keySpace ) ) {
        builder = builder.withKeyspace( keySpace );
      }

      if ( !Utils.isEmpty( username ) ) {
        builder = builder.withAuthCredentials( username, password );
      }

      if ( !Utils.isEmpty( localDataCenter ) ) {
        builder = builder.withLocalDatacenter( localDataCenter );
      }

      if ( !usesApplicationConf ) {

        ProgrammaticDriverConfigLoaderBuilder configBuilder = DriverConfigLoader.programmaticBuilder();

        if ( opts.containsKey( CassandraUtils.ConnectionOptions.SOCKET_TIMEOUT ) ) {
          try {
            int timeoutMs = Integer.parseInt( opts.get( CassandraUtils.ConnectionOptions.SOCKET_TIMEOUT ).trim() );
            if ( timeoutMs > 0 ) {
              configBuilder =
                configBuilder.withDuration( DefaultDriverOption.REQUEST_TIMEOUT, Duration.ofMillis( timeoutMs ) );
            }
          } catch ( NumberFormatException nfe ) {
            // Ignore the connect timeout if not specified properly
          }
        }

        /*
         * if ( opts.containsKey( CassandraUtils.ConnectionOptions.READ_TIMEOUT ) ) { try { int readTimeoutMs =
         * Integer.parseInt( opts.get( CassandraUtils.ConnectionOptions.READ_TIMEOUT ).trim() ); if ( readTimeoutMs > 0 )
         * { socketOptions.setReadTimeoutMillis( readTimeoutMs ); } } catch ( NumberFormatException e ) { // Ignore the
         * read timeout if not specified properly } }
         */

        if ( opts.containsKey( CassandraUtils.ConnectionOptions.ROW_FETCH_SIZE ) ) {
          try {
            int rowFetchSize = Integer.parseInt( opts.get( CassandraUtils.ConnectionOptions.ROW_FETCH_SIZE ).trim() );
            if ( rowFetchSize > 0 ) {
              configBuilder = configBuilder.withInt( DefaultDriverOption.REQUEST_PAGE_SIZE, rowFetchSize );
            }
          } catch ( NumberFormatException e ) {
            // Ignore the row fetch size if not specified properly
          }
        }

        if ( useCompression ) {
          configBuilder = configBuilder.withString( DefaultDriverOption.PROTOCOL_COMPRESSION, "lz4" );
        }

        builder.withConfigLoader( configBuilder.build() );

        if ( opts.containsKey( CassandraUtils.ConnectionOptions.SSL ) ) {

          if ( opts.containsKey( CassandraUtils.ConnectionOptions.SSL_NO_VALIDATION ) ) {
            try {
              SSLContext sslContext = SSLContext.getInstance( "SSL" );
              sslContext.init( null, new TrustManager[] {
                new X509TrustManager() {
                  @Override
                  public X509Certificate[] getAcceptedIssuers() {
                    return null;
                  }

                  @Override
                  public void checkServerTrusted( X509Certificate[] chain, String authType ) throws CertificateException {

                  }

                  @Override
                  public void checkClientTrusted( X509Certificate[] chain, String authType ) throws CertificateException {

                  }
                } }, new SecureRandom() );

              builder = builder.withSslContext( sslContext );

            } catch ( NoSuchAlgorithmException | KeyManagementException e ) {
              throw new RuntimeException( e );
            }

          } else {
            try {
              builder = builder.withSslContext( SSLContext.getDefault() );
            } catch ( NoSuchAlgorithmException e ) {
              throw new RuntimeException( e );
            }
          }
        }
      }

      return builder.build();

    } finally {
      Thread.currentThread().setContextClassLoader( prevLoader );
    }

  }

  public CqlSession getSession( String keyspace ) {
    return sessions.computeIfAbsent( keyspace, ks -> getCluster( ks ) );
  }

  @Override
  public Keyspace getKeyspace( String keyspacename ) throws Exception {
    KeyspaceMetadata keyspace = getCluster().getMetadata().getKeyspace( keyspacename ).get();
    return new DriverKeyspace( this, keyspace, session );
  }

  @Override
  public void close() throws Exception {
    closeConnection();
  }

  public boolean isExpandCollection() {
    return expandCollection;
  }

  protected InetSocketAddress[] getAddresses() {
    if ( Utils.isEmpty( host ) ) {
      return null;
    }
    if ( !host.contains( "," ) && !host.contains( ":" ) ) {
      return new InetSocketAddress[] {
        new InetSocketAddress( host, port )};
    } else {
      String[] hosts = StringUtils.split( this.host, "," );
      InetSocketAddress[] hostAddresses = new InetSocketAddress[hosts.length];
      for ( int i = 0; i < hostAddresses.length; i++ ) {
        String[] hostPair = StringUtils.split( hosts[i], ":" );
        String hostName = hostPair[0].trim();
        int port = this.port;
        if ( hostPair.length > 1 ) {
          try {
            port = Integer.parseInt( hostPair[1].trim() );
          } catch ( NumberFormatException nfe ) {
            // ignored, default
          }
        }
        hostAddresses[i] = new InetSocketAddress( hostName, port );
      }
      return hostAddresses;
    }
  }

}
