/*******************************************************************************
 *
 * Copyright (C) 2018 by Hitachi Vantara : http://www.pentaho.com
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

import java.net.InetSocketAddress;
import java.time.Duration;
import java.util.ArrayList;
import java.util.Collections;
import java.util.HashMap;
import java.util.List;
import java.util.Map;

import org.apache.commons.lang.StringUtils;
import org.pentaho.cassandra.util.CassandraUtils;
import org.pentaho.cassandra.spi.Connection;
import org.pentaho.cassandra.spi.Keyspace;
import org.pentaho.di.core.util.Utils;
import com.datastax.oss.driver.api.core.config.DefaultDriverOption;
import com.datastax.oss.driver.api.core.config.DriverConfigLoader;
import com.datastax.oss.driver.api.core.config.ProgrammaticDriverConfigLoaderBuilder;

import com.datastax.oss.driver.api.core.CqlSession;
import com.datastax.oss.driver.api.core.CqlSessionBuilder;
import com.datastax.oss.driver.api.core.metadata.schema.KeyspaceMetadata;
import com.datastax.oss.driver.api.core.type.codec.MappingCodec;
import com.datastax.oss.driver.api.core.type.codec.registry.CodecRegistry;
import com.datastax.oss.driver.api.core.type.codec.TypeCodec;
import com.datastax.oss.driver.api.core.type.codec.TypeCodecs;
import com.datastax.oss.driver.api.core.type.reflect.GenericType;


/**
 * connection using standard datastax driver<br>
 * not thread-safe
 */
public class DriverConnection implements Connection, AutoCloseable {
  public static final String LZ4_COMPRESSION = "lz4";

  private String host;
  private int port = 9042;
  private String username, password;
  private Map<String, String> opts = new HashMap<>();
  private boolean useCompression;

  private CqlSession session;
  private Map<String, CqlSession> sessions = new HashMap<>();

  private boolean expandCollection = true;

  public DriverConnection() {
  }

  public DriverConnection( String host, int port ) {
    this.host = host;
    this.port = port;
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
    session = baseSessionBuilder().build();
  }

  @Override
  public void closeConnection() throws Exception {
    if ( session != null ) {
      session.close();
    }
    sessions.forEach( ( name, session ) -> session.close() );
    sessions.clear();
  }

  @Override
  public CqlSession getUnderlyingConnection() {
    return session;
  }

  public void setUseCompression( boolean useCompression ) {
    this.useCompression = useCompression;
  }

  public CqlSession getSession( String keyspace ) {
    return sessions.computeIfAbsent( keyspace, ks -> baseSessionBuilder().withKeyspace( ks ).build() );
  }

  private CqlSessionBuilder baseSessionBuilder() {
    CqlSessionBuilder builder = CqlSession.builder();
    builder.addContactPoints( getAddresses() );
    if ( !Utils.isEmpty( username ) ) {
      builder = builder.withCredentials( username, password );
    }
    ProgrammaticDriverConfigLoaderBuilder loaderBuilder = DriverConfigLoader.programmaticBuilder();
    if ( opts.containsKey( CassandraUtils.ConnectionOptions.SOCKET_TIMEOUT ) ) {
      int timeoutMs = Integer.parseUnsignedInt( opts.get( CassandraUtils.ConnectionOptions.SOCKET_TIMEOUT ).trim() );
      loaderBuilder.withDuration( DefaultDriverOption.CONNECTION_CONNECT_TIMEOUT, Duration.ofMillis( timeoutMs ) );
    }
    if ( useCompression ) {
      loaderBuilder.withString( DefaultDriverOption.PROTOCOL_COMPRESSION, LZ4_COMPRESSION );
    }
    loaderBuilder.withString( DefaultDriverOption.LOAD_BALANCING_POLICY_CLASS, "BasicLoadBalancingPolicy" );
    builder.withConfigLoader( loaderBuilder.build() );
    registerCodecs( builder );

    return builder;
  }

  @Override
  public Keyspace getKeyspace( String keyspacename ) throws Exception {
    if ( session == null ) {
      openConnection();
    }
    KeyspaceMetadata keyspace = session.getMetadata().getKeyspace( keyspacename ).orElse( null );
    return new DriverKeyspace( this, keyspace );
  }

  @Override
  public void close() throws Exception {
    closeConnection();
  }

  public boolean isExpandCollection() {
    return expandCollection;
  }

  protected List<InetSocketAddress> getAddresses() {
    if ( !host.contains( "," ) && !host.contains( ":" ) ) {
      return Collections.singletonList( new InetSocketAddress( host, port ) );
    } else {
      String[] hostss = StringUtils.split( this.host, "," );
      List<InetSocketAddress> hosts = new ArrayList<>( hostss.length );
      for ( int i = 0; i < hostss.length; i++ ) {
        String[] hostPair = StringUtils.split( hostss[i], ":" );
        String hostName = hostPair[0].trim();
        int port = this.port;
        if ( hostPair.length > 1 ) {
          try {
            port = Integer.parseInt( hostPair[1].trim() );
          } catch ( NumberFormatException nfe ) {
            // ignored, default
          }
        }
        hosts.add( new InetSocketAddress( hostName, port ) );
      }
      return hosts;
    }
  }

  private void registerCodecs( CqlSessionBuilder builder ) {
    // where kettle expects specific types that don't match default deserialization
    MappingCodec<Integer, Long> codecLong = new MappingCodec<Integer, Long>( TypeCodecs.INT, GenericType.LONG ) {
      @Override
      protected Long innerToOuter( Integer value ) {
        return value == null ? null : value.longValue();
      }
      @Override
      protected Integer outerToInner( Long value ) {
        return value == null ? null : value.intValue();
      }
    };

    MappingCodec<Double, Float> codecFloat = new MappingCodec<Double, Float>( TypeCodecs.DOUBLE, GenericType.FLOAT ) {
      @Override
      protected Double outerToInner( Float value ) {
        return value == null ? null : value.doubleValue();
      }
      @Override
      protected Float innerToOuter( Double value ) {
        return value == null ? null : value.floatValue();
      }
    };

     builder.addTypeCodecs( codecLong, codecFloat );
  }

}
