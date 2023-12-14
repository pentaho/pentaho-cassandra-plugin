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

import java.util.List;
import java.util.Map;
import java.util.Optional;
import java.util.stream.Collectors;

import org.pentaho.cassandra.util.CassandraUtils;
import org.pentaho.cassandra.spi.CQLRowHandler;
import org.pentaho.cassandra.spi.ITableMetaData;
import org.pentaho.cassandra.spi.Connection;
import org.pentaho.cassandra.spi.Keyspace;
import org.pentaho.di.core.logging.LogChannelInterface;
import org.pentaho.di.core.row.RowMetaInterface;
import org.pentaho.di.core.row.ValueMetaInterface;
import org.pentaho.di.core.util.Utils;
import com.datastax.oss.driver.api.core.CqlSession;

import com.datastax.oss.driver.api.core.metadata.schema.KeyspaceMetadata;
import com.datastax.oss.driver.api.core.metadata.schema.TableMetadata;
import com.datastax.oss.driver.api.core.session.Session;
import com.datastax.oss.driver.api.querybuilder.QueryBuilder;
import com.datastax.oss.driver.api.querybuilder.schema.CreateTable;
import com.datastax.oss.driver.api.querybuilder.schema.CreateTableStart;
import com.datastax.oss.driver.api.querybuilder.schema.OngoingPartitionKey;
import com.datastax.oss.driver.api.querybuilder.SchemaBuilder;

public class DriverKeyspace implements Keyspace {

  protected DriverConnection conn;
  private KeyspaceMetadata meta;
  private String name;

  public DriverKeyspace( DriverConnection conn, KeyspaceMetadata keyspace ) {
    this.meta = keyspace;
    this.conn = conn;
    this.name = keyspace.getName().toString();
  }

  @Override
  public void setConnection( Connection conn ) throws Exception {
    this.conn = (DriverConnection) conn;
  }

  @Override
  public DriverConnection getConnection() {
    return conn;
  }

  @Override
  public void setKeyspace( String keyspaceName ) throws Exception {
    this.name = keyspaceName;
  }

  public String getName() {
    return name;
  }

  @Override
  public void setOptions( Map<String, String> options ) {
    conn.setAdditionalOptions( options );
  }

  @Override
  public void executeCQL( String cql, String compresson, String consistencyLevel, LogChannelInterface log )
    throws Exception {
    conn.getSession( name ).execute( cql );
  }

  @Override
  public void createKeyspace( String keyspaceName, Map<String, Object> options, LogChannelInterface log )
    throws Exception {
    SchemaBuilder.createKeyspace( keyspaceName );
  }

  @Override
  public List<String> getTableNamesCQL3() throws Exception {
    return meta.getTables().keySet().stream().map( tab -> tab.toString() ).collect( Collectors.toList() );
  }

  @Override
  public boolean tableExists( String tableName ) throws Exception {
    return meta.getTable( tableName ) != null;
  }

  @Override
  public ITableMetaData getTableMetaData( String familyName ) throws Exception {
    Optional<TableMetadata> tableMeta = meta.getTable( familyName );
    return tableMeta.map( tm -> new TableMetaData( this, tm ) ).orElse( null );
  }

  @Override
  public boolean createTable( String tableName, RowMetaInterface rowMeta, List<Integer> keyIndexes,
      String createTableWithClause, LogChannelInterface log ) throws Exception {
    CreateTableStart createTableStart = SchemaBuilder.createTable( tableName );

    OngoingPartitionKey ongoingPartitionKey = createTableStart;
    CreateTable createTable = null;
    // First add the partition keys. Needed to get to a CreateTable.
    for ( int i = 0; i < rowMeta.size(); i++ ) {
      if ( keyIndexes.contains( i ) ) {
        ValueMetaInterface key = rowMeta.getValueMeta( i );
        createTable = ongoingPartitionKey.withPartitionKey( key.getName(),
                                                            CassandraUtils.getCassandraDataTypeFromValueMeta( key ) );
        ongoingPartitionKey = createTable;
      }
    }
    // add the rest of the columns
    if ( createTable == null ) {
      throw new IllegalArgumentException( "partition keys in keyIndexes are required" );
    }
    for ( int i = 0; i < rowMeta.size(); i++ ) {
      if ( !keyIndexes.contains( i ) ) {
        ValueMetaInterface valueMeta = rowMeta.getValueMeta( i );
        createTable.withColumn( valueMeta.getName(), CassandraUtils.getCassandraDataTypeFromValueMeta( valueMeta ) );
      }
    }
    if ( !Utils.isEmpty( createTableWithClause ) ) {
      StringBuilder cql = new StringBuilder( createTable.asCql() );
      if ( !createTableWithClause.toLowerCase().trim().startsWith( "with" ) ) {
        cql.append( " WITH " );
      }
      cql.append( createTableWithClause );
      getSession().execute( cql.toString() );
    } else {
      getSession().execute( createTable.asCql() );
    }
    return true;
  }

  /**
   * Actually an ALTER to add columns, not UPDATE. Purpose of keyIndexes yet to be determined
   */
  @Override
  public void updateTableCQL3( String tableName, RowMetaInterface rowMeta, List<Integer> keyIndexes,
      LogChannelInterface log ) throws Exception {
    CqlSession session = getSession();
    ITableMetaData table = getTableMetaData( tableName );
    for ( ValueMetaInterface valueMeta : rowMeta.getValueMetaList() ) {
      if ( !table.columnExistsInSchema( valueMeta.getName() ) ) {
        session.execute( SchemaBuilder.alterTable( tableName ).alterColumn( valueMeta.getName(),
            CassandraUtils.getCassandraDataTypeFromValueMeta( valueMeta ) ).build() );
      }
    }
  }

  @Override
  public void truncateTable( String tableName, LogChannelInterface log ) throws Exception {
    getSession().execute( QueryBuilder.truncate( tableName ).build() );
  }

  protected CqlSession getSession() {
    return conn.getSession( name );
  }

  @Override
  public CQLRowHandler getCQLRowHandler() {
    return new DriverCQLRowHandler( this, getSession(), getConnection().isExpandCollection() );
  }
}
