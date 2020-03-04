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
import com.datastax.oss.driver.api.core.metadata.schema.KeyspaceMetadata;
import com.datastax.oss.driver.api.core.metadata.schema.TableMetadata;
import com.datastax.oss.driver.api.querybuilder.QueryBuilder;
import com.datastax.oss.driver.api.querybuilder.SchemaBuilder;
import com.datastax.oss.driver.api.querybuilder.schema.CreateTable;
import com.datastax.oss.driver.api.querybuilder.schema.CreateTableStart;
import org.pentaho.cassandra.spi.CQLRowHandler;
import org.pentaho.cassandra.spi.Connection;
import org.pentaho.cassandra.spi.IQueryMetaData;
import org.pentaho.cassandra.spi.ITableMetaData;
import org.pentaho.cassandra.spi.Keyspace;
import org.pentaho.cassandra.util.CassandraUtils;
import org.pentaho.di.core.logging.LogChannelInterface;
import org.pentaho.di.core.row.RowMetaInterface;
import org.pentaho.di.core.row.ValueMetaInterface;
import org.pentaho.di.core.util.Utils;
import org.pentaho.di.i18n.BaseMessages;

import javax.naming.OperationNotSupportedException;
import java.util.List;
import java.util.Map;
import java.util.Optional;
import java.util.stream.Collectors;

public class DriverKeyspace implements Keyspace {

  private static Class<?> PKG = DriverKeyspace.class;

  protected DriverConnection conn;
  private KeyspaceMetadata meta;
  private String name;
  private CqlSession session;

  public DriverKeyspace( DriverConnection conn, KeyspaceMetadata keyspace, CqlSession session ) {
    this.meta = keyspace;
    this.conn = conn;
    this.name = keyspace.getName().toString();
    this.session = session;
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
    throw new OperationNotSupportedException();
  }

  @Override
  public List<String> getTableNamesCQL3() throws Exception {
    return meta.getTables().values().stream().map( tab -> tab.getName().toString() ).collect( Collectors.toList() );
  }

  @Override
  public boolean tableExists( String tableName ) throws Exception {
    if ( meta == null ) {
      return false;
    }
    return meta.getTable( tableName ).isPresent();
  }

  @Override
  public ITableMetaData getTableMetaData( String familyName ) throws Exception {
    Optional<TableMetadata> tableMeta = meta.getTable( familyName );
    if ( tableMeta.isPresent() ) {
      return new TableMetaData( this, tableMeta.get() );
    } else {
      return null;
    }
  }

  @Override
  public IQueryMetaData getQueryMetaData( String query ) throws Exception {
    DriverQueryMetaData queryMeta = new DriverQueryMetaData();
    queryMeta.setKeyspace( this );
    queryMeta.parseQuery( query );
    return queryMeta;
  }

  @Override
  public boolean createTable( String tableName, RowMetaInterface rowMeta, List<Integer> keyIndexes,
      String createTableWithClause, LogChannelInterface log )
    throws Exception {

    CreateTableStart createTableStart = SchemaBuilder.createTable( getName(), tableName ).ifNotExists();
    CreateTable createTable = null;

    for ( Integer keyIndex : keyIndexes ) {
      ValueMetaInterface keyMeta = rowMeta.getValueMeta( keyIndex );
      if ( createTable == null ) {
        createTable =
            createTableStart.withPartitionKey( keyMeta.getName(),
              CassandraUtils.getCassandraDataTypeFromValueMeta( keyMeta ) );
      } else {
        createTable =
            createTable.withPartitionKey( keyMeta.getName(),
              CassandraUtils.getCassandraDataTypeFromValueMeta( keyMeta ) );
      }
    }

    for ( int i = 0; i < rowMeta.size(); i++ ) {
      if ( !keyIndexes.contains( i ) ) {
        ValueMetaInterface valueMeta = rowMeta.getValueMeta( i );
        createTable =
            createTable.withColumn( valueMeta.getName(),
              CassandraUtils.getCassandraDataTypeFromValueMeta( valueMeta ) );
      }
    }

    String createCql = createTable.asCql();
    if ( !Utils.isEmpty( createTableWithClause ) ) {
      StringBuilder cql = new StringBuilder( createCql );
      if ( !createTableWithClause.toLowerCase().trim().startsWith( "with" ) ) {
        cql.append( " WITH " );
      }
      cql.append( createTableWithClause );
      createCql = cql.toString();
    }

    log.logDetailed( BaseMessages.getString( PKG, "DriverKeyspace.CreateTable.Message" ), tableName, createCql );

    getSession().execute( createCql );

    return true;

  }

  /**
   * Actually an ALTER to add columns, not UPDATE. Purpose of keyIndexes yet to be determined
   */
  @Override
  public void updateTableCQL3( String tableName, RowMetaInterface rowMeta, List<Integer> keyIndexes,
      LogChannelInterface log )
    throws Exception {

    CqlSession session = getSession();
    ITableMetaData table = getTableMetaData( tableName );

    for ( ValueMetaInterface valueMeta : rowMeta.getValueMetaList() ) {
      if ( !table.columnExistsInSchema( valueMeta.getName() ) ) {
        String cqlString =
            SchemaBuilder.alterTable( tableName ).addColumn( valueMeta.getName(),
              CassandraUtils.getCassandraDataTypeFromValueMeta( valueMeta ) ).asCql();
        log.logDetailed( BaseMessages.getString( PKG, "DriverKeyspace.AlterTable.Message" ), tableName, cqlString );
        session.execute( cqlString );
      }
    }
  }

  @Override
  public void truncateTable( String tableName, LogChannelInterface log ) throws Exception {
    getSession().execute( QueryBuilder.truncate( tableName ).build() );
  }

  protected CqlSession getSession() {
    return conn.getSession( name );
    //return this.session;
  }

  @Override
  public CQLRowHandler getCQLRowHandler() {
    return new DriverCQLRowHandler( this, getSession(), getConnection().isExpandCollection() );
  }
}
