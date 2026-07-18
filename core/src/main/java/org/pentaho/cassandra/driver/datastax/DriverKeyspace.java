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

import java.util.List;
import java.util.Map;
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

import com.datastax.driver.core.KeyspaceMetadata;
import com.datastax.driver.core.Session;
import com.datastax.driver.core.TableMetadata;
import com.datastax.driver.core.querybuilder.QueryBuilder;
import com.datastax.driver.core.schemabuilder.Create;
import com.datastax.driver.core.schemabuilder.SchemaBuilder;

public class DriverKeyspace implements Keyspace {

  protected DriverConnection conn;
  private KeyspaceMetadata meta;
  private String name;

  public DriverKeyspace( DriverConnection conn, KeyspaceMetadata keyspace ) {
    this.meta = keyspace;
    this.conn = conn;
    this.name = keyspace.getName();
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
    return meta.getTables().stream().map( tab -> tab.getName() ).collect( Collectors.toList() );
  }

  @Override
  public boolean tableExists( String tableName ) throws Exception {
    return meta.getTable( tableName ) != null;
  }

  @Override
  public ITableMetaData getTableMetaData( String familyName ) throws Exception {
    TableMetadata tableMeta = meta.getTable( familyName );
    return new TableMetaData( this, tableMeta );
  }

  @Override
  public boolean createTable( String tableName, RowMetaInterface rowMeta, List<Integer> keyIndexes,
      String createTableWithClause, LogChannelInterface log ) throws Exception {
    Create createTable = SchemaBuilder.createTable( tableName );
    for ( int i = 0; i < rowMeta.size(); i++ ) {
      if ( !keyIndexes.contains( i ) ) {
        ValueMetaInterface valueMeta = rowMeta.getValueMeta( i );
        createTable.addColumn( valueMeta.getName(), CassandraUtils.getCassandraDataTypeFromValueMeta( valueMeta ) );
      } else {
        ValueMetaInterface key = rowMeta.getValueMeta( i );
        createTable.addPartitionKey( key.getName(), CassandraUtils.getCassandraDataTypeFromValueMeta( key ) );
      }
    }
    if ( !Utils.isEmpty( createTableWithClause ) ) {
      StringBuilder cql = new StringBuilder( createTable.toString() );
      if ( !createTableWithClause.toLowerCase().trim().startsWith( "with" ) ) {
        cql.append( " WITH " );
      }
      cql.append( createTableWithClause );
      getSession().execute( cql.toString() );
    } else {
      getSession().execute( createTable );
    }
    return true;
  }

  /**
   * Actually an ALTER to add columns, not UPDATE. Purpose of keyIndexes yet to be determined
   */
  @Override
  public void updateTableCQL3( String tableName, RowMetaInterface rowMeta, List<Integer> keyIndexes,
      LogChannelInterface log ) throws Exception {
    Session session = getSession();
    ITableMetaData table = getTableMetaData( tableName );
    for ( ValueMetaInterface valueMeta : rowMeta.getValueMetaList() ) {
      if ( !table.columnExistsInSchema( valueMeta.getName() ) ) {
        session.execute( SchemaBuilder.alterTable( tableName ).alterColumn( valueMeta.getName() ).type(
            CassandraUtils.getCassandraDataTypeFromValueMeta( valueMeta ) ) );
      }
    }
  }

  @Override
  public void truncateTable( String tableName, LogChannelInterface log ) throws Exception {
    getSession().execute( QueryBuilder.truncate( tableName ) );
  }

  protected Session getSession() {
    return conn.getSession( name );
  }

  @Override
  public CQLRowHandler getCQLRowHandler() {
    return new DriverCQLRowHandler( this, getSession(), getConnection().isExpandCollection() );
  }
}
