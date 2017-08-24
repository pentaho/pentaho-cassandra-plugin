/*******************************************************************************
 *
 * Copyright (C) 2017 by Pentaho : http://www.pentaho.com
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

package org.pentaho.cassandra.driverimpl;

import java.util.List;
import java.util.Map;
import java.util.stream.Collectors;

import org.pentaho.cassandra.CassandraUtils;
import org.pentaho.cassandra.spi.CQLRowHandler;
import org.pentaho.cassandra.spi.ColumnFamilyMetaData;
import org.pentaho.cassandra.spi.Connection;
import org.pentaho.cassandra.spi.Keyspace;
import org.pentaho.cassandra.spi.NonCQLRowHandler;
import org.pentaho.di.core.logging.LogChannelInterface;
import org.pentaho.di.core.row.RowMetaInterface;
import org.pentaho.di.core.row.ValueMetaInterface;
import org.pentaho.di.core.util.Utils;

import com.datastax.driver.core.DataType;
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
    // TODO ??
    conn.setAdditionalOptions( options );
  }

  @Override
  public void executeCQL( String cql, String compresson, String consistencyLevel, LogChannelInterface log )
    throws UnsupportedOperationException, Exception {
    conn.getSession( name ).execute( cql );
  }

  @Override
  public void createKeyspace( String keyspaceName, Map<String, Object> options, LogChannelInterface log )
    throws UnsupportedOperationException, Exception {
    SchemaBuilder.createKeyspace( keyspaceName );
  }

  @Override
  public List<String> getColumnFamilyNames() throws Exception {
    return meta.getTables().stream().map( tab -> tab.getName() ).collect( Collectors.toList() );
  }

  @Override
  public boolean columnFamilyExists( String colFamName ) throws Exception {
    return meta.getTable( colFamName ) != null;
  }

  @Override
  public ColumnFamilyMetaData getColumnFamilyMetaData( String familyName ) throws Exception {
    TableMetadata tableMeta = meta.getTable( familyName );
    return new TableColumnFamilyMetaData( this, tableMeta );
  }

  @Override
  public boolean createColumnFamily( String colFamName, RowMetaInterface rowMeta, List<Integer> keyIndexes,
      String createTableWithClause, LogChannelInterface log ) throws Exception {
    Create createTable = SchemaBuilder.createTable( colFamName );
    for ( int i = 0; i < rowMeta.size(); i++ ) {
      if ( !keyIndexes.contains( i ) ) {
        ValueMetaInterface valueMeta = rowMeta.getValueMeta( i );
        createTable.addColumn( valueMeta.getName(), getDataType( valueMeta ) );
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
    return true; // TODO:?..
  }

  /**
   * Actually an ALTER to add columns, not UPDATE. Purpose of keyIndexes yet to be determined
   */
  @Override
  public void updateColumnFamily( String colFamName, RowMetaInterface rowMeta, List<Integer> keyIndexes,
      LogChannelInterface log ) throws UnsupportedOperationException, Exception {
    Session session = getSession();
    ColumnFamilyMetaData colFamily = getColumnFamilyMetaData( colFamName );
    for ( ValueMetaInterface valueMeta : rowMeta.getValueMetaList() ) {
      if ( !colFamily.columnExistsInSchema( valueMeta.getName() ) ) {
        session.execute( SchemaBuilder.alterTable( colFamName ).alterColumn( valueMeta.getName() ).type(
            getDataType( valueMeta ) ) );
      }
    }
  }

  @Override
  public void truncateColumnFamily( String colFamName, LogChannelInterface log ) throws UnsupportedOperationException,
    Exception {
    getSession().execute( QueryBuilder.truncate( colFamName ) );
  }

  protected Session getSession() {
    return conn.getSession( name );
  }

  private DataType getDataType( ValueMetaInterface valueMeta ) {
    return CassandraUtils.getCassandraDataTypeFromValueMeta( valueMeta );
  }

  @Override
  public CQLRowHandler getCQLRowHandler() {
    return new DriverCQLRowHandler( this, getSession(), getConnection().isExpandCollection() );
  }

  @Override
  public NonCQLRowHandler getNonCQLRowHandler() {
    throw new UnsupportedOperationException();
  }

}
