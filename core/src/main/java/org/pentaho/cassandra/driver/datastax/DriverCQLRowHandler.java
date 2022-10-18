/*******************************************************************************
 *
 * Copyright (C) 2020 by Hitachi Vantara : http://www.pentaho.com
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

import java.math.BigDecimal;
import java.util.ArrayList;
import java.util.Arrays;
import java.util.Collection;
import java.util.Date;
import java.util.List;
import java.util.Map;
import java.util.concurrent.TimeUnit;
import java.util.concurrent.TimeoutException;

import org.apache.commons.lang.NotImplementedException;
import org.pentaho.cassandra.spi.CQLRowHandler;
import org.pentaho.cassandra.spi.ITableMetaData;
import org.pentaho.cassandra.spi.Keyspace;
import org.pentaho.cassandra.util.CassandraUtils;
import org.pentaho.di.core.exception.KettleException;
import org.pentaho.di.core.logging.LogChannelInterface;
import org.pentaho.di.core.row.RowDataUtil;
import org.pentaho.di.core.row.RowMetaInterface;
import org.pentaho.di.core.row.ValueMetaInterface;
import org.pentaho.di.core.util.Utils;
import org.pentaho.di.i18n.BaseMessages;
import org.pentaho.di.trans.step.StepInterface;

import com.datastax.driver.core.ColumnDefinitions;
import com.datastax.driver.core.ConsistencyLevel;
import com.datastax.driver.core.ResultSet;
import com.datastax.driver.core.Row;
import com.datastax.driver.core.Session;
import com.datastax.driver.core.querybuilder.Batch;
import com.datastax.driver.core.querybuilder.Insert;
import com.datastax.driver.core.querybuilder.QueryBuilder;

public class DriverCQLRowHandler implements CQLRowHandler {

  private final Session session;
  DriverKeyspace keyspace;
  ResultSet result;

  ColumnDefinitions columns;

  private int batchInsertTimeout;
  private int ttlSec;

  private boolean unloggedBatch = true;

  private boolean expandCollection = true;
  private int primaryCollectionOutputIndex = -1;

  public DriverCQLRowHandler( DriverKeyspace keyspace, Session session, boolean expandCollection ) {
    this.keyspace = keyspace;
    this.session = session;
    this.expandCollection = expandCollection;
  }

  public DriverCQLRowHandler( DriverKeyspace keyspace ) {
    this( keyspace, keyspace.getConnection().getSession( keyspace.getName() ), true );
  }

  public boolean supportsCQLVersion( int cqMajorlVersion ) {
    return cqMajorlVersion >= 3 && cqMajorlVersion <= 3;
  }

  @Override
  public void setOptions( Map<String, String> options ) {
    keyspace.setOptions( options );
    if ( options.containsKey( CassandraUtils.BatchOptions.BATCH_TIMEOUT ) ) {
      batchInsertTimeout = Integer.parseInt( options.get( CassandraUtils.BatchOptions.BATCH_TIMEOUT ) );
    }
    if ( options.containsKey( CassandraUtils.BatchOptions.TTL ) ) {
      ttlSec = Integer.parseInt( options.get( CassandraUtils.BatchOptions.TTL ) );
    }
  }

  @Override
  public void setKeyspace( Keyspace keyspace ) {
    this.keyspace = (DriverKeyspace) keyspace;
  }

  @Override
  public void newRowQuery( StepInterface requestingStep, String tableName, String cqlQuery, String compress,
    String consistencyLevel, LogChannelInterface log ) throws Exception {
    result = getSession().execute( cqlQuery );
    columns = result.getColumnDefinitions();
    if ( expandCollection ) {
      for ( int i = 0; i < columns.size(); i++ ) {
        if ( columns.getType( i ).isCollection() ) {
          if ( primaryCollectionOutputIndex < 0 ) {
            primaryCollectionOutputIndex = i;
          } else if ( !keyspace.getTableMetaData( tableName ).getValueMetaForColumn( columns.getName( i ) )
              .isString() ) {
            throw new KettleException( BaseMessages.getString( DriverCQLRowHandler.class,
                "DriverCQLRowHandler.Error.CantHandleAdditionalCollectionsThatAreNotOfTypeText" ) );
          }
        }
      }
    }
  }

  @Override
  public Object[][] getNextOutputRow( RowMetaInterface outputRowMeta, Map<String, Integer> outputFormatMap )
    throws Exception {
    if ( result == null ||  result.isExhausted() ) {
      result = null;
      columns = null;
      return null;
    }
    Row row = result.one();
    Object[][] outputRowData = new Object[1][];
    Object[] baseOutputRowData = RowDataUtil.allocateRowData( Math.max( outputRowMeta.size(), columns.size() ) );
    for ( int i = 0; i < columns.size(); i++ ) {
      baseOutputRowData[i] = readValue( outputRowMeta.getValueMeta( i ), row, i );
    }
    outputRowData[0] = baseOutputRowData;
    if ( primaryCollectionOutputIndex > 0 ) {
      Collection<?> collection = (Collection<?>) row.getObject( primaryCollectionOutputIndex );
      if ( collection != null && !collection.isEmpty()  ) {
        outputRowData = new Object[collection.size()][];
        int i = 0;
        for ( Object obj : collection ) {
          outputRowData[i] = Arrays.copyOf( baseOutputRowData, baseOutputRowData.length );
          outputRowData[i++][primaryCollectionOutputIndex] = obj;
        }
      } else {
        outputRowData[0][primaryCollectionOutputIndex] = null;
      }
    }

    return outputRowData;
  }

  public static Object readValue( ValueMetaInterface meta, Row row, int i ) {

    if ( row.isNull( i ) ) {
      return null;
    }

    switch ( meta.getType() ) {

      case ValueMetaInterface.TYPE_INTEGER:
      case ValueMetaInterface.TYPE_NUMBER:
      case ValueMetaInterface.TYPE_BIGNUMBER:
      case ValueMetaInterface.TYPE_BOOLEAN:
      case ValueMetaInterface.TYPE_DATE:
      case ValueMetaInterface.TYPE_BINARY:
        /*
         * https://docs.datastax.com/en/cql/3.3/cql/cql_reference/cql_data_types_c.html
         * 
         * Calling getLong() on a Cassandra column of INT datatype throws an NPE Calling getDouble() on a Cassandra
         * column of FLOAT datatype throws an NPE ...
         */
        ColumnDefinitions cdef = row.getColumnDefinitions();
        switch ( cdef.getType( i ).getName() ) {
          case BIGINT:
          case COUNTER:
            return row.getLong( i );
          case SMALLINT:
            return (long) row.getShort( i );
          case TINYINT:
            return (long) row.getByte( i );
          case INT:
            return (long) row.getInt( i );
          case FLOAT:
            return (double) row.getFloat( i );
          case DOUBLE:
            return row.getDouble( i );
          case DECIMAL:
            return row.getDecimal( i );
          case VARINT:
            return new BigDecimal( row.getVarint( i ) );
          case BOOLEAN:
            return row.getBool( i );
          case DATE:
            return new Date( row.getDate( i ).getMillisSinceEpoch() );
          case TIMESTAMP:
            return row.getTimestamp( i );
          case TIME:
            return row.getTime( i );
          case BLOB:
            return row.getBytes( i ).array();
          default:
            return row.getObject( i );
        }

      default:
        return row.getObject( i );
    }
  }

  public void batchInsert( RowMetaInterface inputMeta, Iterable<Object[]> rows, ITableMetaData tableMeta,
      String consistencyLevel, boolean insertFieldsNotInMetadata, LogChannelInterface log ) throws Exception {
    String[] columnNames = getColumnNames( inputMeta );
    Batch batch = unloggedBatch ? QueryBuilder.unloggedBatch() : QueryBuilder.batch();
    if ( !Utils.isEmpty( consistencyLevel ) ) {
      try {
        batch.setConsistencyLevel( ConsistencyLevel.valueOf( consistencyLevel ) );
      } catch ( Exception e ) {
        log.logError( e.getLocalizedMessage(), e );
      }
    }

    List<Integer> toRemove = new ArrayList<>();
    if ( !insertFieldsNotInMetadata ) {
      for ( int i = 0; i < columnNames.length; i++ ) {
        if ( !tableMeta.columnExistsInSchema( columnNames[i] ) ) {
          toRemove.add( i );
        }
      }
      if ( toRemove.size() > 0 ) {
        columnNames = copyExcluding( columnNames, new String[columnNames.length - toRemove.size()], toRemove );
      }
    }

    for ( Object[] row : rows ) {
      Object[] values = toRemove.size() == 0
          ? Arrays.copyOf( row, columnNames.length )
          : copyExcluding( row, new Object[ columnNames.length ], toRemove );
      Insert insert = QueryBuilder.insertInto( keyspace.getName(), tableMeta.getTableName() );
      insert = ttlSec > 0
          ? insert.using( QueryBuilder.ttl( ttlSec ) ).values( columnNames, values )
          : insert.values( columnNames, values );
      batch.add( insert );
    }
    if ( batchInsertTimeout > 0 ) {
      try {
        getSession().executeAsync( batch ).getUninterruptibly( batchInsertTimeout, TimeUnit.MILLISECONDS );
      } catch ( TimeoutException e ) {
        log.logError( BaseMessages.getString( DriverCQLRowHandler.class, "DriverCQLRowHandler.Error.TimeoutReached" ) );
      }
    } else {
      getSession().execute( batch );
    }
  }

  protected static <T> T[] copyExcluding( T[] source, T[] target, List<Integer> toRemove ) {
    int removed = toRemove.size();
    int start = 0, dest = 0, removeCount = 0;
    for ( int idx : toRemove ) {
      int len = idx - start;
      if ( len > 0 ) {
        System.arraycopy( source, start, target, dest, len );
        dest += len;
      }
      start = idx + 1;
      removeCount++;
      if ( removeCount == removed && dest < target.length ) { // last one
        System.arraycopy( source, start, target, dest, target.length - dest );
      }
    }
    return target;
  }

  private String[] getColumnNames( RowMetaInterface inputMeta ) {
    String[] columns = new String[inputMeta.size()];
    for ( int i = 0; i < inputMeta.size(); i++ ) {
      columns[i] = inputMeta.getValueMeta( i ).getName();
    }
    return columns;
  }

  @Override
  public void commitCQLBatch( StepInterface requestingStep, StringBuilder batch, String compress,
      String consistencyLevel, LogChannelInterface log ) throws Exception {
    throw new NotImplementedException();
  }

  public void setUnloggedBatch( boolean unloggedBatch ) {
    this.unloggedBatch = unloggedBatch;
  }

  public boolean isUnloggedBatch() {
    return unloggedBatch;
  }

  private Session getSession() {
    return session;
  }

  public void setTtlSec( int ttl ) {
    ttlSec = ttl;
  }

}
