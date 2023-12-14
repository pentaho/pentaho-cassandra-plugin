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
import java.util.HashMap;
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

import com.datastax.oss.driver.api.core.ConsistencyLevel;
import com.datastax.oss.driver.api.core.cql.BatchStatement;
import com.datastax.oss.driver.api.core.cql.BatchStatementBuilder;
import com.datastax.oss.driver.api.core.cql.BatchType;
import com.datastax.oss.driver.api.core.cql.ColumnDefinitions;
import com.datastax.oss.driver.api.core.cql.ResultSet;
import com.datastax.oss.driver.api.core.cql.Row;
import com.datastax.oss.driver.api.core.CqlSession;
import com.datastax.oss.driver.api.core.DefaultConsistencyLevel;
import com.datastax.oss.driver.api.core.session.Session;
import com.datastax.oss.driver.api.querybuilder.insert.Insert;
import com.datastax.oss.driver.api.querybuilder.insert.InsertInto;
import com.datastax.oss.driver.api.querybuilder.QueryBuilder;
import com.datastax.oss.driver.api.querybuilder.term.Term;
import com.datastax.oss.protocol.internal.ProtocolConstants;


public class DriverCQLRowHandler implements CQLRowHandler {

  private final CqlSession session;
  DriverKeyspace keyspace;
  ResultSet result;

  ColumnDefinitions columns;

  private int batchInsertTimeout;
  private int ttlSec;

  private boolean unloggedBatch = true;

  private boolean expandCollection = true;
  private int primaryCollectionOutputIndex = -1;

  public DriverCQLRowHandler( DriverKeyspace keyspace, CqlSession session, boolean expandCollection ) {
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
        if ( CassandraUtils.isCollection( columns.get( i ).getType() ) ) {
          if ( primaryCollectionOutputIndex < 0 ) {
            primaryCollectionOutputIndex = i;
          } else if ( !keyspace.getTableMetaData( tableName ).getValueMetaForColumn( columns.get( i ).getName().toString() )
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
    Row row = result != null ? result.one() : null;
    if ( row == null ) {
      result = null;
      columns = null;
      return null;
    }
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
        switch ( cdef.get( i ).getType().getProtocolCode() ) {
          case ProtocolConstants.DataType.BIGINT:
          case ProtocolConstants.DataType.COUNTER:
            return row.getLong( i );
          case ProtocolConstants.DataType.SMALLINT:
            return (long) row.getShort( i );
          case ProtocolConstants.DataType.TINYINT:
            return (long) row.getByte( i );
          case ProtocolConstants.DataType.INT:
            return (long) row.getInt( i );
          case ProtocolConstants.DataType.FLOAT:
            return (double) row.getFloat( i );
          case ProtocolConstants.DataType.DOUBLE:
            return row.getDouble( i );
          case ProtocolConstants.DataType.DECIMAL:
            return row.getBigDecimal( i );
          case ProtocolConstants.DataType.VARINT:
            return new BigDecimal( row.getBigInteger( i ) );
          case ProtocolConstants.DataType.BOOLEAN:
            return row.getBool( i );
          case ProtocolConstants.DataType.DATE:
            return new Date( row.getInstant( i ).toEpochMilli() );
          case ProtocolConstants.DataType.TIMESTAMP:
            return new Date( row.getInstant( i ).toEpochMilli() );
          case ProtocolConstants.DataType.TIME:
            return row.getLocalTime( i ).toNanoOfDay();
          case ProtocolConstants.DataType.BLOB:
            return row.getByteBuffer( i ).array();
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
    BatchType type = unloggedBatch ? BatchType.UNLOGGED : BatchType.LOGGED;
    BatchStatementBuilder batch = new BatchStatementBuilder( type );
    if ( !Utils.isEmpty( consistencyLevel ) ) {
      try {
        batch.setConsistencyLevel( DefaultConsistencyLevel.valueOf( consistencyLevel ) );
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
      Map<String, Term> toInsert = new HashMap<>();
      for ( int i = 0; i< columnNames.length; i++ ) {
        toInsert.put( CassandraUtils.quoteIdentifier( columnNames[1] ), QueryBuilder.literal( values[i] ) );
      }
      InsertInto insertInto = QueryBuilder.insertInto( CassandraUtils.quoteIdentifier( keyspace.getName() ),
                                                       CassandraUtils.quoteIdentifier( tableMeta.getTableName() ) );
      Insert insert = insertInto.values( toInsert );
      if ( ttlSec > 0 ) {
        insert.usingTtl( ttlSec );
      }
      batch.addStatement( insert.build() );
    }
    if ( batchInsertTimeout > 0 ) {
      try {
        getSession().executeAsync( batch.build() ).toCompletableFuture().get( batchInsertTimeout, TimeUnit.MILLISECONDS );
      } catch ( TimeoutException e ) {
        log.logError( BaseMessages.getString( DriverCQLRowHandler.class, "DriverCQLRowHandler.Error.TimeoutReached" ) );
      }
    } else {
      getSession().execute( batch.build() );
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

  private CqlSession getSession() {
    return session;
  }

  public void setTtlSec( int ttl ) {
    ttlSec = ttl;
  }

}
