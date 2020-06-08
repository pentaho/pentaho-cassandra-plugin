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

package org.pentaho.cassandra.driver.datastax;

import com.datastax.oss.driver.api.core.ConsistencyLevel;
import com.datastax.oss.driver.api.core.CqlSession;
import com.datastax.oss.driver.api.core.cql.BatchStatementBuilder;
import com.datastax.oss.driver.api.core.cql.BatchType;
import com.datastax.oss.driver.api.core.cql.ColumnDefinitions;
import com.datastax.oss.driver.api.core.cql.ResultSet;
import com.datastax.oss.driver.api.core.cql.Row;
import com.datastax.oss.driver.api.core.session.Session;
import com.datastax.oss.driver.api.core.type.DataType;
import com.datastax.oss.driver.api.core.type.DataTypes;
import com.datastax.oss.driver.api.querybuilder.QueryBuilder;
import com.datastax.oss.driver.api.querybuilder.insert.RegularInsert;
import org.apache.commons.lang.NotImplementedException;
import org.pentaho.cassandra.spi.CQLRowHandler;
import org.pentaho.cassandra.spi.ITableMetaData;
import org.pentaho.cassandra.spi.Keyspace;
import org.pentaho.cassandra.util.CassandraUtils;
import org.pentaho.di.core.logging.LogChannelInterface;
import org.pentaho.di.core.row.RowDataUtil;
import org.pentaho.di.core.row.RowMetaInterface;
import org.pentaho.di.core.row.ValueMetaInterface;
import org.pentaho.di.core.util.Utils;
import org.pentaho.di.trans.step.StepInterface;

import java.math.BigDecimal;
import java.util.ArrayList;
import java.util.Arrays;
import java.util.Collection;
import java.util.Date;
import java.util.List;
import java.util.Map;
import java.util.Objects;
import java.util.stream.Collectors;
import java.util.stream.IntStream;

public class DriverCQLRowHandler implements CQLRowHandler {

  private final CqlSession session;
  DriverKeyspace keyspace;
  ResultSet result;

  ColumnDefinitions columns;

  private int batchInsertTimeout;
  private int ttlSec;

  private boolean unloggedBatch = true;

  private boolean expandCollection = true;
  private boolean notExpandingMaps = false;
  private int primaryCollectionOutputIndex = -1;

  public DriverCQLRowHandler( DriverKeyspace keyspace, Session session, boolean expandCollection, boolean notExpandingMaps ) {
    this.keyspace = keyspace;
    this.session = (CqlSession) session;
    this.expandCollection = expandCollection;
    this.notExpandingMaps = notExpandingMaps;
  }

  public DriverCQLRowHandler( DriverKeyspace keyspace ) throws Exception {
    this( keyspace, keyspace.getConnection().getSession( keyspace.getName() ), true, false );
  }

  public boolean supportsCQLVersion( int cqMajorlVersion ) {
    return cqMajorlVersion == 3;
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
      String consistencyLevel, LogChannelInterface log )
    throws Exception {
    result = getSession().execute( cqlQuery );
    columns = result.getColumnDefinitions();
    if ( expandCollection ) {
      for ( int i = 0; i < columns.size(); i++ ) {
        if ( CassandraUtils.isCollection( columns.get( i ).getType(), notExpandingMaps ) ) {
          if ( primaryCollectionOutputIndex < 0 ) {
            primaryCollectionOutputIndex = i;
          }
        }
        // TODO: Investigate why Text type is okay, but everything else is not
        /*
         * else if ( !keyspace.getTableMetaData( tableName ).getValueMetaForColumn( columns.getName( i ) ) .isString() )
         * { throw new KettleException( BaseMessages.getString( DriverCQLRowHandler.class,
         * "DriverCQLRowHandler.Error.CantHandleAdditionalCollectionsThatAreNotOfTypeText" ) ); } }
         */
      }
    }
  }

  @Override
  public Object[][] getNextOutputRow( RowMetaInterface outputRowMeta, Map<String, Integer> outputFormatMap )
    throws Exception {

    Row row = null;
    if ( result == null || ( row = result.one() ) == null ) { // removed isExhaused
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
    if ( primaryCollectionOutputIndex >= 0 ) {
      Object collectionObject = row.getObject( primaryCollectionOutputIndex );
      if ( collectionObject != null
        && ( collectionObject instanceof Collection<?> || ( !notExpandingMaps && collectionObject instanceof Map<?, ?> ) ) ) {

        Collection<?> collection =
            ( collectionObject instanceof Map<?, ?> ) ? ( (Map<?, ?>) collectionObject ).keySet()
                : (Collection<?>) collectionObject;

        if ( !collection.isEmpty() ) {

          outputRowData = new Object[collection.size()][];
          int i = 0;
          for ( Object obj : collection ) {
            outputRowData[i] = Arrays.copyOf( baseOutputRowData, baseOutputRowData.length );
            outputRowData[i++][primaryCollectionOutputIndex] = obj;
          }
        } else {
          outputRowData[0][primaryCollectionOutputIndex] = null;
        }
      } else {
        outputRowData[0][primaryCollectionOutputIndex] = null;
      }
    }

    return outputRowData;
  }

  public Object readValue( ValueMetaInterface meta, Row row, int i ) {

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

        DataType dt = columns.get( i ).getType();

        if ( dt.equals( DataTypes.BIGINT ) || dt.equals( DataTypes.COUNTER ) ) {
          return row.getLong( i );
        } else if ( dt.equals( DataTypes.SMALLINT ) ) {
          return (long) row.getShort( i );
        } else if ( dt.equals( DataTypes.TINYINT ) ) {
          return (long) row.getByte( i );
        } else if ( dt.equals( DataTypes.INT ) ) {
          return (long) row.getInt( i );
        } else if ( dt.equals( DataTypes.FLOAT ) ) {
          return (double) row.getFloat( i );
        } else if ( dt.equals( DataTypes.DOUBLE ) ) {
          return row.getDouble( i );
        } else if ( dt.equals( DataTypes.DECIMAL ) ) {
          return row.getBigDecimal( i );
        } else if ( dt.equals( DataTypes.VARINT ) ) {
          return new BigDecimal( row.getBigInteger( i ) );
        } else if ( dt.equals( DataTypes.BOOLEAN ) ) {
          return row.getBoolean( i );
        } else if ( dt.equals( DataTypes.DATE ) ) {
          return java.sql.Date.valueOf( row.getLocalDate( i ) );
        } else if ( dt.equals( DataTypes.TIMESTAMP ) ) {
          return Date.from( row.getInstant( i ) );
        } else if ( dt.equals( DataTypes.BLOB ) ) {
          return row.getByteBuffer( i ).array();
        } else {
          return Objects.toString( row.getObject( i ), null );
        }

      default:
        return Objects.toString( row.getObject( i ), null );
    }
  }

  public void batchInsert( RowMetaInterface inputMeta, Iterable<Object[]> rows, ITableMetaData tableMeta,
      String consistencyLevel, boolean insertFieldsNotInMetadata, LogChannelInterface log )
    throws Exception {

    BatchStatementBuilder batchBuilder =
        new BatchStatementBuilder( unloggedBatch ? BatchType.UNLOGGED : BatchType.LOGGED );

    String[] columnNames = getColumnNames( inputMeta );
    if ( !Utils.isEmpty( consistencyLevel ) ) {
      try {
        batchBuilder.setConsistencyLevel( ConsistencyLevelEnum.valueOf( consistencyLevel ).getConsistencyLevel() );
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

    final String[] finalColumnNames = columnNames;

    for ( Object[] row : rows ) {
      Object[] values =
          toRemove.size() == 0
              ? Arrays.copyOf( row, columnNames.length )
              : copyExcluding( row, new Object[columnNames.length], toRemove );

      RegularInsert insert =
          QueryBuilder.insertInto( keyspace.getName(), tableMeta.getTableName() )
              .values(
                IntStream.range( 0, values.length ).boxed()
                    .collect( Collectors.toMap( i -> finalColumnNames[i], i -> QueryBuilder.literal( values[i] ) ) ) );

      if ( ttlSec > 0 ) {
        insert.usingTtl( ttlSec );
      }

      batchBuilder.addStatement( insert.build() );
    }

    getSession().execute( batchBuilder.build() );

    // TODO: Re-enable batch insert timeout
    /*
     * if ( batchInsertTimeout > 0 ) { try { //getSession().executeAsync( batchBuilder.build() ).get
     * //.getUninterruptibly( batchInsertTimeout, TimeUnit.MILLISECONDS ); } catch ( TimeoutException e ) {
     * log.logError( BaseMessages.getString( DriverCQLRowHandler.class, "DriverCQLRowHandler.Error.TimeoutReached" ) );
     * } } else {
     * 
     * }
     */

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
      String consistencyLevel, LogChannelInterface log )
    throws Exception {
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

  public static enum ConsistencyLevelEnum {

    ALL( ConsistencyLevel.ALL ),
    ANY( ConsistencyLevel.ANY ),
    ONE( ConsistencyLevel.ONE ),
    TWO( ConsistencyLevel.TWO ),
    THREE( ConsistencyLevel.THREE ),
    QUORUM( ConsistencyLevel.QUORUM ),
    SERIAL( ConsistencyLevel.SERIAL ),
    LOCAL_ONE( ConsistencyLevel.LOCAL_ONE ),
    LOCAL_QUORUM( ConsistencyLevel.LOCAL_QUORUM ),
    LOCAL_SERIAL( ConsistencyLevel.LOCAL_SERIAL ),
    EACH_QUORUM( ConsistencyLevel.EACH_QUORUM );

    private final ConsistencyLevel cl;

    private ConsistencyLevelEnum( ConsistencyLevel cl ) {
      this.cl = cl;
    }

    public ConsistencyLevel getConsistencyLevel() {
      return cl;
    }

  }

}
