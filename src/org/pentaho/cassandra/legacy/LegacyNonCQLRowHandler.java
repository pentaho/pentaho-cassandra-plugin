/*******************************************************************************
 *
 * Pentaho Big Data
 *
 * Copyright (C) 2002-2013 by Pentaho : http://www.pentaho.com
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

import java.nio.ByteBuffer;
import java.util.ArrayList;
import java.util.Date;
import java.util.HashMap;
import java.util.List;
import java.util.Map;
import java.util.concurrent.atomic.AtomicBoolean;

import org.apache.cassandra.thrift.Column;
import org.apache.cassandra.thrift.ColumnOrSuperColumn;
import org.apache.cassandra.thrift.ColumnParent;
import org.apache.cassandra.thrift.ConsistencyLevel;
import org.apache.cassandra.thrift.KeyRange;
import org.apache.cassandra.thrift.KeySlice;
import org.apache.cassandra.thrift.Mutation;
import org.apache.cassandra.thrift.SlicePredicate;
import org.apache.cassandra.thrift.SliceRange;
import org.apache.cassandra.thrift.TimedOutException;
import org.pentaho.cassandra.CassandraUtils;
import org.pentaho.cassandra.spi.Keyspace;
import org.pentaho.cassandra.spi.NonCQLRowHandler;
import org.pentaho.di.core.Const;
import org.pentaho.di.core.logging.LogChannelInterface;
import org.pentaho.di.core.row.RowDataUtil;
import org.pentaho.di.core.row.RowMetaInterface;
import org.pentaho.di.core.row.ValueMeta;
import org.pentaho.di.core.row.ValueMetaInterface;
import org.pentaho.di.i18n.BaseMessages;
import org.pentaho.di.trans.step.StepInterface;

/**
 * Implementation of NonCQLRowHandler that wraps the legacy Thrift-based implementation
 * 
 * @author Mark Hall (mhall{[at]}pentaho{[dot]}com)
 */
public class LegacyNonCQLRowHandler implements NonCQLRowHandler {

  protected static final Class<?> PKG = LegacyNonCQLRowHandler.class;

  protected LegacyKeyspace m_keyspace;
  protected Map<String, String> m_options;

  /** meta data for the current column family */
  protected CassandraColumnMetaData m_metaData;

  protected StepInterface m_requestingStep;

  protected int m_timeout;

  protected boolean m_newSliceQuery = false;
  protected List<String> m_requestedCols = null;
  protected int m_sliceRowsMax;
  protected int m_sliceColsMax;
  protected int m_sliceRowsBatchSize;
  protected int m_sliceColsBatchSize;
  protected SliceRange m_sliceRange;
  protected KeyRange m_keyRange;
  protected SlicePredicate m_slicePredicate;
  protected ColumnParent m_colParent;
  protected int m_rowIndex;
  protected int m_colIndex;
  protected ConsistencyLevel m_consistencyLevel;

  // current batch of rows
  protected List<KeySlice> m_cassandraRows;

  // current batch of columns from current row
  protected List<ColumnOrSuperColumn> m_currentCols;
  protected int m_colCount;
  protected int m_rowCount;

  protected int m_currentBatchCounter = -1;

  protected int m_ttl = -1;

  @Override
  public void setOptions( Map<String, String> options ) {
    m_options = options;

    if ( m_options != null ) {
      for ( Map.Entry<String, String> e : m_options.entrySet() ) {
        if ( e.getKey().equalsIgnoreCase( CassandraUtils.BatchOptions.BATCH_TIMEOUT ) ) {
          try {
            m_timeout = Integer.parseInt( e.getValue() );
          } catch ( NumberFormatException ex ) {
            // don't complain
          }
        } else if ( e.getKey().equalsIgnoreCase( "ttl" ) ) {
          try {
            m_ttl = Integer.parseInt( e.getValue() );
          } catch ( NumberFormatException ex ) {
            // don't complain
          }
        }
      }
    }
  }

  @Override
  public void setKeyspace( Keyspace keyspace ) {
    m_keyspace = (LegacyKeyspace) keyspace;
  }

  @Override
  public void newRowQuery( StepInterface requestingStep, String colFamilyName, List<String> colNames, int rowLimit,
      int colLimit, int rowBatchSize, int colBatchSize, String consistencyLevel, LogChannelInterface log )
    throws Exception {

    if ( m_keyspace == null ) {
      throw new Exception( BaseMessages.getString( PKG, "LegacyNonCQLRowHandler.Error.NoKeyspaceSpecified" ) ); //$NON-NLS-1$
    }

    m_metaData = (CassandraColumnMetaData) m_keyspace.getColumnFamilyMetaData( colFamilyName );

    m_requestingStep = requestingStep;

    m_newSliceQuery = true;
    m_requestedCols = colNames;
    m_sliceRowsMax = rowLimit;
    m_sliceColsMax = colLimit;
    m_sliceRowsBatchSize = rowBatchSize;
    m_sliceColsBatchSize = colBatchSize;
    m_rowIndex = 0;
    m_colIndex = 0;
    m_currentBatchCounter = -1;

    if ( !Const.isEmpty( consistencyLevel ) ) {
      m_consistencyLevel = ConsistencyLevel.valueOf( consistencyLevel );
    } else {
      m_consistencyLevel = ConsistencyLevel.ONE;
    }

    if ( m_sliceColsBatchSize <= 0 ) {
      m_sliceColsBatchSize = Integer.MAX_VALUE;
    }

    if ( m_sliceRowsBatchSize <= 0 ) {
      m_sliceRowsBatchSize = Integer.MAX_VALUE;
    }

    List<ByteBuffer> specificCols = null;
    if ( m_requestedCols != null && m_requestedCols.size() > 0 ) {
      specificCols = new ArrayList<ByteBuffer>();

      // encode the textual column names
      for ( String colName : m_requestedCols ) {
        ByteBuffer encoded = m_metaData.columnNameToByteBuffer( colName );
        specificCols.add( encoded );
      }
    }

    m_slicePredicate = new SlicePredicate();

    if ( specificCols == null ) {
      m_sliceRange =
          new SliceRange( ByteBuffer.wrap( new byte[0] ), ByteBuffer.wrap( new byte[0] ), false, m_sliceColsBatchSize );
      m_slicePredicate.setSlice_range( m_sliceRange );
    } else {
      m_slicePredicate.setColumn_names( specificCols );
    }

    m_keyRange = new KeyRange( m_sliceRowsBatchSize );
    m_keyRange.setStart_key( new byte[0] );
    m_keyRange.setEnd_key( new byte[0] );

    m_colParent = new ColumnParent( colFamilyName );
  }

  private void advanceToNonEmptyRow() {
    KeySlice row = m_cassandraRows.get( m_rowIndex );
    m_currentCols = row.getColumns();

    int skipSize = 0;
    while ( m_currentCols.size() == skipSize && m_rowIndex < m_cassandraRows.size() - 1 ) {
      m_rowIndex++;
      row = m_cassandraRows.get( m_rowIndex );
      m_currentCols = row.getColumns();
    }

    if ( m_currentCols.size() == skipSize ) {
      // we've been through the batch and there are no columns in any of these
      // rows -
      // so nothing to output! Indicate this by setting currentCols to null
      m_currentCols = null;
    }
  }

  private void getNextBatchOfRows() throws Exception {

    if ( m_requestingStep.isStopped() || m_requestingStep.isPaused() ) {
      return; // don't do anything if we're paused or stopped
    }

    // reset the column range (if necessary)
    if ( m_requestedCols == null ) {
      m_sliceRange = m_sliceRange.setStart( ByteBuffer.wrap( new byte[0] ) );
      m_sliceRange = m_sliceRange.setFinish( ByteBuffer.wrap( new byte[0] ) );

      m_slicePredicate.setSlice_range( m_sliceRange );
    }

    // set the key range start to the last key from the last batch of rows
    m_keyRange.setStart_key( m_cassandraRows.get( m_cassandraRows.size() - 1 ).getKey() );
    m_cassandraRows =
        ( (CassandraConnection) m_keyspace.getConnection() ).getClient().get_range_slices( m_colParent,
            m_slicePredicate, m_keyRange, m_consistencyLevel );

    m_colCount = 0;

    // key ranges are *inclusive* of the start key - we will have already
    // processed the first
    // row in the last batch. Hence start at index 1 of this batch
    m_rowIndex = 1;
    if ( m_cassandraRows == null || m_cassandraRows.size() <= 1 || m_rowCount == m_sliceRowsMax ) {
      // indicate done
      m_currentCols = null;
      m_cassandraRows = null;
    } else {
      advanceToNonEmptyRow();
    }
  }

  private void getNextBatchOfColumns() throws Exception {

    if ( m_requestingStep.isStopped() || m_requestingStep.isPaused() ) {
      return; // don't do anything if we're paused or stopped
    }

    m_sliceRange = m_sliceRange.setStart( m_currentCols.get( m_currentCols.size() - 1 ).getColumn().bufferForName() );
    m_slicePredicate.setSlice_range( m_sliceRange );

    // fetch the next bunch of columns for the current row
    m_currentCols =
        ( (CassandraConnection) m_keyspace.getConnection() ).getClient().get_slice(
            m_cassandraRows.get( m_rowIndex ).bufferForKey(), m_colParent, m_slicePredicate, ConsistencyLevel.ONE );

    // as far as I understand it - these things are always inclusive of the
    // start element,
    // so we need to skip the first element cause it was processed already in
    // the last batch
    // of columns
    if ( m_currentCols == null || m_currentCols.size() <= 1 ) {
      // no more columns in the current row - move to the next row
      m_rowCount++;
      m_rowIndex++;
      m_colCount = 0;

      if ( m_rowIndex == m_cassandraRows.size() ) {
        getNextBatchOfRows();

        while ( m_cassandraRows != null && m_currentCols == null ) {
          // keep going until we get some rows with columns!
          getNextBatchOfRows();
        }
      } else {
        advanceToNonEmptyRow();

        while ( m_cassandraRows != null && m_currentCols == null ) {
          // keep going until we get some rows with columns!
          getNextBatchOfRows();
        }
      }
    } else {
      // we need to discard the first col in the list since we will have
      // processed
      // that already in the batch
      m_currentCols.remove( 0 );
    }
  }

  private boolean getMoreData() throws Exception {
    // get more data to process

    m_currentRowKeyValue = null;

    int timeouts = 0;

    while ( timeouts < 5 ) {
      try {
        if ( m_newSliceQuery ) {
          m_cassandraRows =
              ( (CassandraConnection) m_keyspace.getConnection() ).getClient().get_range_slices( m_colParent,
                  m_slicePredicate, m_keyRange, ConsistencyLevel.ONE );
          if ( m_cassandraRows == null || m_cassandraRows.size() == 0 ) {
            // done
            return false;
          } else {
            advanceToNonEmptyRow();
            while ( m_cassandraRows != null && m_currentCols == null ) {
              // keep going until we get some rows with columns!
              getNextBatchOfRows();
            }

            if ( m_cassandraRows == null ) {
              // we're done
              return false;
            }

            m_colCount = 0;
            m_rowCount = 0;
            m_newSliceQuery = false;
          }
        } else {
          // determine what we need to get next - more columns from current
          // row, or start next row
          // or get next row batch or done
          if ( m_cassandraRows == null ) {
            // we're done
            return false;
          }

          if ( m_rowCount == m_sliceRowsMax ) {
            // hit our LIMIT of rows - done
            return false;
          }

          if ( m_rowIndex == m_cassandraRows.size() ) {
            // get next batch of rows
            getNextBatchOfRows();
            while ( m_cassandraRows != null && m_currentCols == null ) {
              // keep going until we get some rows with columns!
              getNextBatchOfRows();
            }

            if ( m_cassandraRows == null ) {
              // we're done
              return false;
            }
          } else if ( m_colCount == -1 ) {
            // get next row
            KeySlice row = m_cassandraRows.get( m_rowIndex );
            m_currentCols = row.getColumns();

            m_colCount = 0;
          } else {
            getNextBatchOfColumns();

            // check against our limit again
            if ( m_rowCount == m_sliceRowsMax ) {
              return false;
            }

            if ( m_cassandraRows == null ) {
              // we're done
              return false;
            }
          }
        }

        break;
      } catch ( TimedOutException e ) {
        timeouts++;
      }
    }

    if ( timeouts == 5 ) {
      throw new Exception( BaseMessages.getString( PKG,
          "LegacyNonCQLRowHandler.Error.MaximumNumberOfConsecutiveTimeoutsExceeded" ) ); //$NON-NLS-1$
    }

    m_currentBatchCounter = 0;
    KeySlice row = m_cassandraRows.get( m_rowIndex );
    m_currentRowKeyValue = m_metaData.getKeyValue( row );
    if ( m_currentRowKeyValue == null ) {
      throw new Exception( BaseMessages.getString( PKG, "LegacyNonCQLRowHandler.Error.UnableToObtainAKeyValueForRow" ) ); //$NON-NLS-1$
    }

    return true;
  }

  private boolean skipNullColumns() throws Exception {

    Column col = m_currentCols.get( m_currentBatchCounter++ ).getColumn();
    Object colValue = m_metaData.getColumnValue( col );

    while ( colValue == null && m_currentBatchCounter < m_currentCols.size() ) {
      // skip null columns (only applies if we're processing
      // a specified list of columns rather than all columns
      col = m_currentCols.get( m_currentBatchCounter++ ).getColumn();
      colValue = m_metaData.getColumnValue( col );
    }

    if ( colValue == null ) {
      // try getting more data
      if ( !getMoreData() ) {
        return false;
      }

      return skipNullColumns();
    }

    // we've advanced beyond the non-null column
    m_currentBatchCounter--;

    return true;
  }

  protected Object m_currentRowKeyValue = null;

  @Override
  public Object[] getNextOutputRow( RowMetaInterface outputRowMeta ) throws Exception {

    if ( m_currentBatchCounter < 0 ) {
      if ( !getMoreData() ) {
        // we're done
        return null;
      }
    }

    // String keyName = m_metaData.getKeyName();
    String keyName = "KEY"; // always use this name for tuple mode //$NON-NLS-1$
    int keyIndex = outputRowMeta.indexOfValue( keyName );
    if ( keyIndex < 0 ) {
      throw new Exception( BaseMessages.getString( PKG,
          "LegacyNonCQLRowHandler.Error.UnableToFindKeyFieldName", keyName ) ); //$NON-NLS-1$
    }

    Object[] outputRowData = RowDataUtil.allocateRowData( outputRowMeta.size() );

    if ( !skipNullColumns() ) {
      // we're done
      return null;
    }

    Column col = m_currentCols.get( m_currentBatchCounter++ ).getColumn();
    String colName = m_metaData.getColumnName( col );
    Object colValue = m_metaData.getColumnValue( col );

    outputRowData[keyIndex] = m_currentRowKeyValue;
    outputRowData[1] = colName;
    String stringV = colValue.toString();
    outputRowData[2] = stringV;

    if ( colValue instanceof Date ) {
      ValueMeta tempDateMeta = new ValueMeta( "temp", //$NON-NLS-1$
          ValueMetaInterface.TYPE_DATE );
      stringV = tempDateMeta.getString( colValue );
      outputRowData[2] = stringV;
    } else if ( colValue instanceof byte[] ) {
      outputRowData[2] = colValue;
    }
    // the timestamp as a date object
    long timestampL = col.getTimestamp();
    outputRowData[3] = timestampL;

    m_colCount++;
    if ( m_colCount == m_sliceColsMax && m_requestedCols == null ) {
      // max number of cols reached for this row
      m_colCount = -1; // indicate move to the next row
      m_currentBatchCounter = -1; // force a call to getMoreData() on the next
                                  // call to us

      m_rowCount++;
      m_rowIndex++;
      // break; // don't process any more
    }

    if ( m_requestedCols != null && m_currentBatchCounter == m_currentCols.size() ) {
      // assume that we don't need to page columns when the user has
      // explicitly named the ones that they want
      m_colCount = -1;
      m_rowCount++;
      m_rowIndex++;
    }

    if ( m_currentBatchCounter == m_currentCols.size() ) {
      m_currentBatchCounter = -1; // getMoreData() next time
    }

    return outputRowData;
  }

  private static Map<ByteBuffer, Map<String, List<Mutation>>> createThriftBatch( List<Object[]> rowBatch ) {
    Map<ByteBuffer, Map<String, List<Mutation>>> thriftBatch =
        new HashMap<ByteBuffer, Map<String, List<Mutation>>>( rowBatch.size() );

    return thriftBatch;
  }

  /**
   * Commit a batch.
   * 
   * @param requestingStep
   *          the step that is requesting the rows - clients can use this primarily to check whether the running
   *          transformation has been paused or stopped (via isPaused() and isStopped())
   * @param batch
   *          the batch to commit
   * @param rowMeta
   *          the structure of the rows being written
   * @param keyIndex
   *          the index of the key in the rows
   * @param colFamName
   *          the name of the column family being written to
   * @param consistencyLevel
   *          the consistency level to use
   * @param log
   *          the log to use
   * @throws Exception
   *           if a problem occurs
   */
  @Override
  @SuppressWarnings( "deprecation" )
  public void commitNonCQLBatch( StepInterface requestingStep, List<Object[]> batch, RowMetaInterface rowMeta,
      int keyIndex, String colFamName, String consistencyLevel, LogChannelInterface log ) throws Exception {

    m_requestingStep = requestingStep;

    // Convert the row list over to a Map of mutations

    CassandraColumnMetaData famMeta = (CassandraColumnMetaData) m_keyspace.getColumnFamilyMetaData( colFamName );

    ValueMetaInterface keyMeta = rowMeta.getValueMeta( keyIndex );
    Map<ByteBuffer, Map<String, List<Mutation>>> thriftBatch = createThriftBatch( batch );

    for ( Object[] row : batch ) {
      ByteBuffer keyBuff = famMeta.kettleValueToByteBuffer( keyMeta, row[keyIndex], true );

      Map<String, List<Mutation>> mapCF = thriftBatch.get( keyBuff );
      List<Mutation> mutList = null;

      // check to see if we have already got some mutations for this key in
      // the batch
      if ( mapCF != null ) {
        mutList = mapCF.get( colFamName );
      } else {
        mapCF = new HashMap<String, List<Mutation>>( 1 );
        mutList = new ArrayList<Mutation>();
      }

      for ( int i = 0; i < rowMeta.size(); i++ ) {
        if ( i != keyIndex ) {
          ValueMetaInterface colMeta = rowMeta.getValueMeta( i );
          String colName = colMeta.getName();

          // don't insert if null!
          if ( colMeta.isNull( row[i] ) ) {
            continue;
          }

          Column col = new Column( famMeta.columnNameToByteBuffer( colName ) );
          col = col.setValue( famMeta.kettleValueToByteBuffer( colMeta, row[i], false ) );
          col = col.setTimestamp( System.currentTimeMillis() );
          if ( m_ttl > 0 ) {
            col.setTtl( m_ttl );
          }
          ColumnOrSuperColumn cosc = new ColumnOrSuperColumn();
          cosc.setColumn( col );
          Mutation mut = new Mutation();
          mut.setColumn_or_supercolumn( cosc );
          mutList.add( mut );
        }
      }

      // column family name -> mutations
      mapCF.put( colFamName, mutList );

      // row key -> column family - > mutations
      thriftBatch.put( keyBuff, mapCF );
    }

    ConsistencyLevel levelToUse = ConsistencyLevel.ANY;
    if ( !Const.isEmpty( consistencyLevel ) ) {
      try {
        levelToUse = ConsistencyLevel.valueOf( consistencyLevel );
      } catch ( IllegalArgumentException ex ) {
        if ( log != null && log.isDebug() ) {
          log.logDebug( BaseMessages.getString( PKG, "LegacyCQLRowHandler.Error.NoValidConsistencyLevelSpecified",
              consistencyLevel, levelToUse.toString() ) );
        }
      }
    }

    final ConsistencyLevel fLevelToUse = levelToUse;
    final Map<ByteBuffer, Map<String, List<Mutation>>> fThriftBatch = thriftBatch;

    // do commit in separate thread to be able to monitor timeout
    long start = System.currentTimeMillis();
    long time = System.currentTimeMillis() - start;
    final Exception[] e = new Exception[1];
    final AtomicBoolean done = new AtomicBoolean( false );
    Thread t = new Thread( new Runnable() {
      @Override
      public void run() {
        try {
          ( (CassandraConnection) m_keyspace.getConnection() ).getClient().batch_mutate( fThriftBatch, fLevelToUse );
        } catch ( Exception ex ) {
          e[0] = ex;
        } finally {
          done.set( true );
        }
      }
    } );

    // don't commit if we've been stopped, but do if we've
    // been paused (otherwise we'll lose this batch)
    if ( !m_requestingStep.isStopped() ) {
      t.start();

      // wait for it to complete
      while ( !done.get() ) {
        time = System.currentTimeMillis() - start;
        if ( m_timeout > 0 && time > m_timeout ) {
          try {
            // try to kill it!
            t.stop();
          } catch ( Exception ex ) {
          }

          throw new Exception( BaseMessages.getString( PKG, "LegacyNonCQLRowHandler.Error.TimeoutReached" ) ); //$NON-NLS-1$
        }
        // wait
        Thread.sleep( 100 );
      }
    }
    // was there a problem?
    if ( e[0] != null ) {
      throw e[0];
    }
  }
}
