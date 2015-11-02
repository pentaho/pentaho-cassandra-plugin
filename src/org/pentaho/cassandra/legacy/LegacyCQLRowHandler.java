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
import java.util.Arrays;
import java.util.Collection;
import java.util.Date;
import java.util.Iterator;
import java.util.List;
import java.util.Map;
import java.util.concurrent.atomic.AtomicBoolean;

import org.apache.cassandra.thrift.Column;
import org.apache.cassandra.thrift.Compression;
import org.apache.cassandra.thrift.ConsistencyLevel;
import org.apache.cassandra.thrift.CqlResult;
import org.apache.cassandra.thrift.CqlRow;
import org.pentaho.cassandra.CassandraUtils;
import static org.pentaho.cassandra.cql.CQLUtils.getColumnsInSelect;
import static org.pentaho.cassandra.cql.CQLUtils.getSelectExpression;
import org.pentaho.cassandra.cql.Selector;
import org.pentaho.cassandra.spi.CQLRowHandler;
import org.pentaho.cassandra.spi.Keyspace;
import org.pentaho.di.core.Const;
import org.pentaho.di.core.exception.KettleException;
import org.pentaho.di.core.logging.LogChannelInterface;
import org.pentaho.di.core.row.RowDataUtil;
import org.pentaho.di.core.row.RowMetaInterface;
import org.pentaho.di.core.row.ValueMeta;
import org.pentaho.di.core.row.ValueMetaInterface;
import org.pentaho.di.i18n.BaseMessages;
import org.pentaho.di.trans.step.StepInterface;

/**
 * Implementation of CQLRowHandler that wraps the legacy Thrift-based implementation
 * 
 * @author Mark Hall (mhall{[at]}pentaho{[dot]}com)
 */
public class LegacyCQLRowHandler implements CQLRowHandler {

  protected static final Class<?> PKG = LegacyCQLRowHandler.class;

  protected LegacyKeyspace m_keyspace;
  protected Map<String, String> m_options;

  /** meta data for the current column family */
  protected CassandraColumnMetaData m_metaData;

  protected Iterator<CqlRow> m_resultIterator;

  protected Iterator<Column> m_cassandraColIter;
  protected CqlRow m_currentTupleRow;

  protected Object m_currentRowKey = null;

  protected boolean m_cql3;
  protected int m_timeout;

  protected boolean m_isSelectStarQuery;

  protected boolean m_outputTuples;

  protected StepInterface m_requestingStep;

  private Selector[] selectorList;

  @Override
  public boolean supportsCQLVersion( int cqlMajVersion ) {
    return ( cqlMajVersion == 2 || cqlMajVersion == 3 );
  }

  @Override
  public void setOptions( Map<String, String> options ) {
    m_options = options;

    if ( m_options != null ) {
      for ( Map.Entry<String, String> e : m_options.entrySet() ) {
        if ( e.getKey().equalsIgnoreCase( CassandraUtils.CQLOptions.CQLVERSION_OPTION )
            && e.getValue().equals( CassandraUtils.CQLOptions.CQL3_STRING ) ) {
          m_cql3 = true;
        }

        if ( e.getKey().equalsIgnoreCase( CassandraUtils.BatchOptions.BATCH_TIMEOUT ) ) {
          try {
            m_timeout = Integer.parseInt( e.getValue() );
          } catch ( NumberFormatException ex ) {
          }
        }
      }
    }
  }

  @Override
  @SuppressWarnings( "deprecation" )
  public void commitCQLBatch( StepInterface requestingStep, StringBuilder batch, String compress,
      String consistencyLevel, LogChannelInterface log ) throws Exception {

    m_requestingStep = requestingStep;

    if ( !batch.toString().toLowerCase().endsWith( "apply batch" ) ) { //$NON-NLS-1$
      CassandraUtils.completeCQLBatch( batch );
    }

    ConsistencyLevel c = ConsistencyLevel.ONE; // default for CQL
    if ( !Const.isEmpty( consistencyLevel ) ) {
      try {
        c = ConsistencyLevel.valueOf( consistencyLevel );
      } catch ( IllegalArgumentException e ) {
        if ( log != null && log.isDebug() ) {
          log.logDebug( BaseMessages.getString( PKG, "LegacyCQLRowHandler.Error.NoValidConsistencyLevelSpecified",
              consistencyLevel, c.toString() ) );
        }
      }
    }

    Compression comp = Compression.NONE;
    if ( !Const.isEmpty( compress ) ) {
      comp = Compression.valueOf( compress );

      if ( comp == null ) {
        comp = Compression.NONE;
      }
    }

    final byte[] toSend = CassandraUtils.compressCQLQuery( batch.toString(), comp );

    if ( log != null ) {
      log.logDetailed( BaseMessages.getString( PKG, "LegacyCQLRowHandler.Message.UsingConsistencyLevel", c.toString() ) ); //$NON-NLS-1$
    }

    // do commit in separate thread to be able to monitor timeout
    long start = System.currentTimeMillis();
    long time = System.currentTimeMillis() - start;
    final Exception[] e = new Exception[1];
    final AtomicBoolean done = new AtomicBoolean( false );
    final Compression comp2 = comp;
    final ConsistencyLevel c2 = c;
    Thread t = new Thread( new Runnable() {
      @Override
      public void run() {
        try {
          if ( m_cql3 ) {
            ( (CassandraConnection) m_keyspace.getConnection() ).getClient().execute_cql3_query(
                ByteBuffer.wrap( toSend ), comp2, c2 );
          } else {
            ( (CassandraConnection) m_keyspace.getConnection() ).getClient().execute_cql_query(
                ByteBuffer.wrap( toSend ), comp2 );
          }
        } catch ( Exception ex ) {
          e[0] = ex;
        } finally {
          done.set( true );
        }
      }
    } );
    t.start();

    // wait for it to complete
    while ( !done.get() ) {
      time = System.currentTimeMillis() - start;
      if ( m_timeout > 0 && time > m_timeout ) {
        try {
          // try to kill it!
          t.stop();
        } catch ( Exception ex ) { /* YUM! */
        }

        throw new KettleException( BaseMessages.getString( PKG, "LegacyCQLRowHandler.Error.TimeoutReached" ) ); //$NON-NLS-1$
      }
      // wait
      Thread.sleep( 100 );
    }
    // was there a problem?
    if ( e[0] != null ) {
      throw e[0];
    }
  }

  /**
   * Add a row to a CQL batch. Clients can implement this if the static utility method by the same name in
   * CassandraUtils is not sufficient.
   * 
   * @param batch
   *          the batch to add to
   * @param colFamilyName
   *          the name of the column family that the batch insert applies to
   * @param inputMeta
   *          the structure of the incoming Kettle rows
   * @param keyIndex
   *          the index of the incoming field to use as the key for inserting
   * @param row
   *          the Kettle row
   * @param insertFieldsNotInMetaData
   *          true if any Kettle fields that are not in the Cassandra column family (table) meta data are to be
   *          inserted. This is irrelevant if the user has opted to have the step initially update the Cassandra meta
   *          data for incoming fields that are not known about.
   * @param log
   *          for logging
   * @return true if the row was added to the batch
   * @throws Exception
   *           if a problem occurs
   */
  @Override
  public boolean addRowToCQLBatch( StringBuilder batch, String colFamilyName, RowMetaInterface inputMeta, Object[] row,
      boolean insertFieldsNotInMetaData, LogChannelInterface log ) throws Exception {

    if ( m_metaData == null || !colFamilyName.equalsIgnoreCase( m_metaData.getColumnFamilyName() ) ) {
      m_metaData = (CassandraColumnMetaData) m_keyspace.getColumnFamilyMetaData( colFamilyName );
    }

    return CassandraUtils.addRowToCQLBatch( batch, colFamilyName, inputMeta, row, m_metaData,
        insertFieldsNotInMetaData, ( m_cql3 ? 3 : 2 ), m_options, log );
  }

  /**
   * Executes a new CQL query and initializes ready for iteration over the results. Closes/discards any previous query.
   * 
   * @param requestingStep
   *          the step that is requesting the rows - clients can use this primarily to check whether the running
   *          transformation has been paused or stopped (via isPaused() and isStopped())
   * @param colFamName
   *          the name of the column family to execute the query against
   * @param cqlQuery
   *          the CQL query to execute
   * @param compress
   *          the name of the compression to use (may be null for no compression)
   * @param consistencyLevel
   *          the consistency level to use
   * @param outputTuples
   *          true if the output rows should be key, value, timestamp tuples
   * @param log
   *          the log to use
   * @throws Exception
   *           if a problem occurs
   */
  @Override
  public void newRowQuery( StepInterface requestingStep, String colFamName, String cqlQuery, String compress,
      String consistencyLevel, boolean outputTuples, LogChannelInterface log ) throws Exception {

    if ( m_keyspace == null ) {
      throw new Exception( BaseMessages.getString( PKG, "LegacyCQLRowHandler.Error.NoKeyspaceSpecified" ) ); //$NON-NLS-1$
    }

    m_metaData = (CassandraColumnMetaData) m_keyspace.getColumnFamilyMetaData( colFamName );

    m_isSelectStarQuery = ( cqlQuery.toLowerCase().indexOf( "select *" ) >= 0 ); //$NON-NLS-1$
    if ( !m_isSelectStarQuery ) {
      selectorList = getColumnsInSelect( getSelectExpression( cqlQuery ), m_cql3 );
    }

    m_outputTuples = outputTuples;
    m_requestingStep = requestingStep;

    ConsistencyLevel c = ConsistencyLevel.ONE; // default for CQL
    Compression z = Compression.NONE;

    if ( !Const.isEmpty( consistencyLevel ) ) {
      try {
        c = ConsistencyLevel.valueOf( consistencyLevel );
      } catch ( IllegalArgumentException e ) {
        if ( log != null && log.isDebug() ) {
          log.logDebug( BaseMessages.getString( PKG, "LegacyCQLRowHandler.Error.NoValidConsistencyLevelSpecified",
              consistencyLevel, c.toString() ) );
        }
      }
    }
    if ( !Const.isEmpty( compress ) ) {
      if ( compress.equalsIgnoreCase( "gzip" ) ) { //$NON-NLS-1$
        z = Compression.GZIP;
      } else {
        z = Compression.NONE;
      }
    }

    byte[] queryBytes = CassandraUtils.compressCQLQuery( cqlQuery, z );
    CqlResult result = null;

    if ( m_cql3 ) {
      result =
          ( (CassandraConnection) m_keyspace.getConnection() ).getClient().execute_cql3_query(
              ByteBuffer.wrap( queryBytes ), z, c );
    } else {
      result =
          ( (CassandraConnection) m_keyspace.getConnection() ).getClient().execute_cql_query(
              ByteBuffer.wrap( queryBytes ), z );
    }

    m_resultIterator = result.getRowsIterator();
  }

  /**
   * Get the next output row(s) from the query. This might be a tuple row (i.e. a tuple representing one column value
   * from the current row) if tuple mode is activated. There might also be more than one row returned if the query is
   * CQL 3 and there is a collection that has been unwound. Returns null when there are no more output rows to be
   * produced from the query.
   * 
   * @param outputRowMeta
   *          the Kettle output row structure
   * @param outputFormatMap
   *          map of field names to 0-based indexes in the outgoing row structure
   * @return the next output row(s) from the query
   * @throws Exception
   *           if a query hasn't been executed or another problem occurs.
   */
  @Override
  public Object[][] getNextOutputRow( RowMetaInterface outputRowMeta, Map<String, Integer> outputFormatMap )
    throws Exception {

    if ( !m_outputTuples ) {
      if ( !m_resultIterator.hasNext() ) {
        return null;
      }

      CqlRow nextRow = m_resultIterator.next();
      return cassandraRowToKettle( nextRow, outputRowMeta, outputFormatMap );
    }

    // Tuple mode
    if ( m_cassandraColIter == null || !m_cassandraColIter.hasNext() ) {
      // get next row first

      if ( !m_resultIterator.hasNext() ) {
        return null; // done
      }

      m_currentTupleRow = m_resultIterator.next();

      if ( m_currentTupleRow == null ) {
        // done - no more data
        m_cassandraColIter = null;

        return null;
      }

      // set up a new column iterator and get the key value for this row
      List<Column> colsList = m_currentTupleRow.getColumns();
      m_cassandraColIter = colsList.iterator();
      if ( !m_cql3 ) {
        m_currentRowKey = m_metaData.getKeyValue( m_currentTupleRow );

        if ( m_isSelectStarQuery ) {
          // advance beyond the key (which is always the first col in a select *
          // query).
          // The reason for this is that if we are reading all columns from
          // a dynamic column family where the column names (comparator) are
          // non-textual then the comparator cannot be used to decode the
          // key name (because the key's name is always textual)
          if ( m_cassandraColIter.hasNext() ) {
            m_cassandraColIter.next();
          }
        }
      } else {
        // For CQL 3 we actually have to go through all the columns and pull
        // out the values for the cols that make up the key. This is because
        // CqlRow.getKey() returns zero bytes when reading from a CQL 3 table

        List<String> keyColNames = m_metaData.getKeyColumnNames();
        int totalKeyCols = keyColNames.size();
        int keyColCount = 0;
        StringBuilder buff = new StringBuilder();
        for ( Column c : colsList ) {
          String colName = m_metaData.getColumnName( c ).trim();
          if ( keyColNames.contains( colName ) ) {
            Object val = m_metaData.getColumnValue( c );
            ValueMetaInterface cv = m_metaData.getValueMetaForColumn( colName );
            String valS = ( val == null ) ? null : cv.getString( val );

            if ( keyColCount == 0 ) {
              buff.append( valS );
            } else {
              buff.append( "," ).append( valS ); //$NON-NLS-1$
            }

            keyColCount++;
            if ( keyColCount == totalKeyCols ) {
              break;
            }
          }
        }

        m_currentRowKey = buff.toString();
      }
      if ( m_currentRowKey == null ) {
        throw new KettleException( BaseMessages.getString( PKG,
            "LegacyCQLRowHandler.Error.UnableToObtainAKeyValueForRow" ) ); //$NON-NLS-1$
      }
    }

    return cassandraRowToKettleTupleMode( outputRowMeta );
  }

  @Override
  public void setKeyspace( Keyspace keyspace ) {
    m_keyspace = (LegacyKeyspace) keyspace;
  }

  protected Object[][] cassandraRowToKettleTupleMode( RowMetaInterface outputRowMeta ) throws Exception {
    Object[][] outputRowData = new Object[1][];
    outputRowData[0] = RowDataUtil.allocateRowData( outputRowMeta.size() );

    // String keyName = m_metaData.getKeyName();
    String keyName = "KEY"; // always use this name for tuple mode //$NON-NLS-1$
    int keyIndex = outputRowMeta.indexOfValue( keyName );
    if ( keyIndex < 0 ) {
      throw new Exception( BaseMessages.getString( PKG, "LegacyCQLRowHandler.Error.UnableToFindKeyFieldName", keyName ) ); //$NON-NLS-1$
    }
    outputRowData[0][keyIndex] = m_currentRowKey;

    // advance the iterator to the next column
    if ( m_cassandraColIter.hasNext() ) {
      Column aCol = m_cassandraColIter.next();

      String colName = m_metaData.getColumnName( aCol );

      // for queries that specify column names we need to check that the value
      // is not null in this row
      while ( m_metaData.getColumnValue( aCol ) == null ) {
        if ( m_cassandraColIter.hasNext() ) {
          aCol = m_cassandraColIter.next();
          colName = m_metaData.getColumnName( aCol );
        } else {
          return null;
        }
      }

      outputRowData[0][1] = colName;

      // do the value (stored as a string)
      Object colValue = m_metaData.getColumnValue( aCol );
      ValueMetaInterface colMeta = m_metaData.getValueMetaForColumn( colName );

      String stringV = colMeta.getString( colValue );
      outputRowData[0][2] = stringV;

      if ( colValue instanceof Date ) {
        ValueMeta tempDateMeta = new ValueMeta( "temp", //$NON-NLS-1$
            ValueMetaInterface.TYPE_DATE );
        stringV = tempDateMeta.getString( colValue );
        outputRowData[0][2] = stringV;
      } else if ( colValue instanceof byte[] ) {
        outputRowData[0][2] = colValue;
      }

      // The timestamp as a date object. For some reason, this is
      // not available when reading from a CQL3 table (timestamp
      // returned is always 0)
      long timestampL = aCol.getTimestamp();
      outputRowData[0][3] = timestampL;
    } else {
      m_currentRowKey = null;
      return null; // signify no more columns for this row...
    }

    return outputRowData;
  }

  /**
   * Concatenate the entries of a Cassandra set or list value into a comma-separated string list
   * 
   * @param toConcat
   *          the set/list to concatenate
   * @return a comma-separated string list
   */
  protected static String concatTextCollection( Collection<?> toConcat ) {
    StringBuilder b = new StringBuilder();
    boolean first = true;
    for ( Object o : toConcat ) {
      if ( first ) {
        b.append( o.toString() );
        first = false;
      } else {
        b.append( "," ).append( o.toString() ); //$NON-NLS-1$
      }
    }

    return b.toString();
  }

  /**
   * Concatenate the entries of a Cassandra map value into a comma-separated string list
   * 
   * @param toConcat
   *          the Map to concatenate
   * @return a comma-separated string list
   */
  protected static String concatTextMap( Map<?, ?> toConcat ) {
    StringBuilder b = new StringBuilder();
    boolean first = true;
    for ( Map.Entry<?, ?> e : ( (Map<?, ?>) toConcat ).entrySet() ) {
      String pair = e.getKey().toString() + ":" + e.getValue().toString(); //$NON-NLS-1$
      if ( first ) {
        b.append( pair );
        first = false;
      } else {
        b.append( "," ).append( pair );
      }
    }

    return b.toString();
  }

  /**
   * Converts a cassandra row to Kettle row(s)
   * 
   * @param cassandraRow
   *          a row from the column family
   * @param outputRowMeta
   *          the output row metadata
   * @param outputFormatMap
   *          a Map of output field names to indexes in the outgoing Kettle row structure
   * @return one or more Kettle rows
   * @throws Exception
   *           if we encounter more than one collection value and the additional ones are not text collections, or if
   *           some other problem occurs
   */
  @SuppressWarnings( "unchecked" )
  protected Object[][] cassandraRowToKettle( CqlRow cassandraRow, RowMetaInterface outputRowMeta,
      Map<String, Integer> outputFormatMap ) throws Exception {

    Object[][] outputRowData = new Object[1][];
    Object[] baseOutputRowData = RowDataUtil.allocateRowData( outputRowMeta.size() );
    Collection<?> primaryCollection = null;
    int primaryCollectionOutputIndex = -1;

    // do the columns
    List<Column> rowColumns = cassandraRow.getColumns();
    boolean first = true;
    for ( Column aCol : rowColumns ) {
      String colName = m_metaData.getColumnName( aCol );

      // the key is always first in a CQL2 result
      if ( !m_cql3 && first ) {
        // for CQL 2 'key' is a reserved word, so even if we create a
        // table with a properly quoted primary key called 'key' (and,
        // in this case, CfDef.key_alias even returns 'key' in lower case),
        // Column will have upper cased it to KEY when we get results back
        // from Cassandra!

        if ( colName.equals( "KEY" ) ) {
          colName = "key";
        }
      }

      first = false;

      Integer outputIndex = outputFormatMap.get( colName );
      if ( outputIndex != null ) {
        Object colValue = m_metaData.getColumnValue( aCol, getSelector( outputIndex ) );
        if ( colValue instanceof Collection || colValue instanceof Map ) {
          if ( primaryCollection == null ) {
            if ( colValue instanceof Collection ) {
              primaryCollection = (Collection) colValue;
            } else {
              primaryCollection = new ArrayList<String>();
              for ( Map.Entry<?, ?> e : ( (Map<?, ?>) colValue ).entrySet() ) {
                String pair = e.getKey().toString() + ":" + e.getValue().toString(); //$NON-NLS-1$
                ( (List<String>) primaryCollection ).add( pair );
              }
            }
            primaryCollectionOutputIndex = outputIndex;
          } else {
            // concat the values only if the type of the collection/map is string -
            // we can only do this for string collections because unwinding to single
            // values and producing one concatenated value is the same type
            if ( outputRowMeta.getValueMeta( outputIndex.intValue() ).isString() ) {
              String concat =
                  colValue instanceof Map ? concatTextMap( (Map<?, ?>) colValue )
                      : concatTextCollection( (Collection<?>) colValue );
              baseOutputRowData[outputIndex.intValue()] = concat;
            } else {
              throw new KettleException( BaseMessages.getString( PKG,
                  "LegacyCQLRowHandler.Error.CantHandleAdditionalCollectionsThatAreNotOfTypeText" ) ); //$NON-NLS-1$
            }
          }
        } else {
          baseOutputRowData[outputIndex.intValue()] = colValue;
        }
      }
    }

    if ( primaryCollection == null ) {
      outputRowData[0] = baseOutputRowData;
    } else {
      // We need to unwind (relax :-))
      int numElements = primaryCollection.size();
      outputRowData = new Object[numElements][];
      int count = 0;
      Iterator<?> i = primaryCollection.iterator();
      while ( i.hasNext() ) {
        Object collVal = i.next();
        outputRowData[count] = Arrays.copyOf( baseOutputRowData, baseOutputRowData.length );
        outputRowData[count][primaryCollectionOutputIndex] = collVal;
        count++;
      }
    }

    return outputRowData;
  }

  /**
   * Returns selector by index from the list of selectors if this is not null,
   * 
   * @param index
   *          the index of element
   * @return selector
   */
  private Selector getSelector( int index ) {
    Selector selector = null;
    if ( selectorList != null ) {
      selector = selectorList[index];
    }
    return selector;
  }

}
