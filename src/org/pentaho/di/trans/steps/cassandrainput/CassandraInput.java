/*******************************************************************************
 *
 * Pentaho Big Data
 *
 * Copyright (C) 2002-2017 by Pentaho : http://www.pentaho.com
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

package org.pentaho.di.trans.steps.cassandrainput;

import java.util.HashMap;
import java.util.Iterator;
import java.util.List;
import java.util.Map;

import org.apache.cassandra.thrift.Compression;
import org.apache.cassandra.thrift.CqlRow;
import org.pentaho.cassandra.CassandraUtils;
import org.pentaho.cassandra.ConnectionFactory;
import org.pentaho.cassandra.spi.CQLRowHandler;
import org.pentaho.cassandra.spi.ColumnFamilyMetaData;
import org.pentaho.cassandra.spi.Connection;
import org.pentaho.cassandra.spi.Keyspace;
import org.pentaho.cassandra.spi.NonCQLRowHandler;
import org.pentaho.di.core.exception.KettleException;
import org.pentaho.di.core.row.RowMeta;
import org.pentaho.di.core.util.Utils;
import org.pentaho.di.i18n.BaseMessages;
import org.pentaho.di.trans.Trans;
import org.pentaho.di.trans.TransMeta;
import org.pentaho.di.trans.step.BaseStep;
import org.pentaho.di.trans.step.StepDataInterface;
import org.pentaho.di.trans.step.StepInterface;
import org.pentaho.di.trans.step.StepMeta;
import org.pentaho.di.trans.step.StepMetaInterface;

/**
 * Class providing an input step for reading data from a table (column family) in Cassandra. Accesses the schema
 * information stored in Cassandra for type information.
 * 
 * @author Mark Hall (mhall{[at]}pentaho{[dot]}com)
 * @version $Revision$
 */
public class CassandraInput extends BaseStep implements StepInterface {

  protected CassandraInputMeta m_meta;
  protected CassandraInputData m_data;

  public CassandraInput( StepMeta stepMeta, StepDataInterface stepDataInterface, int copyNr, TransMeta transMeta,
      Trans trans ) {

    super( stepMeta, stepDataInterface, copyNr, transMeta, trans );
  }

  /** Connection to cassandra */
  protected Connection m_connection;

  /** Keyspace */
  protected Keyspace m_keyspace;

  /** Column meta data and schema information */
  protected ColumnFamilyMetaData m_cassandraMeta;

  /** Handler for CQL-based row fetching */
  protected CQLRowHandler m_cqlHandler;

  /** Handler for non-CQL (i.e. slice type) row fetching */
  protected NonCQLRowHandler m_nonCqlHandler;

  /** For iterating over a result set */
  protected Iterator<CqlRow> m_resultIterator;

  /**
   * map of indexes into the output field structure (key is special - it's always the first field in the output row meta
   */
  protected Map<String, Integer> m_outputFormatMap = new HashMap<String, Integer>();

  /** Current input row being processed (if executing for each row) */
  protected Object[] m_currentInputRowDrivingQuery = null;

  /** Column family name */
  protected String m_colFamName;

  @Override
  public boolean processRow( StepMetaInterface smi, StepDataInterface sdi ) throws KettleException {

    if ( !isStopped() ) {

      if ( m_meta.getExecuteForEachIncomingRow() && m_currentInputRowDrivingQuery == null ) {
        m_currentInputRowDrivingQuery = getRow();

        if ( m_currentInputRowDrivingQuery == null ) {
          // no more input, no more queries to make
          setOutputDone();
          return false;
        }

        if ( !first ) {
          initQuery();
        }
      }

      if ( first ) {
        first = false;

        // Get the connection to Cassandra
        String hostS = environmentSubstitute( m_meta.getCassandraHost() );
        String portS = environmentSubstitute( m_meta.getCassandraPort() );
        String timeoutS = environmentSubstitute( m_meta.getSocketTimeout() );
        String maxLength = environmentSubstitute( m_meta.getMaxLength() );
        String userS = m_meta.getUsername();
        String passS = m_meta.getPassword();
        if ( !Utils.isEmpty( userS ) && !Utils.isEmpty( passS ) ) {
          userS = environmentSubstitute( userS );
          passS = environmentSubstitute( passS );
        }
        String keyspaceS = environmentSubstitute( m_meta.getCassandraKeyspace() );

        if ( Utils.isEmpty( hostS ) || Utils.isEmpty( portS ) || Utils.isEmpty( keyspaceS ) ) {
          throw new KettleException( "Some connection details are missing!!" ); //$NON-NLS-1$
        }

        logBasic( BaseMessages.getString( CassandraInputMeta.PKG,
            "CassandraInput.Info.Connecting", hostS, portS, keyspaceS ) ); //$NON-NLS-1$

        Map<String, String> opts = new HashMap<String, String>();

        if ( !Utils.isEmpty( timeoutS ) ) {
          opts.put( CassandraUtils.ConnectionOptions.SOCKET_TIMEOUT, timeoutS );
        }

        if ( !Utils.isEmpty( maxLength ) ) {
          opts.put( CassandraUtils.ConnectionOptions.MAX_LENGTH, maxLength );
        }

        if ( m_meta.getUseCQL3() ) {
          opts.put( CassandraUtils.CQLOptions.CQLVERSION_OPTION, CassandraUtils.CQLOptions.CQL3_STRING );
        }

        if ( m_meta.getUseCompression() ) {
          opts.put( CassandraUtils.ConnectionOptions.COMPRESSION, Boolean.TRUE.toString() );
        }

        if ( opts.size() > 0 ) {
          logBasic( BaseMessages.getString( CassandraInputMeta.PKG, "CassandraInput.Info.UsingConnectionOptions", //$NON-NLS-1$
              CassandraUtils.optionsToString( opts ) ) );
        }

        try {
          m_connection =
              CassandraUtils.getCassandraConnection( hostS, Integer.parseInt( portS ), userS, passS,
                  m_meta.isUseDriver()
                    ? ConnectionFactory.Driver.BINARY_CQL3_PROTOCOL
                    : ConnectionFactory.Driver.LEGACY_THRIFT, opts );

          m_keyspace = m_connection.getKeyspace( keyspaceS );
        } catch ( Exception ex ) {
          closeConnection();
          throw new KettleException( ex.getMessage(), ex );
        }

        // check the source column family (table) first
        m_colFamName =
            CassandraUtils.getColumnFamilyNameFromCQLSelectQuery( environmentSubstitute( m_meta.getCQLSelectQuery() ) );

        if ( Utils.isEmpty( m_colFamName ) ) {
          throw new KettleException( BaseMessages.getString( CassandraInputMeta.PKG,
              "CassandraInput.Error.NonExistentColumnFamily" ) ); //$NON-NLS-1$
        }

        try {
          if ( !m_keyspace.columnFamilyExists( m_colFamName ) ) {
            throw new KettleException(
                BaseMessages
                    .getString(
                        CassandraInputMeta.PKG,
                        "CassandraInput.Error.NonExistentColumnFamily", m_meta.getUseCQL3() ? CassandraUtils.removeQuotes( m_colFamName ) : m_colFamName, //$NON-NLS-1$
                        keyspaceS ) );
          }
        } catch ( Exception ex ) {
          closeConnection();

          throw new KettleException( ex.getMessage(), ex );
        }

        // set up the output row meta
        m_data.setOutputRowMeta( new RowMeta() );
        m_meta.getFields( m_data.getOutputRowMeta(), getStepname(), null, null, this );

        // check that there are some outgoing fields!
        if ( m_data.getOutputRowMeta().size() == 0 ) {
          throw new KettleException( BaseMessages.getString( CassandraInputMeta.PKG,
              "CassandraInput.Error.QueryWontProduceOutputFields" ) ); //$NON-NLS-1$
        }

        // set up the lookup map
        if ( !m_meta.getOutputKeyValueTimestampTuples() ) {
          for ( int i = 0; i < m_data.getOutputRowMeta().size(); i++ ) {
            String fieldName = m_data.getOutputRowMeta().getValueMeta( i ).getName();
            m_outputFormatMap.put( fieldName, i );
          }
        }

        // column family name (key) is the first field output
        try {
          logBasic( BaseMessages
              .getString( CassandraInputMeta.PKG, "CassandraInput.Info.GettintMetaData", m_colFamName ) ); //$NON-NLS-1$

          m_cassandraMeta = m_keyspace.getColumnFamilyMetaData( m_colFamName );
        } catch ( Exception e ) {
          closeConnection();
          throw new KettleException( e.getMessage(), e );
        }

        initQuery();
      }

      Object[][] outRowData = new Object[1][];
      if ( !m_meta.getUseThriftIO() ) {
        try {
          outRowData = m_cqlHandler.getNextOutputRow( m_data.getOutputRowMeta(), m_outputFormatMap );
        } catch ( Exception e ) {
          throw new KettleException( e.getMessage(), e );
        }

        if ( outRowData != null ) {

          for ( Object[] r : outRowData ) {
            putRow( m_data.getOutputRowMeta(), r );
          }

          if ( log.isRowLevel() ) {
            log.logRowlevel( toString(), "Outputted row #" + getProcessed() //$NON-NLS-1$
                + " : " + outRowData ); //$NON-NLS-1$
          }

          if ( checkFeedback( getProcessed() ) ) {
            logBasic( "Read " + getProcessed() + " rows from Cassandra" ); //$NON-NLS-1$ //$NON-NLS-2$
          }
        }
      } else if ( m_meta.getOutputKeyValueTimestampTuples() ) {
        // Execute for each row does not make sense for thrift mode since
        // a where clause can't be used.
        outRowData = new Object[1][];
        try {
          outRowData[0] = m_nonCqlHandler.getNextOutputRow( m_data.getOutputRowMeta() );
        } catch ( Exception e ) {
          throw new KettleException( e.getMessage(), e );
        }

        if ( outRowData[0] != null && outRowData[0].length > 0 ) {

          putRow( m_data.getOutputRowMeta(), outRowData[0] );

          if ( log.isRowLevel() ) {
            log.logRowlevel( toString(), "Outputted row #" + getProcessed() //$NON-NLS-1$
                + " : " + outRowData ); //$NON-NLS-1$
          }

          if ( checkFeedback( getProcessed() ) ) {
            logBasic( "Read " + getProcessed() + " rows from Cassandra" ); //$NON-NLS-1$ //$NON-NLS-2$
          }
        } else {
          outRowData = null;
        }
      } else {
        throw new KettleException( BaseMessages.getString( CassandraInputMeta.PKG,
            "CassandraInput.Error.TupleModeMustBeUsedForNonCQLIO" ) ); //$NON-NLS-1$
      }

      if ( outRowData == null ) {
        if ( !m_meta.getExecuteForEachIncomingRow() ) {
          // we're done now
          closeConnection();
          setOutputDone();
          return false;
        } else {
          m_currentInputRowDrivingQuery = null; // finished with this row
        }
      }
    } else {
      closeConnection();
      return false;
    }

    return true;
  }

  @Override
  public boolean init( StepMetaInterface stepMeta, StepDataInterface stepData ) {
    if ( super.init( stepMeta, stepData ) ) {
      m_data = (CassandraInputData) stepData;
      m_meta = (CassandraInputMeta) stepMeta;
    }

    return true;
  }

  protected void initQuery() throws KettleException {
    String queryS = environmentSubstitute( m_meta.getCQLSelectQuery() );
    if ( m_meta.getExecuteForEachIncomingRow() ) {
      queryS = fieldSubstitute( queryS, getInputRowMeta(), m_currentInputRowDrivingQuery );
    }
    Compression compression = m_meta.getUseCompression() ? Compression.GZIP : Compression.NONE;
    try {
      if ( !m_meta.getUseThriftIO() ) {
        logBasic( BaseMessages.getString( CassandraInputMeta.PKG, "CassandraInput.Info.ExecutingQuery", //$NON-NLS-1$
            queryS, ( m_meta.getUseCompression() ? BaseMessages.getString( CassandraInputMeta.PKG,
                "CassandraInput.Info.UsingGZIPCompression" ) : "" ) ) ); //$NON-NLS-1$ //$NON-NLS-2$

        if ( m_cqlHandler == null ) {
          m_cqlHandler = m_keyspace.getCQLRowHandler();
        }
        m_cqlHandler.newRowQuery( this, m_colFamName, queryS, compression.name(), "", //$NON-NLS-1$
            m_meta.getOutputKeyValueTimestampTuples(), log );

      } else if ( m_meta.getOutputKeyValueTimestampTuples() ) {
        // --------------- use non-CQL IO (only applicable for <key, value>
        // tuple mode at present) ----------

        if ( m_nonCqlHandler == null ) {
          m_nonCqlHandler = m_keyspace.getNonCQLRowHandler();
        }

        List<String> userCols =
            ( m_meta.m_specificCols != null && m_meta.m_specificCols.size() > 0 ) ? m_meta.m_specificCols : null;

        m_nonCqlHandler.newRowQuery( this, m_colFamName, userCols, m_meta.m_rowLimit, m_meta.m_colLimit,
            m_meta.m_rowBatchSize, m_meta.m_colBatchSize, "", log ); //$NON-NLS-1$

        // --------------- end non-CQL IO mode
      }
    } catch ( Exception e ) {
      closeConnection();

      throw new KettleException( e.getMessage(), e );
    }
  }

  @Override
  public void setStopped( boolean stopped ) {
    if ( isStopped() && stopped == true ) {
      return;
    }
    super.setStopped( stopped );
  }

  @Override
  public void dispose( StepMetaInterface smi, StepDataInterface sdi ) {
    try {
      closeConnection();
    } catch ( KettleException e ) {
      e.printStackTrace();
    }
  }

  protected void closeConnection() throws KettleException {
    if ( m_connection != null ) {
      logBasic( BaseMessages.getString( CassandraInputMeta.PKG, "CassandraInput.Info.ClosingConnection" ) ); //$NON-NLS-1$
      try {
        m_connection.closeConnection();
        m_connection = null;
      } catch ( Exception e ) {
        throw new KettleException( e.getMessage(), e );
      }
    }
  }
}
