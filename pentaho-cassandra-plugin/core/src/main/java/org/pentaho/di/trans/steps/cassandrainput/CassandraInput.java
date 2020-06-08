/*******************************************************************************
 *
 * Pentaho Big Data
 *
 * Copyright (C) 2002-2020 by Hitachi Vantara : http://www.pentaho.com
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

import org.pentaho.cassandra.driver.datastax.DriverConnection;
import org.pentaho.cassandra.spi.CQLRowHandler;
import org.pentaho.cassandra.spi.Keyspace;
import org.pentaho.cassandra.util.Compression;
import org.pentaho.di.core.exception.KettleException;
import org.pentaho.di.core.row.RowMeta;
import org.pentaho.di.i18n.BaseMessages;
import org.pentaho.di.trans.Trans;
import org.pentaho.di.trans.TransMeta;
import org.pentaho.di.trans.step.BaseStep;
import org.pentaho.di.trans.step.StepDataInterface;
import org.pentaho.di.trans.step.StepInterface;
import org.pentaho.di.trans.step.StepMeta;
import org.pentaho.di.trans.step.StepMetaInterface;

import java.util.HashMap;
import java.util.Map;

/**
 * Class providing an input step for reading data from a table in Cassandra. Accesses the schema information stored in
 * Cassandra for type information.
 *
 * @author Mark Hall (mhall{[at]}pentaho{[dot]}com)
 * @version $Revision$
 */
public class CassandraInput extends BaseStep implements StepInterface {

  protected CassandraInputMeta meta;
  protected CassandraInputData data;

  public CassandraInput( StepMeta stepMeta, StepDataInterface stepDataInterface, int copyNr, TransMeta transMeta,
      Trans trans ) {

    super( stepMeta, stepDataInterface, copyNr, transMeta, trans );
  }

  /** Connection to cassandra */
  protected DriverConnection connection;

  /** Keyspace */
  protected Keyspace keyspace;

  /** Handler for CQL-based row fetching */
  protected CQLRowHandler cqlHandler;

  /**
   * map of indexes into the output field structure (key is special - it's always the first field in the output row meta
   */
  protected Map<String, Integer> outputFormatMap = new HashMap<String, Integer>();

  /** Current input row being processed (if executing for each row) */
  protected Object[] currentInputRowDrivingQuery = null;

  /** Column family name */
  protected String tableName;

  @Override
  public boolean processRow( StepMetaInterface smi, StepDataInterface sdi ) throws KettleException {

    if ( !isStopped() ) {

      if ( meta.getExecuteForEachIncomingRow() && currentInputRowDrivingQuery == null ) {
        currentInputRowDrivingQuery = getRow();

        if ( currentInputRowDrivingQuery == null ) {
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

        String keyspaceS = environmentSubstitute( meta.getCassandraKeyspace() );

        try {
          connection = CassandraInputData.getCassandraConnection( meta, this, getLogChannel(), true );
          connection.setExpandCollection( meta.isExpandComplex() );
          keyspace = connection.getKeyspace( keyspaceS );

        } catch ( Exception ex ) {
          closeConnection();
          throw new KettleException( ex.getMessage(), ex );
        }

        // set up the output row meta
        data.setOutputRowMeta( new RowMeta() );
        meta.getFields( data.getOutputRowMeta(), getStepname(), null, null, this, repository, metaStore );

        // check that there are some outgoing fields!
        if ( data.getOutputRowMeta().size() == 0 ) {
          throw new KettleException( BaseMessages.getString( CassandraInputMeta.PKG,
            "CassandraInput.Error.QueryWontProduceOutputFields" ) ); //$NON-NLS-1$
        }

        // set up the lookup map
        for ( int i = 0; i < data.getOutputRowMeta().size(); i++ ) {
          String fieldName = data.getOutputRowMeta().getValueMeta( i ).getName();
          outputFormatMap.put( fieldName, i );
        }

        initQuery();
      }

      Object[][] outRowData = new Object[1][];
      try {
        outRowData = cqlHandler.getNextOutputRow( data.getOutputRowMeta(), outputFormatMap );
      } catch ( Exception e ) {
        throw new KettleException( e.getMessage(), e );
      }

      if ( outRowData != null ) {
        for ( Object[] r : outRowData ) {
          putRow( data.getOutputRowMeta(), r );
        }

        if ( log.isRowLevel() ) {
          log.logRowlevel( toString(), "Outputted row #" + getProcessed() //$NON-NLS-1$
              + " : " + outRowData ); //$NON-NLS-1$
        }

        if ( checkFeedback( getProcessed() ) ) {
          logBasic( "Read " + getProcessed() + " rows from Cassandra" ); //$NON-NLS-1$ //$NON-NLS-2$
        }
      }

      if ( outRowData == null ) {
        if ( !meta.getExecuteForEachIncomingRow() ) {
          // we're done now
          closeConnection();
          setOutputDone();
          return false;
        } else {
          currentInputRowDrivingQuery = null; // finished with this row
        }
      }
    } else {
      closeConnection();
      setOutputDone();
      return false;
    }

    return true;
  }

  @Override
  public boolean init( StepMetaInterface stepMeta, StepDataInterface stepData ) {
    if ( super.init( stepMeta, stepData ) ) {
      data = (CassandraInputData) stepData;
      meta = (CassandraInputMeta) stepMeta;
    }

    return true;
  }

  protected void initQuery() throws KettleException {
    String queryS = environmentSubstitute( meta.getCQLSelectQuery() );
    if ( meta.getExecuteForEachIncomingRow() ) {
      queryS = fieldSubstitute( queryS, getInputRowMeta(), currentInputRowDrivingQuery );
    }
    Compression compression = meta.getUseCompression() ? Compression.GZIP : Compression.NONE;
    try {
      if ( log.isDebug() ) {
        logDebug( BaseMessages.getString( CassandraInputMeta.PKG, "CassandraInput.Info.ExecutingQuery", //$NON-NLS-1$
          queryS, ( meta.getUseCompression() ? BaseMessages.getString( CassandraInputMeta.PKG,
            "CassandraInput.Info.UsingGZIPCompression" ) : "" ) ) ); // $NON-NLS-!$ //$NON-NLS-2$
      }
      if ( cqlHandler == null ) {
        cqlHandler = keyspace.getCQLRowHandler( meta.isNotExpandingMaps() );
      }
      cqlHandler.newRowQuery( this, tableName, queryS, compression.name(), "", log );
    } catch ( Exception e ) {
      closeConnection();

      throw new KettleException( e.getMessage(), e );
    }
  }
  /*
   * Why would one do this
   * 
   * @Override public void setStopped( boolean stopped ) { if ( isStopped() && stopped == true ) { return; }
   * super.setStopped( stopped ); }
   */

  @Override
  public void dispose( StepMetaInterface smi, StepDataInterface sdi ) {
    try {
      closeConnection();
    } catch ( KettleException e ) {
      e.printStackTrace();
    }
  }

  protected void closeConnection() throws KettleException {
    if ( connection != null ) {
      logBasic( BaseMessages.getString( CassandraInputMeta.PKG, "CassandraInput.Info.ClosingConnection" ) ); //$NON-NLS-1$
      try {
        connection.closeConnection();
        connection = null;
      } catch ( Exception e ) {
        throw new KettleException( e.getMessage(), e );
      }
    }
  }
}
