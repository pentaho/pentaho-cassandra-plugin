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

package org.pentaho.di.trans.steps.cassandraoutput;

import org.pentaho.cassandra.ConnectionFactory;
import org.pentaho.cassandra.datastax.DriverCQLRowHandler;
import org.pentaho.cassandra.datastax.DriverConnection;
import org.pentaho.cassandra.spi.CQLRowHandler;
import org.pentaho.cassandra.spi.ITableMetaData;
import org.pentaho.cassandra.spi.Keyspace;
import org.pentaho.cassandra.util.CassandraUtils;
import org.pentaho.di.core.exception.KettleException;
import org.pentaho.di.core.util.Utils;
import org.pentaho.di.i18n.BaseMessages;
import org.pentaho.di.trans.Trans;
import org.pentaho.di.trans.TransMeta;
import org.pentaho.di.trans.step.BaseStep;
import org.pentaho.di.trans.step.StepDataInterface;
import org.pentaho.di.trans.step.StepInterface;
import org.pentaho.di.trans.step.StepMeta;
import org.pentaho.di.trans.step.StepMetaInterface;

import java.util.ArrayList;
import java.util.HashMap;
import java.util.List;
import java.util.Map;

/**
 * Class providing an output step for writing data to a cassandra table. Can create the specified
 * table (if it doesn't already exist) and can update table meta data.
 *
 * @author Mark Hall (mhall{[at]}pentaho{[dot]}com)
 */
public class CassandraOutput extends BaseStep implements StepInterface {

  protected CassandraOutputMeta meta;
  protected CassandraOutputData data;

  public CassandraOutput( StepMeta stepMeta, StepDataInterface stepDataInterface, int copyNr, TransMeta transMeta,
      Trans trans ) {

    super( stepMeta, stepDataInterface, copyNr, transMeta, trans );
  }

  protected DriverConnection connection;

  protected Keyspace keyspace;

  protected CQLRowHandler cqlHandler = null;

  /** Column meta data and schema information */
  protected ITableMetaData cassandraMeta;

  /** Holds batch insert CQL statement */
  protected StringBuilder batchInsertCql;

  /** Current batch of rows to insert */
  protected List<Object[]> batch;

  /** The number of rows seen so far for this batch */
  protected int rowsSeen;

  /** The batch size to use */
  protected int batchSize = 100;

  /** The consistency to use - null means to use the cassandra default */
  protected String consistency = null;

  /** The name of the table to write to */
  protected String tableName;

  /** The name of the keyspace */
  protected String keyspaceName;

  /** The index of the key field in the incoming rows */
  protected List<Integer> keyIndexes = null;

  protected int cqlBatchInsertTimeout = 0;

  /** Default batch split factor */
  protected int batchSplitFactor = 10;

  /** Consistency level to use */
  protected String consistencyLevel;

  /** Options for keyspace and row handlers */
  protected Map<String, String> opts;

  protected void initialize( StepMetaInterface smi, StepDataInterface sdi ) throws KettleException {

    meta = (CassandraOutputMeta) smi;
    data = (CassandraOutputData) sdi;

    first = false;
    rowsSeen = 0;

    // Get the connection to Cassandra
    String hostS = environmentSubstitute( meta.getCassandraHost() );
    String portS = environmentSubstitute( meta.getCassandraPort() );
    String userS = meta.getUsername();
    String passS = meta.getPassword();
    String batchTimeoutS = environmentSubstitute( meta.getCQLBatchInsertTimeout() );
    String batchSplitFactor = environmentSubstitute( meta.getCQLSubBatchSize() );
    String schemaHostS = environmentSubstitute( meta.getSchemaHost() );
    String schemaPortS = environmentSubstitute( meta.getSchemaPort() );
    String applicationConf = environmentSubstitute( meta.getApplicationConf() );

    if ( Utils.isEmpty( schemaHostS ) ) {
      schemaHostS = hostS;
    }
    if ( Utils.isEmpty( schemaPortS ) ) {
      schemaPortS = portS;
    }

    if ( !Utils.isEmpty( userS ) && !Utils.isEmpty( passS ) ) {
      userS = environmentSubstitute( userS );
      passS = environmentSubstitute( passS );
    }
    keyspaceName = environmentSubstitute( meta.getCassandraKeyspace() );
    tableName = CassandraUtils.cql3MixedCaseQuote( environmentSubstitute( meta.getTableName() ) );
    consistencyLevel = environmentSubstitute( meta.getConsistency() );

    String keyField = environmentSubstitute( meta.getKeyField() );

    try {

      if ( !Utils.isEmpty( batchTimeoutS ) ) {
        try {
          cqlBatchInsertTimeout = Integer.parseInt( batchTimeoutS );
          if ( cqlBatchInsertTimeout < 500 ) {
            logBasic( BaseMessages.getString( CassandraOutputMeta.PKG, "CassandraOutput.Message.MinimumTimeout" ) ); //$NON-NLS-1$
            cqlBatchInsertTimeout = 500;
          }
        } catch ( NumberFormatException e ) {
          logError( BaseMessages.getString( CassandraOutputMeta.PKG, "CassandraOutput.Error.CantParseTimeout" ) ); //$NON-NLS-1$
          cqlBatchInsertTimeout = 10000;
        }
      }

      if ( !Utils.isEmpty( batchSplitFactor ) ) {
        try {
          this.batchSplitFactor = Integer.parseInt( batchSplitFactor );
        } catch ( NumberFormatException e ) {
          logError( BaseMessages.getString( CassandraOutputMeta.PKG, "CassandraOutput.Error.CantParseSubBatchSize" ) ); //$NON-NLS-1$
        }
      }

      if ( Utils.isEmpty( applicationConf ) && ( Utils.isEmpty( hostS ) || Utils.isEmpty( portS ) || Utils.isEmpty( keyspaceName ) ) ) {
        throw new KettleException( BaseMessages.getString( CassandraOutputMeta.PKG,
            "CassandraOutput.Error.MissingConnectionDetails" ) ); //$NON-NLS-1$
      }

      if ( Utils.isEmpty( tableName ) ) {
        throw new KettleException( BaseMessages.getString( CassandraOutputMeta.PKG,
            "CassandraOutput.Error.NoTableSpecified" ) ); //$NON-NLS-1$
      }

      if ( Utils.isEmpty( keyField ) ) {
        throw new KettleException( BaseMessages.getString( CassandraOutputMeta.PKG,
            "CassandraOutput.Error.NoIncomingKeySpecified" ) ); //$NON-NLS-1$
      }

      // check that the specified key field is present in the incoming data
      String[] kparts = keyField.split( "," ); //$NON-NLS-1$
      keyIndexes = new ArrayList<Integer>();
      for ( String kpart : kparts ) {
        int index = getInputRowMeta().indexOfValue( kpart.trim() );
        if ( index < 0 ) {
          throw new KettleException( BaseMessages.getString( CassandraOutputMeta.PKG,
              "CassandraOutput.Error.CantFindKeyField", keyField ) ); //$NON-NLS-1$
        }
        keyIndexes.add( index );
      }

      logBasic( BaseMessages.getString( CassandraOutputMeta.PKG,
          "CassandraOutput.Message.ConnectingForSchemaOperations", schemaHostS, //$NON-NLS-1$
          schemaPortS, keyspaceName ) );

      DriverConnection connection = null;

      // open up a connection to perform any schema changes
      try {
        connection = openConnection( true );
        Keyspace keyspace = connection.getKeyspace( keyspaceName );

        // Try to execute any apriori CQL commands?
        if ( !Utils.isEmpty( meta.getAprioriCQL() ) ) {
          String aprioriCQL = environmentSubstitute( meta.getAprioriCQL() );
          List<String> statements = CassandraUtils.splitCQLStatements( aprioriCQL );

          logBasic( BaseMessages.getString( CassandraOutputMeta.PKG, "CassandraOutput.Message.ExecutingAprioriCQL", //$NON-NLS-1$
            tableName, aprioriCQL ) );

          String compression = meta.getUseCompression() ? "gzip" : ""; //$NON-NLS-1$ //$NON-NLS-2$

          for ( String cqlS : statements ) {
            try {
              keyspace.executeCQL( cqlS, compression, consistencyLevel, log );
            } catch ( Exception e ) {
              if ( meta.getDontComplainAboutAprioriCQLFailing() ) {
                // just log and continue
                logBasic( "WARNING: " + e.toString() ); //$NON-NLS-1$
              } else {
                throw e;
              }
            }
          }
        }

        if ( !keyspace.tableExists( tableName ) ) {
          if ( meta.getCreateTable() ) {
            // create the table
            boolean result =
                keyspace.createTable( tableName, getInputRowMeta(), keyIndexes,
                    environmentSubstitute( meta.getCreateTableWithClause() ), log );

            if ( !result ) {
              throw new KettleException( BaseMessages.getString( CassandraOutputMeta.PKG,
                  "CassandraOutput.Error.NeedAtLeastOneFieldAppartFromKey" ) ); //$NON-NLS-1$
            }
          } else {
            throw new KettleException( BaseMessages.getString( CassandraOutputMeta.PKG,
                "CassandraOutput.Error.TableDoesNotExist", //$NON-NLS-1$
              tableName, keyspaceName ) );
          }
        }

        if ( meta.getUpdateCassandraMeta() ) {
          // Update cassandra meta data for unknown incoming fields?
          keyspace.updateTableCQL3( tableName, getInputRowMeta(), keyIndexes, log );
        }

        // get the table meta data
        logBasic( BaseMessages.getString( CassandraOutputMeta.PKG,
            "CassandraOutput.Message.GettingMetaData", tableName ) ); //$NON-NLS-1$

        cassandraMeta = keyspace.getTableMetaData( tableName );

        // output (downstream) is the same as input
        data.setOutputRowMeta( getInputRowMeta() );

        String batchSize = environmentSubstitute( meta.getBatchSize() );
        if ( !Utils.isEmpty( batchSize ) ) {
          try {
            this.batchSize = Integer.parseInt( batchSize );
          } catch ( NumberFormatException e ) {
            logError( BaseMessages.getString( CassandraOutputMeta.PKG, "CassandraOutput.Error.CantParseBatchSize" ) ); //$NON-NLS-1$
            this.batchSize = 100;
          }
        } else {
          throw new KettleException( BaseMessages.getString( CassandraOutputMeta.PKG,
              "CassandraOutput.Error.NoBatchSizeSet" ) ); //$NON-NLS-1$
        }

        // Truncate (remove all data from) table first?
        if ( meta.getTruncateTable() ) {
          keyspace.truncateTable( tableName, log );
        }
      } finally {
        if ( connection != null ) {
          closeConnection( connection );
          connection = null;
        }
      }

      consistency = environmentSubstitute( meta.getConsistency() );
      batchInsertCql =
          CassandraUtils.newCQLBatch( batchSize, meta.getUseUnloggedBatch() );

      batch = new ArrayList<Object[]>();

      // now open the main connection to use
      openConnection( false );

    } catch ( Exception ex ) {
      logError( BaseMessages.getString( CassandraOutputMeta.PKG, "CassandraOutput.Error.InitializationProblem" ), ex ); //$NON-NLS-1$
    }
  }

  @Override
  public boolean processRow( StepMetaInterface smi, StepDataInterface sdi ) throws KettleException {

    Object[] r = getRow();

    if ( r == null ) {
      // no more output

      // flush the last batch
      if ( rowsSeen > 0 && !isStopped() ) {
        doBatch();
      }
      batchInsertCql = null;
      batch = null;

      closeConnection( connection );
      connection = null;
      keyspace = null;
      cqlHandler = null;

      setOutputDone();
      return false;
    }

    if ( !isStopped() ) {
      if ( first ) {
        initialize( smi, sdi );
      }

      batch.add( r );
      rowsSeen++;

      if ( rowsSeen == batchSize ) {
        doBatch();
      }
    } else {
      closeConnection( connection );
      return false;
    }

    return true;
  }

  protected void doBatch() throws KettleException {

    try {
      doBatch( batch );
    } catch ( Exception e ) {
      logError( BaseMessages.getString( CassandraOutputMeta.PKG,
          "CassandraOutput.Error.CommitFailed", batchInsertCql.toString(), e ) ); //$NON-NLS-1$
      throw new KettleException( e.fillInStackTrace() );
    }

    // ready for a new batch
    batch.clear();
    rowsSeen = 0;
  }

  protected void doBatch( List<Object[]> batch ) throws Exception {
    // stopped?
    if ( isStopped() ) {
      logDebug( BaseMessages.getString( CassandraOutputMeta.PKG, "CassandraOutput.Message.StoppedSkippingBatch" ) ); //$NON-NLS-1$
      return;
    }
    // ignore empty batch
    if ( batch == null || batch.isEmpty() ) {
      logDebug( BaseMessages.getString( CassandraOutputMeta.PKG, "CassandraOutput.Message.SkippingEmptyBatch" ) ); //$NON-NLS-1$
      return;
    }
    // construct CQL/thrift batch and commit
    int size = batch.size();
    try {
      // construct CQL
      batchInsertCql =
          CassandraUtils.newCQLBatch( batchSize, meta.getUseUnloggedBatch() );
      int rowsAdded = 0;
      batch = CassandraUtils.fixBatchMismatchedTypes( batch, getInputRowMeta(), cassandraMeta );
      DriverCQLRowHandler handler = (DriverCQLRowHandler) cqlHandler;
      handler.setUnloggedBatch( meta.getUseUnloggedBatch() );
      handler.batchInsert( getInputRowMeta(), batch, cassandraMeta, consistencyLevel, meta
          .getInsertFieldsNotInMeta(), getLogChannel() );
      // commit
      if ( connection == null ) {
        openConnection( false );
      }

      logDetailed( BaseMessages.getString( CassandraOutputMeta.PKG,
          "CassandraOutput.Message.CommittingBatch", tableName, "" //$NON-NLS-1$ //$NON-NLS-2$
              + rowsAdded ) );
    } catch ( Exception e ) {
      logError( e.getLocalizedMessage(), e );
      setErrors( getErrors() + 1 );
      closeConnection( connection );
      connection = null;
      logDetailed( BaseMessages.getString( CassandraOutputMeta.PKG,
          "CassandraOutput.Error.FailedToInsertBatch", "" + size ), e ); //$NON-NLS-1$ //$NON-NLS-2$

      logDetailed( BaseMessages.getString( CassandraOutputMeta.PKG,
          "CassandraOutput.Message.WillNowTrySplittingIntoSubBatches" ) ); //$NON-NLS-1$

      // is it possible to divide and conquer?
      if ( size == 1 ) {
        // single error row - found it!
        if ( getStepMeta().isDoingErrorHandling() ) {
          putError( getInputRowMeta(), batch.get( 0 ), 1L, e.getMessage(), null, "ERR_INSERT01" ); //$NON-NLS-1$
        }
      } else if ( size > batchSplitFactor ) {
        // split into smaller batches and try separately
        List<Object[]> subBatch = new ArrayList<Object[]>();
        while ( batch.size() > batchSplitFactor ) {
          while ( subBatch.size() < batchSplitFactor && batch.size() > 0 ) {
            // remove from the right - avoid internal shifting
            subBatch.add( batch.remove( batch.size() - 1 ) );
          }
          doBatch( subBatch );
          subBatch.clear();
        }
        doBatch( batch );
      } else {
        // try each row individually
        List<Object[]> subBatch = new ArrayList<Object[]>();
        while ( batch.size() > 0 ) {
          subBatch.clear();
          // remove from the right - avoid internal shifting
          subBatch.add( batch.remove( batch.size() - 1 ) );
          doBatch( subBatch );
        }
      }
    }
  }

  @Override
  public void setStopped( boolean stopped ) {
    if ( isStopped() && stopped == true ) {
      return;
    }
    super.setStopped( stopped );
  }

  protected DriverConnection openConnection( boolean forSchemaChanges ) throws KettleException {
    // Get the connection to Cassandra
    String hostS = environmentSubstitute( meta.getCassandraHost() );
    String portS = environmentSubstitute( meta.getCassandraPort() );
    String userS = meta.getUsername();
    String passS = meta.getPassword();
    String timeoutS = environmentSubstitute( meta.getSocketTimeout() );
    String schemaHostS = environmentSubstitute( meta.getSchemaHost() );
    String schemaPortS = environmentSubstitute( meta.getSchemaPort() );
    String localDataCenter = environmentSubstitute( meta.getLocalDataCenter() );
    String applicationConf = environmentSubstitute( meta.getApplicationConf() );

    if ( Utils.isEmpty( schemaHostS ) ) {
      schemaHostS = hostS;
    }
    if ( Utils.isEmpty( schemaPortS ) ) {
      schemaPortS = portS;
    }

    if ( !Utils.isEmpty( userS ) && !Utils.isEmpty( passS ) ) {
      userS = environmentSubstitute( userS );
      passS = environmentSubstitute( passS );
    }

    opts = new HashMap<String, String>();
    if ( Utils.isEmpty( applicationConf ) ) {
      if ( !Utils.isEmpty( timeoutS ) ) {
        opts.put( CassandraUtils.ConnectionOptions.SOCKET_TIMEOUT, timeoutS );
      }

      if ( meta.isSsl() ) {
        opts.put( CassandraUtils.ConnectionOptions.SSL, "Y" );
      }

    } else {
      opts.put( CassandraUtils.ConnectionOptions.APPLICATION_CONF, applicationConf );
    }

    opts.put( CassandraUtils.BatchOptions.BATCH_TIMEOUT, "" //$NON-NLS-1$
      + cqlBatchInsertTimeout );

    opts.put( CassandraUtils.CQLOptions.DATASTAX_DRIVER_VERSION, CassandraUtils.CQLOptions.CQL3_STRING );

    // Set TTL if specified
    String ttl = meta.getTTL();
    ttl = environmentSubstitute( ttl );
    if ( !Utils.isEmpty( ttl ) && !ttl.startsWith( "-" ) ) {
      String ttlUnit = meta.getTTLUnit();
      CassandraOutputMeta.TTLUnits theUnit = CassandraOutputMeta.TTLUnits.NONE;
      for ( CassandraOutputMeta.TTLUnits u : CassandraOutputMeta.TTLUnits.values() ) {
        if ( ttlUnit.equals( u.toString() ) ) {
          theUnit = u;
          break;
        }
      }
      int value = -1;
      try {
        value = Integer.parseInt( ttl );
        value = theUnit.convertToSeconds( value );
        opts.put( CassandraUtils.BatchOptions.TTL, "" + value );
      } catch ( NumberFormatException e ) {
        logDebug( BaseMessages.getString( CassandraOutputMeta.PKG, "CassandraOutput.Error.CantParseTTL", ttl ) );
      }
    }

    if ( opts.size() > 0 ) {
      logBasic( BaseMessages.getString( CassandraOutputMeta.PKG, "CassandraOutput.Message.UsingConnectionOptions", //$NON-NLS-1$
          CassandraUtils.optionsToString( opts ) ) );
    }

    DriverConnection connection = null;

    try {

      String actualHostToUse = forSchemaChanges ? schemaHostS : hostS;
      String actualPortToUse = forSchemaChanges ? schemaPortS : portS;

      connection =
          CassandraUtils.getCassandraConnection( actualHostToUse, actualPortToUse, userS, passS,
              ConnectionFactory.Driver.BINARY_CQL3_PROTOCOL, opts, localDataCenter );

      // set the global connection only if this connection is not being used
      // just for schema changes
      if ( !forSchemaChanges ) {
        this.connection = connection;
        keyspace = this.connection.getKeyspace( keyspaceName );
        cqlHandler = keyspace.getCQLRowHandler();

      }
    } catch ( Exception ex ) {
      closeConnection( connection );
      throw new KettleException( ex.getMessage(), ex );
    }

    return connection;
  }

  @Override
  public void dispose( StepMetaInterface smi, StepDataInterface sdi ) {
    try {
      closeConnection( connection );
    } catch ( KettleException e ) {
      e.printStackTrace();
    }

    super.dispose( smi, sdi );
  }

  protected void closeConnection( DriverConnection conn ) throws KettleException {
    if ( conn != null ) {
      logBasic( BaseMessages.getString( CassandraOutputMeta.PKG, "CassandraOutput.Message.ClosingConnection" ) ); //$NON-NLS-1$
      try {
        conn.closeConnection();
      } catch ( Exception e ) {
        throw new KettleException( e );
      }
    }
  }
}
