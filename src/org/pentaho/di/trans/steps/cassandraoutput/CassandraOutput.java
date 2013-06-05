/*******************************************************************************
 *
 * Pentaho Big Data
 *
 * Copyright (C) 2002-2012 by Pentaho : http://www.pentaho.com
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

import java.util.ArrayList;
import java.util.HashMap;
import java.util.List;
import java.util.Map;

import org.pentaho.cassandra.CassandraUtils;
import org.pentaho.cassandra.ConnectionFactory;
import org.pentaho.cassandra.spi.CQLRowHandler;
import org.pentaho.cassandra.spi.ColumnFamilyMetaData;
import org.pentaho.cassandra.spi.Connection;
import org.pentaho.cassandra.spi.Keyspace;
import org.pentaho.cassandra.spi.NonCQLRowHandler;
import org.pentaho.di.core.Const;
import org.pentaho.di.core.exception.KettleException;
import org.pentaho.di.i18n.BaseMessages;
import org.pentaho.di.trans.Trans;
import org.pentaho.di.trans.TransMeta;
import org.pentaho.di.trans.step.BaseStep;
import org.pentaho.di.trans.step.StepDataInterface;
import org.pentaho.di.trans.step.StepInterface;
import org.pentaho.di.trans.step.StepMeta;
import org.pentaho.di.trans.step.StepMetaInterface;

/**
 * Class providing an output step for writing data to a cassandra table (column
 * family). Can create the specified column family (if it doesn't already exist)
 * and can update column family meta data.
 * 
 * @author Mark Hall (mhall{[at]}pentaho{[dot]}com)
 */
public class CassandraOutput extends BaseStep implements StepInterface {

  protected CassandraOutputMeta m_meta;
  protected CassandraOutputData m_data;

  public CassandraOutput(StepMeta stepMeta,
      StepDataInterface stepDataInterface, int copyNr, TransMeta transMeta,
      Trans trans) {

    super(stepMeta, stepDataInterface, copyNr, transMeta, trans);
  }

  protected Connection m_connection;

  protected Keyspace m_keyspace;

  protected CQLRowHandler m_cqlHandler = null;
  protected NonCQLRowHandler m_nonCqlHandler = null;

  /** Column meta data and schema information */
  protected ColumnFamilyMetaData m_cassandraMeta;

  /** Holds batch mutate for Thrift-based IO */
  protected List<Object[]> m_nonCQLBatch;

  /** Holds batch insert CQL statement */
  protected StringBuilder m_batchInsertCQL;

  /** Current batch of rows to insert */
  protected List<Object[]> m_batch;

  /** The number of rows seen so far for this batch */
  protected int m_rowsSeen;

  /** The batch size to use */
  protected int m_batchSize = 100;

  /** The consistency to use - null means to use the cassandra default */
  protected String m_consistency = null;

  /** The name of the column family (table) to write to */
  protected String m_columnFamilyName;

  /** The name of the keyspace */
  protected String m_keyspaceName;

  /** The index of the key field in the incoming rows */
  protected List<Integer> m_keyIndexes = null;

  protected int m_cqlBatchInsertTimeout = 0;

  /** Default batch split factor */
  protected int m_batchSplitFactor = 10;

  /** Whether to use Thrift for IO or not */
  protected boolean m_useThriftIO;

  protected String m_consistencyLevel;

  protected void initialize(StepMetaInterface smi, StepDataInterface sdi)
      throws KettleException {

    m_meta = (CassandraOutputMeta) smi;
    m_data = (CassandraOutputData) sdi;

    first = false;
    m_rowsSeen = 0;

    // Get the connection to Cassandra
    String hostS = environmentSubstitute(m_meta.getCassandraHost());
    String portS = environmentSubstitute(m_meta.getCassandraPort());
    String userS = m_meta.getUsername();
    String passS = m_meta.getPassword();
    String batchTimeoutS = environmentSubstitute(m_meta
        .getCQLBatchInsertTimeout());
    String batchSplitFactor = environmentSubstitute(m_meta.getCQLSubBatchSize());
    String schemaHostS = environmentSubstitute(m_meta.getSchemaHost());
    String schemaPortS = environmentSubstitute(m_meta.getSchemaPort());
    if (Const.isEmpty(schemaHostS)) {
      schemaHostS = hostS;
    }
    if (Const.isEmpty(schemaPortS)) {
      schemaPortS = portS;
    }

    if (!Const.isEmpty(userS) && !Const.isEmpty(passS)) {
      userS = environmentSubstitute(userS);
      passS = environmentSubstitute(passS);
    }
    m_keyspaceName = environmentSubstitute(m_meta.getCassandraKeyspace());
    m_columnFamilyName = environmentSubstitute(m_meta.getColumnFamilyName());
    m_consistencyLevel = environmentSubstitute(m_meta.getConsistency());

    String keyField = environmentSubstitute(m_meta.getKeyField());

    try {

      if (!Const.isEmpty(batchTimeoutS)) {
        try {
          m_cqlBatchInsertTimeout = Integer.parseInt(batchTimeoutS);
          if (m_cqlBatchInsertTimeout < 500) {
            logBasic(BaseMessages.getString(CassandraOutputMeta.PKG,
                "CassandraOutput.Message.MinimumTimeout")); //$NON-NLS-1$
            m_cqlBatchInsertTimeout = 500;
          }
        } catch (NumberFormatException e) {
          logError(BaseMessages.getString(CassandraOutputMeta.PKG,
              "CassandraOutput.Error.CantParseTimeout")); //$NON-NLS-1$
          m_cqlBatchInsertTimeout = 10000;
        }
      }

      if (!Const.isEmpty(batchSplitFactor)) {
        try {
          m_batchSplitFactor = Integer.parseInt(batchSplitFactor);
        } catch (NumberFormatException e) {
          logError(BaseMessages.getString(CassandraOutputMeta.PKG,
              "CassandraOutput.Error.CantParseSubBatchSize")); //$NON-NLS-1$
        }
      }

      if (Const.isEmpty(hostS) || Const.isEmpty(portS)
          || Const.isEmpty(m_keyspaceName)) {
        throw new KettleException(BaseMessages.getString(
            CassandraOutputMeta.PKG,
            "CassandraOutput.Error.MissingConnectionDetails")); //$NON-NLS-1$
      }

      if (Const.isEmpty(m_columnFamilyName)) {
        throw new KettleException(BaseMessages.getString(
            CassandraOutputMeta.PKG,
            "CassandraOutput.Error.NoColumnFamilySpecified")); //$NON-NLS-1$
      }

      if (Const.isEmpty(keyField)) {
        throw new KettleException(BaseMessages.getString(
            CassandraOutputMeta.PKG,
            "CassandraOutput.Error.NoIncomingKeySpecified")); //$NON-NLS-1$
      }

      // check that the specified key field is present in the incoming data
      String[] kparts = keyField.split(","); //$NON-NLS-1$
      if (!m_meta.getUseCQL3() && kparts.length > 1) {
        // TODO messageify
        throw new KettleException(
            "Only one partition key field is supported for CQL 2/Thrift IO"); //$NON-NLS-1$
      }
      m_keyIndexes = new ArrayList<Integer>();
      for (int i = 0; i < kparts.length; i++) {
        int index = getInputRowMeta().indexOfValue(kparts[i].trim());
        if (index < 0) {
          throw new KettleException(BaseMessages.getString(
              CassandraOutputMeta.PKG,
              "CassandraOutput.Error.CantFindKeyField", keyField)); //$NON-NLS-1$
        }
        m_keyIndexes.add(index);
      }

      m_useThriftIO = m_meta.getUseThriftIO();

      logBasic(BaseMessages.getString(CassandraOutputMeta.PKG,
          "CassandraOutput.Message.ConnectingForSchemaOperations", schemaHostS, //$NON-NLS-1$
          schemaPortS, m_keyspaceName));

      Connection connection = null;

      // open up a connection to perform any schema changes
      try {
        connection = openConnection(true);
        Keyspace keyspace = connection.getKeyspace(m_keyspaceName);

        // Try to execute any apriori CQL commands?
        if (!Const.isEmpty(m_meta.getAprioriCQL())) {
          String aprioriCQL = environmentSubstitute(m_meta.getAprioriCQL());
          List<String> statements = CassandraUtils
              .splitCQLStatements(aprioriCQL);

          logBasic(BaseMessages.getString(CassandraOutputMeta.PKG,
              "CassandraOutput.Message.ExecutingAprioriCQL", //$NON-NLS-1$
              m_columnFamilyName, aprioriCQL));

          String compression = m_meta.getUseCompression() ? "gzip" : ""; //$NON-NLS-1$ //$NON-NLS-2$

          for (String cqlS : statements) {
            try {
              keyspace.executeCQL(cqlS, compression, m_consistencyLevel, log);
            } catch (Exception e) {
              if (m_meta.getDontComplainAboutAprioriCQLFailing()) {
                // just log and continue
                logBasic("WARNING: " + e.toString()); //$NON-NLS-1$
              } else {
                throw e;
              }
            }
          }
        }

        if (!keyspace.columnFamilyExists(m_columnFamilyName)) {
          if (m_meta.getCreateColumnFamily()) {
            // create the column family (table)
            boolean result = keyspace.createColumnFamily(m_columnFamilyName,
                getInputRowMeta(), m_keyIndexes,
                environmentSubstitute(m_meta.getCreateTableWithClause()), log);

            if (!result) {
              throw new KettleException(BaseMessages.getString(
                  CassandraOutputMeta.PKG,
                  "CassandraOutput.Error.NeedAtLeastOneFieldAppartFromKey")); //$NON-NLS-1$
            }
          } else {
            throw new KettleException(BaseMessages.getString(
                CassandraOutputMeta.PKG,
                "CassandraOutput.Error.ColumnFamilyDoesNotExist", //$NON-NLS-1$
                m_columnFamilyName, m_keyspaceName));
          }
        }

        if (m_meta.getUpdateCassandraMeta()) {
          // Update cassandra meta data for unknown incoming fields?
          keyspace.updateColumnFamily(m_columnFamilyName, getInputRowMeta(),
              m_keyIndexes, log);
        }

        // get the column family meta data
        logBasic(BaseMessages.getString(CassandraOutputMeta.PKG,
            "CassandraOutput.Message.GettingMetaData", m_columnFamilyName)); //$NON-NLS-1$

        m_cassandraMeta = keyspace.getColumnFamilyMetaData(m_columnFamilyName);

        // check that we have at least one incoming field apart from the key
        // (CQL 2 only)
        if (!m_meta.getUseCQL3()) {
          if (CassandraUtils.numFieldsToBeWritten(getInputRowMeta(),
              m_keyIndexes, m_cassandraMeta, m_meta.getInsertFieldsNotInMeta()) < 2) {
            throw new KettleException(BaseMessages.getString(
                CassandraOutputMeta.PKG,
                "CassandraOutput.Error.NeedAtLeastOneFieldAppartFromKey")); //$NON-NLS-1$
          }
        }

        // output (downstream) is the same as input
        m_data.setOutputRowMeta(getInputRowMeta());

        String batchSize = environmentSubstitute(m_meta.getBatchSize());
        if (!Const.isEmpty(batchSize)) {
          try {
            m_batchSize = Integer.parseInt(batchSize);
          } catch (NumberFormatException e) {
            logError(BaseMessages.getString(CassandraOutputMeta.PKG,
                "CassandraOutput.Error.CantParseBatchSize")); //$NON-NLS-1$
            m_batchSize = 100;
          }
        } else {
          throw new KettleException(BaseMessages.getString(
              CassandraOutputMeta.PKG, "CassandraOutput.Error.NoBatchSizeSet")); //$NON-NLS-1$
        }

        // Truncate (remove all data from) column family first?
        if (m_meta.getTruncateColumnFamily()) {
          keyspace.truncateColumnFamily(m_columnFamilyName, log);
        }
      } finally {
        if (connection != null) {
          closeConnection(connection);
          connection = null;
        }
      }

      m_consistency = environmentSubstitute(m_meta.getConsistency());
      m_batchInsertCQL = CassandraUtils.newCQLBatch(m_batchSize, m_consistency,
          (m_meta.getUseUnloggedBatch() && m_meta.getUseCQL3()));

      m_batch = new ArrayList<Object[]>();

      // now open the main connection to use
      openConnection(false);

    } catch (Exception ex) {
      logError(BaseMessages.getString(CassandraOutputMeta.PKG,
          "CassandraOutput.Error.InitializationProblem"), ex); //$NON-NLS-1$
    }
  }

  @Override
  public boolean processRow(StepMetaInterface smi, StepDataInterface sdi)
      throws KettleException {

    Object[] r = getRow();

    if (r == null) {
      // no more output

      // flush the last batch
      if (m_rowsSeen > 0 && !isStopped()) {
        doBatch();
      }
      m_batchInsertCQL = null;
      m_batch = null;
      m_nonCQLBatch = null;

      closeConnection(m_connection);
      m_connection = null;
      m_keyspace = null;
      m_cqlHandler = null;
      m_nonCqlHandler = null;

      setOutputDone();
      return false;
    }

    if (!isStopped()) {
      if (first) {
        initialize(smi, sdi);
      }

      m_batch.add(r);
      m_rowsSeen++;

      if (m_rowsSeen == m_batchSize) {
        doBatch();
      }
    } else {
      closeConnection(m_connection);
      return false;
    }

    return true;
  }

  protected void doBatch() throws KettleException {

    try {
      doBatch(m_batch);
    } catch (Exception e) {
      logError(BaseMessages.getString(CassandraOutputMeta.PKG,
          "CassandraOutput.Error.CommitFailed", m_batchInsertCQL.toString(), e)); //$NON-NLS-1$
      throw new KettleException(e.fillInStackTrace());
    }

    // ready for a new batch
    m_batch.clear();
    m_rowsSeen = 0;
  }

  protected void doBatch(List<Object[]> batch) throws Exception {
    // stopped?
    if (isStopped()) {
      logDebug(BaseMessages.getString(CassandraOutputMeta.PKG,
          "CassandraOutput.Message.StoppedSkippingBatch")); //$NON-NLS-1$
      return;
    }
    // ignore empty batch
    if (batch == null || batch.isEmpty()) {
      logDebug(BaseMessages.getString(CassandraOutputMeta.PKG,
          "CassandraOutput.Message.SkippingEmptyBatch")); //$NON-NLS-1$
      return;
    }
    // construct CQL/thrift batch and commit
    int size = batch.size();
    try {
      if (m_useThriftIO) {
        m_nonCQLBatch = CassandraUtils.newNonCQLBatch(size);
      } else {
        // construct CQL
        m_batchInsertCQL = CassandraUtils.newCQLBatch(m_batchSize,
            m_consistency,
            (m_meta.getUseUnloggedBatch() && m_meta.getUseCQL3()));
      }
      int rowsAdded = 0;
      for (Object[] r : batch) {
        // add the row to the batch
        if (m_useThriftIO) {
          if (CassandraUtils.addRowToNonCQLBatch(m_nonCQLBatch, r,
              getInputRowMeta(), m_cassandraMeta,
              m_meta.getInsertFieldsNotInMeta(), log)) {
            rowsAdded++;
          }
        } else {
          if (CassandraUtils.addRowToCQLBatch(m_batchInsertCQL,
              m_columnFamilyName, getInputRowMeta(), r, m_cassandraMeta,
              m_meta.getInsertFieldsNotInMeta(), (m_meta.getUseCQL3() ? 3 : 2),
              log)) {
            rowsAdded++;
          }
        }
      }
      if (rowsAdded == 0) {
        logDebug(BaseMessages.getString(CassandraOutputMeta.PKG,
            "CassandraOutput.Message.SkippingEmptyBatch")); //$NON-NLS-1$
        return;
      }

      if (!m_useThriftIO) {
        CassandraUtils.completeCQLBatch(m_batchInsertCQL);
      }

      // commit
      if (m_connection == null) {
        openConnection(false);
      }

      logDetailed(BaseMessages.getString(CassandraOutputMeta.PKG,
          "CassandraOutput.Message.CommittingBatch", m_columnFamilyName, "" //$NON-NLS-1$ //$NON-NLS-2$
              + rowsAdded));

      if (m_useThriftIO) {
        m_nonCqlHandler.commitNonCQLBatch(this, m_nonCQLBatch,
            getInputRowMeta(), m_keyIndexes.get(0), m_columnFamilyName,
            m_consistency, log);
      } else {
        String compress = m_meta.getUseCompression() ? "gzip" : ""; //$NON-NLS-1$ //$NON-NLS-2$
        m_cqlHandler.commitCQLBatch(this, m_batchInsertCQL, compress,
            m_consistencyLevel, log);
      }
    } catch (Exception e) {
      e.printStackTrace();
      closeConnection(m_connection);
      m_connection = null;
      logDetailed(BaseMessages.getString(CassandraOutputMeta.PKG,
          "CassandraOutput.Error.FailedToInsertBatch", "" + size), e); //$NON-NLS-1$ //$NON-NLS-2$

      logDetailed(BaseMessages.getString(CassandraOutputMeta.PKG,
          "CassandraOutput.Message.WillNowTrySplittingIntoSubBatches")); //$NON-NLS-1$

      // is it possible to divide and conquer?
      if (size == 1) {
        // single error row - found it!
        if (getStepMeta().isDoingErrorHandling()) {
          putError(getInputRowMeta(), batch.get(0), 1L, e.getMessage(), null,
              "ERR_INSERT01"); //$NON-NLS-1$
        }
      } else if (size > m_batchSplitFactor) {
        // split into smaller batches and try separately
        List<Object[]> subBatch = new ArrayList<Object[]>();
        while (batch.size() > m_batchSplitFactor) {
          while (subBatch.size() < m_batchSplitFactor && batch.size() > 0) {
            // remove from the right - avoid internal shifting
            subBatch.add(batch.remove(batch.size() - 1));
          }
          doBatch(subBatch);
          subBatch.clear();
        }
        doBatch(batch);
      } else {
        // try each row individually
        List<Object[]> subBatch = new ArrayList<Object[]>();
        while (batch.size() > 0) {
          subBatch.clear();
          // remove from the right - avoid internal shifting
          subBatch.add(batch.remove(batch.size() - 1));
          doBatch(subBatch);
        }
      }
    }
  }

  @Override
  public void setStopped(boolean stopped) {
    if (isStopped() && stopped == true) {
      return;
    }
    super.setStopped(stopped);
  }

  protected Connection openConnection(boolean forSchemaChanges)
      throws KettleException {
    // Get the connection to Cassandra
    String hostS = environmentSubstitute(m_meta.getCassandraHost());
    String portS = environmentSubstitute(m_meta.getCassandraPort());
    String userS = m_meta.getUsername();
    String passS = m_meta.getPassword();
    String timeoutS = environmentSubstitute(m_meta.getSocketTimeout());
    String schemaHostS = environmentSubstitute(m_meta.getSchemaHost());
    String schemaPortS = environmentSubstitute(m_meta.getSchemaPort());
    if (Const.isEmpty(schemaHostS)) {
      schemaHostS = hostS;
    }
    if (Const.isEmpty(schemaPortS)) {
      schemaPortS = portS;
    }

    if (!Const.isEmpty(userS) && !Const.isEmpty(passS)) {
      userS = environmentSubstitute(userS);
      passS = environmentSubstitute(passS);
    }

    Map<String, String> opts = new HashMap<String, String>();
    if (!Const.isEmpty(timeoutS)) {
      opts.put(CassandraUtils.ConnectionOptions.SOCKET_TIMEOUT, timeoutS);
    }

    opts.put(CassandraUtils.BatchOptions.BATCH_TIMEOUT, "" //$NON-NLS-1$
        + m_cqlBatchInsertTimeout);

    // Set CQL version 3 if specified
    if (m_meta.getUseCQL3()) {
      opts.put(CassandraUtils.CQLOptions.CQLVERSION_OPTION,
          CassandraUtils.CQLOptions.CQL3_STRING);
    }

    if (opts.size() > 0) {
      logBasic(BaseMessages.getString(CassandraOutputMeta.PKG,
          "CassandraOutput.Message.UsingConnectionOptions", //$NON-NLS-1$
          CassandraUtils.optionsToString(opts)));
    }

    Connection connection = null;

    try {

      String actualHostToUse = forSchemaChanges ? schemaHostS : hostS;

      connection = CassandraUtils.getCassandraConnection(actualHostToUse,
          Integer.parseInt(portS), userS, passS,
          ConnectionFactory.Driver.LEGACY_THRIFT, opts);

      // set the global connection only if this connection is not being used
      // just for schema changes
      if (!forSchemaChanges) {
        m_connection = connection;
        m_keyspace = m_connection.getKeyspace(m_keyspaceName);
        if (m_useThriftIO) {
          m_nonCqlHandler = m_keyspace.getNonCQLRowHandler();
        }
        m_cqlHandler = m_keyspace.getCQLRowHandler();
      }
    } catch (Exception ex) {
      closeConnection(connection);
      throw new KettleException(ex.getMessage(), ex);
    }

    return connection;
  }

  @Override
  public void dispose(StepMetaInterface smi, StepDataInterface sdi) {
    try {
      closeConnection(m_connection);
    } catch (KettleException e) {
      e.printStackTrace();
    }

    super.dispose(smi, sdi);
  }

  protected void closeConnection(Connection conn) throws KettleException {
    if (conn != null) {
      logBasic(BaseMessages.getString(CassandraOutputMeta.PKG,
          "CassandraOutput.Message.ClosingConnection")); //$NON-NLS-1$
      try {
        conn.closeConnection();
      } catch (Exception e) {
        throw new KettleException(e);
      }
    }
  }
}
