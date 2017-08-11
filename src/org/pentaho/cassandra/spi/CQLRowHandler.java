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

package org.pentaho.cassandra.spi;

import java.util.Map;

import org.pentaho.di.core.logging.LogChannelInterface;
import org.pentaho.di.core.row.RowMetaInterface;
import org.pentaho.di.trans.step.StepInterface;

/**
 * Interface to something that can process rows (read and write) via CQL.
 * 
 * @author Mark Hall (mhall{[at]}pentaho{[dot]}com)
 */
public interface CQLRowHandler {

  /**
   * Returns true if the underlying driver supports the provided CQL major version number (e.g. 2 or 3).
   * 
   * @param cqMajorlVersion
   *          the major version number of the CQL version to check
   * @return true if the underlying driver supports this major version of CQL
   */
  boolean supportsCQLVersion( int cqMajorlVersion );

  /**
   * Set any special options for (e.g. timeouts etc)
   * 
   * @param options
   *          the options to use. Can be null.
   */
  void setOptions( Map<String, String> options );

  /**
   * Set the specific underlying Keyspace implementation to use
   * 
   * @param keyspace
   *          the keyspace to use
   */
  void setKeyspace( Keyspace keyspace );

  /**
   * Commit a batch CQL statement via the underlying driver
   * 
   * @param requestingStep
   *          the step that is requesting the commit - clients can use this primarily to check whether the running
   *          transformation has been paused or stopped (via isPaused() and isStopped())
   * @param batch
   *          the CQL batch insert statement
   * @param compress
   *          name of the compression to use (may be null for no compression)
   * @param consistencyLevel
   *          the consistency level to use
   * @param log
   *          the log to use
   * @throws Exception
   *           if a problem occurs
   */
  void commitCQLBatch( StepInterface requestingStep, StringBuilder batch, String compress, String consistencyLevel,
      LogChannelInterface log ) throws Exception;

  /**
   * Add a row to a CQL batch. Clients can implement this if the static utility method by the same name in
   * CassandraUtils is not sufficient.
   * 
   * @param batch
   *          the batch to add to
   * @param colFamilyName
   *          the name of the column family that the batch insert applies to
   * @param inputMeta
   *          the structure of the incoming Kettle rows inserting
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
   * @deprecated
   */
  @Deprecated
  boolean addRowToCQLBatch( StringBuilder batch, String colFamilyName, RowMetaInterface inputMeta, Object[] row,
      boolean insertFieldsNotInMetaData, LogChannelInterface log ) throws Exception;

  /**
   * Executes a new CQL query and initializes ready for iteration over the results. Closes/discards any previous query.
   * 
   * @param requestingStep
   *          the step that is requesting rows - clients can use this primarily to check whether the running
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
  void newRowQuery( StepInterface requestingStep, String colFamName, String cqlQuery, String compress,
      String consistencyLevel, boolean outputTuples, LogChannelInterface log ) throws Exception;

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
  Object[][] getNextOutputRow( RowMetaInterface outputRowMeta, Map<String, Integer> outputFormatMap ) throws Exception;

}
