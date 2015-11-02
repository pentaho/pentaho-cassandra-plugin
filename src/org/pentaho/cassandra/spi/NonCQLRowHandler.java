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

package org.pentaho.cassandra.spi;

import java.util.List;
import java.util.Map;

import org.pentaho.di.core.logging.LogChannelInterface;
import org.pentaho.di.core.row.RowMetaInterface;
import org.pentaho.di.trans.step.StepInterface;

/**
 * Implementation of NonCQLRowHandler that wraps the legacy Thrift-based
 * implementation
 * 
 * @author Mark Hall (mhall{[at]}pentaho{[dot]}com)
 */
public interface NonCQLRowHandler {

  /**
   * Set any special options for (e.g. timeouts etc)
   * 
   * @param options the options to use. Can be null.
   */
  void setOptions( Map<String, String> options );

  /**
   * Set the specific underlying Keyspace implementation to use
   * 
   * @param keyspace the keyspace to use
   */
  void setKeyspace( Keyspace keyspace );

  /**
   * Initializes a new slice-style row query and prepares for iteration over the rows and columns.
   * 
   * @param requestingStep
   *          the step that is requesting the rows - clients can use this primarily to check whether the running
   *          transformation has been paused or stopped (via isPaused() and isStopped())
   * @param colFamilyName
   *          the column family to query
   * @param colNames
   *          the names of the columns that the user is interested in (may be null for all columns)
   * @param rowLimit
   *          the maximum number of rows to return (-1 for no limit)
   * @param colLimit
   *          the maximum number of columns to return (-1 for no limit)
   * @param rowBatchSize
   *          the number of rows to fetch in one go from the server
   * @param colBatchSize
   *          the number of columns to fetch in one go from the server
   * @param consistencyLevel
   *          the consistency level to use
   * @param log
   *          the log to use
   * @throws Exception
   *           if a problem occurs
   */
  void newRowQuery( StepInterface requestingStep, String colFamilyName, List<String> colNames, int rowLimit,
      int colLimit, int rowBatchSize, int colBatchSize, String consistencyLevel, LogChannelInterface log )
    throws Exception;

  /**
   * Return a row (or null if no more data) from the current query. The row returned is in "tuple" format - i.e. <key,
   * column name, column value, time stamp>
   * 
   * @param outputRowMeta
   * @return
   * @throws Exception
   *           if something goes wrong
   */
  Object[] getNextOutputRow( RowMetaInterface outputRowMeta ) throws Exception;

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
  void commitNonCQLBatch( StepInterface requestingStep, List<Object[]> batch, RowMetaInterface rowMeta, int keyIndex,
      String colFamName, String consistencyLevel, LogChannelInterface log ) throws Exception;
}
