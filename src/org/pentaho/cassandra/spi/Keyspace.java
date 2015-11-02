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

/**
 * Interface to something that can represent a keyspace in Cassandra
 * 
 * @author Mark Hall (mhall{[at]}pentaho{[dot]}com)
 * 
 */
public interface Keyspace {

  /**
   * Set the connection for this keyspace to use
   * 
   * @param conn the connection to use
   * @throws Exception if a problem occurs
   */
  void setConnection( Connection conn ) throws Exception;

  /**
   * Get the current connection
   * 
   * @return the current connection
   */
  Connection getConnection();

  /**
   * Set the current keyspace
   * 
   * @param keyspaceName the name of the keyspace to use
   * @throws Exception if a problem occurs
   */
  void setKeyspace( String keyspaceName ) throws Exception;

  /**
   * Set any special options for keyspace operations (e.g. timeouts etc.)
   * 
   * @param options the options to use. Can be null.
   */
  void setOptions( Map<String, String> options );

  /**
   * Execute a CQL statement.
   * 
   * @param cql
   *          the CQL to execute
   * @param compression
   *          the compression to use (GZIP or NONE)
   * @param consistencyLevel
   *          the consistency level to use
   * @param log
   *          log to write to (may be null)
   * @throws UnsupportedOperationException
   *           if CQL is not supported by the underlying driver
   * @throws Exception
   *           if a problem occurs
   */
  void executeCQL( String cql, String compresson, String consistencyLevel, LogChannelInterface log )
    throws UnsupportedOperationException, Exception;

  /**
   * Create a keyspace.
   * 
   * @param keyspaceName
   *          the name of the keyspace
   * @param options
   *          additional options (see http://www.datastax.com/docs/1.0/configuration /storage_configuration)
   * @param log
   *          log to write to (may be null)
   * @throws UnsupportedOperationException
   *           if the underlying driver does not support creating keyspaces
   * @throws Exception
   *           if a problem occurs
   */
  void createKeyspace( String keyspaceName, Map<String, Object> options, LogChannelInterface log )
    throws UnsupportedOperationException, Exception;

  /**
   * Get a list of the names of the column families in this keyspace
   * 
   * @return a list of column family names in the current keyspace
   * @throws Exception if a problem occurs
   */
  List<String> getColumnFamilyNames() throws Exception;

  /**
   * Check to see if the named column family exists in the current keyspace
   * 
   * @param colFamName
   *          the column family name to check
   * @return true if the named column family exists in the current keyspace
   * @throws Exception
   *           if a problem occurs
   */
  boolean columnFamilyExists( String colFamName ) throws Exception;

  /**
   * Get meta data for the named column family
   * 
   * @param familyName
   *          the name of the column family to get meta data for
   * @return the column family meta data
   * @throws Exception
   *           if the named column family does not exist in the keyspace or a problem occurs
   */
  ColumnFamilyMetaData getColumnFamilyMetaData( String familyName ) throws Exception;

  /**
   * Create a column family in the current keyspace.
   * 
   * @param colFamName
   *          the name of the column family to create
   * @param rowMeta
   *          the incoming fields to base the column family schema on
   * @param keyIndexes
   *          the index(es) of the incoming field(s) to use as the key
   * @param createTableWithClause
   *          any WITH clause to include when creating the table
   * @param log
   *          log to write to (may be null)
   * @return true if the column family was created successfully
   * @throws Exception
   *           if a problem occurs
   */
  boolean createColumnFamily( String colFamName, RowMetaInterface rowMeta, List<Integer> keyIndexes,
      String createTableWithClause, LogChannelInterface log ) throws Exception;

  /**
   * Update the named column family with any incoming fields that are not present in its schema already
   * 
   * @param colFamName
   *          the name of the column family to update
   * @param rowMeta
   *          the incoming row meta data
   * @param keyIndexes
   *          the index(es) of the incoming field(s) that make up the key
   * @param log
   *          the log to write to (may be null)
   * @throws UnsupportedOperationException
   *           if the underlying driver does not support updating column family schema information
   * @throws Exception
   *           if a problem occurs
   */
  void updateColumnFamily( String colFamName, RowMetaInterface rowMeta, List<Integer> keyIndexes,
      LogChannelInterface log ) throws UnsupportedOperationException, Exception;

  /**
   * Truncate the named column family.
   * 
   * @param colFamName
   *          the name of the column family to truncate
   * @param log
   *          log to write to (may be null)
   * @throws UnsupportedOperationException
   *           if the underlying driver does not support truncating a column family
   * @throws Exception
   *           if a problem occurs
   */
  void truncateColumnFamily( String colFamName, LogChannelInterface log ) throws UnsupportedOperationException,
    Exception;

  /**
   * Get a concrete implementation of CQLRowHandler for the underlying driver
   * 
   * @return a CQLRowHandler or null if the driver in question does not support
   *         CQL
   */
  CQLRowHandler getCQLRowHandler();

  /**
   * Get a non-cql row handler for the underlying driver. May return null if
   * this driver does not handle non CQL based communication
   * 
   * @return a NonCQLRowHandler
   */
  NonCQLRowHandler getNonCQLRowHandler();
}
