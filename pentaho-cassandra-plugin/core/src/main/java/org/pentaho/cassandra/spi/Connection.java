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

package org.pentaho.cassandra.spi;

import com.datastax.oss.driver.api.core.CqlSession;

import java.util.Map;

/**
 * Interface for something that implements a connection to Cassandra
 * 
 * @author Mark Hall (mhall{[at]}pentaho{[dot]}com)
 */
public interface Connection {

  /**
   * Set seed host(s)
   * 
   * @param hosts comma separated host:[port] pairs
   */
  void setHosts( String hosts );

  /**
   * Default port to use if no port(s) are provided via setHosts()
   * 
   * @param port default port to use for all hosts
   */
  void setDefaultPort( int port );

  /**
   * Set username for authentication
   * 
   * @param username the username to authenticate with
   */
  void setUsername( String username );

  /**
   * Set password for authentication
   * 
   * @param password the password to authenticate with
   */
  void setPassword( String password );

  /**
   * Map of additional options.
   * 
   * @param opts additional options to pass to the underlying connection
   */
  void setAdditionalOptions( Map<String, String> opts );

  /**
   * Get any additional options
   * 
   * @return additional options
   */
  Map<String, String> getAdditionalOptions();

  /**
   * Open the connection (if necessary)
   */
  CqlSession openConnection() throws Exception;

  /**
   * Opens a connection for a particular keyspace
   * @param keyspace
   * @throws Exception
   */
  CqlSession openConnection( String keyspace ) throws Exception;

  /**
   * Close the connection
   */
  void closeConnection() throws Exception;

  /**
   * Get the underlying concrete implementation (in case the implementer
   * delegates)
   * 
   * @return the underlying concrete implementation
   */
  Object getUnderlyingConnection();

  /**
   * Returns a concrete implementation of the Keyspace for the driver in
   * question
   * 
   * @param keyspacename the name of the keyspace to use
   * @return a Keyspace implementation
   * @throws Exception if a problem occurs
   */
  Keyspace getKeyspace( String keyspacename ) throws Exception;
}
