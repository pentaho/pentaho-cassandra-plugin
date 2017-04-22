/*! ******************************************************************************
 *
 * Pentaho Data Integration
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

package org.pentaho.di.trans.steps.cassandrasstableoutput.writer;

import java.util.Map;

public abstract class AbstractSSTableWriter {
  private static final int DEFAULT_BUFFER_SIZE_MB = 16;
  private int bufferSize = DEFAULT_BUFFER_SIZE_MB;
  private String directory = System.getProperty( "java.io.tmpdir" );
  private String keyspace;
  private String columnFamily;
  private String keyField;

  public abstract void init() throws Exception;

  public abstract void processRow( Map<String, Object> record ) throws Exception;

  public abstract void close() throws Exception;

  protected String getDirectory() {
    return directory;
  }

  /**
   * Set the directory to read the sstables from
   *
   * @param directory
   *          the directory to read the sstables from
   */
  public void setDirectory( String directory ) {
    this.directory = directory;
  }

  protected String getKeyspace() {
    return keyspace;
  }

  /**
   * Set the target keyspace
   *
   * @param keyspace
   *          the keyspace to use
   */
  public void setKeyspace( String keyspace ) {
    this.keyspace = keyspace;
  }

  protected String getColumnFamily() {
    return columnFamily;
  }

  /**
   * Set the column family (table) to load to. Note: it is assumed that this column family exists in the keyspace
   * apriori.
   *
   * @param columnFamily
   *          the column family to load to.
   */
  public void setColumnFamily( String columnFamily ) {
    this.columnFamily = columnFamily;
  }

  protected int getBufferSize() {
    return bufferSize;
  }

  /**
   * Set the buffer size (Mb) to use. A new table file is written every time the buffer is full.
   *
   * @param bufferSize
   *          the size of the buffer to use
   */
  public void setBufferSize( int bufferSize ) {
    this.bufferSize = bufferSize;
  }

  protected String getKeyField() {
    return keyField;
  }

  public void setKeyField( String keyField ) {
    this.keyField = keyField;
  }
}
