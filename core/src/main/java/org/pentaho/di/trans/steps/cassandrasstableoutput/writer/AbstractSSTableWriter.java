/*! ******************************************************************************
 *
 * Pentaho
 *
 * Copyright (C) 2002 - 2026 by Pentaho Canada Inc. : http://www.pentaho.com
 *
 * Use of this software is governed by the Business Source License included
 * in the LICENSE.TXT file.
 *
 * Change Date: 2030-06-15
 ******************************************************************************/


package org.pentaho.di.trans.steps.cassandrasstableoutput.writer;

import org.pentaho.cassandra.util.CassandraUtils;

import java.util.Map;

public abstract class AbstractSSTableWriter {
  private static final int DEFAULT_BUFFER_SIZE_MB = 16;
  private int bufferSize = DEFAULT_BUFFER_SIZE_MB;
  private String directory = System.getProperty( "java.io.tmpdir" );
  private String keyspace;
  private String table;
  private String primaryKey;
  private String partitionerClass;

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

  protected String getTable() {
    return table;
  }

  /**
   * Set the table to load to. Note: it is assumed that this table exists in the keyspace
   * apriori.
   *
   * @param table
   *          the table to load to.
   */
  public void setTable( String table ) {
    this.table = table;
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

  protected String getPrimaryKey() {
    return primaryKey;
  }

  public void setPrimaryKey( String primaryKey ) {
    this.primaryKey = primaryKey;
  }

  protected String getPartitionerClass( ) {
    return partitionerClass;
  }

  public void setPartitionerClass( String partitionerClass ) {
    this.partitionerClass = partitionerClass;
  }

  protected String getPartitionKey( ) {
    return CassandraUtils.getPartitionKey( primaryKey );
  }
}
