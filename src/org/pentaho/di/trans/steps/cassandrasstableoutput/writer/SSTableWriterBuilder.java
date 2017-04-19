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

import org.apache.cassandra.config.YamlConfigurationLoader;
import org.apache.cassandra.exceptions.ConfigurationException;
import org.pentaho.di.core.row.RowMetaInterface;

/**
 * Builder is used to create specific SSTableWriter depending mostly on CQL version
 *
 * @author Pavel Sakun
 */
public class SSTableWriterBuilder {
  /**
   * Path to cassandra YAML config
   */
  private String configFilePath;

  /**
   * CQL Version
   */
  private int cqlVersion;

  /**
   * The directory to output to
   */
  private String directory;

  /**
   * The keyspace to use
   */
  private String keyspace;

  /**
   * The name of the column family (table) to write to
   */
  private String columnFamily;

  /**
   * The key field used to determine unique keys (IDs) for rows
   */
  private String keyField;

  /**
   * Size (MB) of write buffer
   */
  private int bufferSize;

  /**
   * Input row meta
   */
  private RowMetaInterface rowMeta;

  public SSTableWriterBuilder withConfig( String configFilePath ) {
    if ( !configFilePath.startsWith( "file:" ) ) {
      this.configFilePath = "file:" + configFilePath;
    } else {
      this.configFilePath = configFilePath;
    }
    return this;
  }

  public SSTableWriterBuilder withDirectory( String outputDirectoryPath ) {
    this.directory = outputDirectoryPath;
    return this;
  }

  public SSTableWriterBuilder withKeyspace( String keyspaceName ) {
    this.keyspace = keyspaceName;
    return this;
  }

  public SSTableWriterBuilder withColumnFamily( String columnFamilyName ) {
    this.columnFamily = columnFamilyName;
    return this;
  }

  public SSTableWriterBuilder withKeyField( String keyField ) {
    this.keyField = keyField;
    return this;
  }

  public SSTableWriterBuilder withBufferSize( int bufferSize ) {
    this.bufferSize = bufferSize;
    return this;
  }

  public SSTableWriterBuilder withRowMeta( RowMetaInterface rowMeta ) {
    this.rowMeta = rowMeta;
    return this;
  }

  public SSTableWriterBuilder withCqlVersion( int cqlVersion ) {
    this.cqlVersion = cqlVersion;
    return this;
  }

  public AbstractSSTableWriter build() throws Exception {
    System.setProperty( "cassandra.config", configFilePath );
    AbstractSSTableWriter result;

    if ( cqlVersion == 3 ) {
      CQL3SSTableWriter writer = getCql3SSTableWriter();

      writer.setRowMeta( rowMeta );

      result = writer;
    } else {
      CQL2SSTableWriter writer = getCql2SSTableWriter();

      writer.setPartitionerClassName( getPartitionerClass() );

      result = writer;
    }
    result.setDirectory( directory );
    result.setKeyspace( keyspace );
    result.setColumnFamily( columnFamily );
    result.setKeyField( keyField );
    result.setBufferSize( bufferSize );

    return result;
  }

  String getPartitionerClass() throws ConfigurationException {
    return new YamlConfigurationLoader().loadConfig().partitioner;
  }

  CQL2SSTableWriter getCql2SSTableWriter() {
    return new CQL2SSTableWriter();
  }

  CQL3SSTableWriter getCql3SSTableWriter() {
    return new CQL3SSTableWriter();
  }
}
