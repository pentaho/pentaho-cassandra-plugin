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

package org.pentaho.di.trans.steps.cassandrasstableoutput.writer;

/*
 * Adapted from DataStax DataImportExample
 * http://www.datastax.com/wp-content/uploads/2011/08/DataImportExample.java
 *
 * Original Disclaimer:
 * This file is an example on how to use the Cassandra SSTableSimpleUnsortedWriter class to create
 * sstables from a csv input file.
 * While this has been tested to work, this program is provided "as is" with no guarantee. Moreover,
 * it's primary aim is toward simplicity rather than completness. In partical, don't use this as an
 * example to parse csv files at home.
 *
 */

import org.apache.cassandra.db.marshal.AsciiType;
import org.apache.cassandra.dht.IPartitioner;
import org.apache.cassandra.dht.Murmur3Partitioner;
import org.apache.cassandra.io.sstable.SSTableSimpleUnsortedWriter;
import org.apache.cassandra.utils.ByteBufferUtil;
import org.pentaho.di.core.Const;
import org.pentaho.di.core.exception.KettleException;
import org.pentaho.di.i18n.BaseMessages;

import java.io.File;
import java.nio.ByteBuffer;
import java.util.Date;
import java.util.Map;
import java.util.Map.Entry;

import static org.apache.cassandra.utils.ByteBufferUtil.bytes;

/**
 * Outputs Cassandra SSTables (sorted-string tables) to a directory.
 *
 * Adapted from DataStax DataImportExample http://www.datastax.com/wp-content/uploads/2011/08/DataImportExample.java
 *
 * @author Rob Turner (robert{[at]}robertturner{[dot]}com{[dot]}au)
 */
class CQL2SSTableWriter extends AbstractSSTableWriter {
  private String keyField;
  private String partitionerClassName;
  private SSTableSimpleUnsortedWriter writer;
  protected static final Class<?> PKG = CQL2SSTableWriter.class;

  /**
   * Initialization. Creates target directory if needed and establishes the writer
   *
   * @throws Exception
   *           if a problem occurs
   */
  public void init() throws Exception {
    try {
      Class partitionerClass = Murmur3Partitioner.class;
      if ( !Const.isEmpty( partitionerClassName ) ) {
        try {
          partitionerClass = Class.forName( partitionerClassName );
        } catch ( ClassNotFoundException cnfe ) {
          // Use default partitioner
        }
      }

      writer =
        getSsTableSimpleUnsortedWriter( new File( getDirectory() ), partitionerClass, getKeyspace(), getColumnFamily(),
          getBufferSize() );
    } catch ( Throwable t ) {
      throw new KettleException( BaseMessages.getString( PKG, "SSTableOutput.Error.WriterCreation" ), t );
    }
  }

  SSTableSimpleUnsortedWriter getSsTableSimpleUnsortedWriter( File file, Class partitionerClass, String keyspace,
                                                              String columnFamily, int bufferSize )
    throws InstantiationException, IllegalAccessException {
    return new SSTableSimpleUnsortedWriter( file, (IPartitioner) partitionerClass.newInstance(),
      keyspace, columnFamily, AsciiType.instance, null, bufferSize );
  }

  /**
   * Process a row of data
   *
   * @param record
   *          a row of data as a Map of column names to values
   * @throws Exception
   *           if a problem occurs
   */
  public void processRow( Map<String, Object> record ) throws Exception {
    // get UUID
    ByteBuffer uuid = valueToBytes( record.get( keyField ) );
    // write record
    writer.newRow( uuid );
    long timestamp = System.currentTimeMillis() * 1000;
    for ( Entry<String, Object> entry : record.entrySet() ) {
      // don't write the key as a column!
      if ( !entry.getKey().equals( keyField ) ) {
        writer.addColumn( bytes( entry.getKey() ), valueToBytes( entry.getValue() ), timestamp );
      }
    }
  }

  /**
   * Close the writer
   *
   * @throws Exception
   *           if a problem occurs
   */
  public void close() throws Exception {
    if ( writer != null ) {
      writer.close();
    }
  }

  /**
   * Set the key field name
   *
   * @param keyField
   *          the key field name
   */
  public void setKeyField( String keyField ) {
    this.keyField = keyField;
  }

  /**
   * Set paritioner class name to be used.
   *
   * @param partitionerClassName
   *          class name
   */
  public void setPartitionerClassName( String partitionerClassName ) {
    this.partitionerClassName = partitionerClassName;
  }

  static ByteBuffer valueToBytes( Object val ) throws Exception {
    if ( val == null ) {
      return ByteBufferUtil.EMPTY_BYTE_BUFFER;
    }
    if ( val instanceof String ) {
      return bytes( (String) val );
    }
    if ( val instanceof Integer ) {
      return bytes( (Integer) val );
    }
    if ( val instanceof Float ) {
      return bytes( (Float) val );
    }
    if ( val instanceof Boolean ) {
      // will return "true" or "false"
      return bytes( val.toString() );
    }
    if ( val instanceof Date ) {
      return bytes( ( (Date) val ).getTime() );
    }
    if ( val instanceof Long ) {
      return bytes( ( (Long) val ).longValue() );
    }
    if ( val instanceof Double ) {
      return bytes( (Double) val );
    }

    if ( val instanceof byte[] ) {
      return ByteBuffer.wrap( (byte[]) val );
    }

    // reduce to string
    return bytes( val.toString() );
  }
}
