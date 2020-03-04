/*! ******************************************************************************
 *
 * Pentaho Data Integration
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

package org.pentaho.di.trans.steps.cassandrasstableoutput.writer;

import com.google.common.base.Joiner;
import org.apache.cassandra.config.CFMetaData;
import org.apache.cassandra.config.Schema;
import org.apache.cassandra.db.marshal.UTF8Type;
import org.apache.cassandra.dht.ByteOrderedPartitioner;
import org.apache.cassandra.dht.IPartitioner;
import org.apache.cassandra.dht.Murmur3Partitioner;
import org.apache.cassandra.dht.OrderPreservingPartitioner;
import org.apache.cassandra.dht.RandomPartitioner;
import org.apache.cassandra.io.sstable.CQLSSTableWriter;
import org.pentaho.di.core.row.RowMetaInterface;
import org.pentaho.di.core.row.ValueMetaInterface;

import java.util.Arrays;
import java.util.Map;

class CQL3SSTableWriter extends AbstractSSTableWriter {
  private CQLSSTableWriter writer;
  private RowMetaInterface rowMeta;

  @Override
  public void init() throws Exception {
    //Allow table to be reloaded
    purgeSchemaInstance();
    writer = getCQLSSTableWriter();
  }

  void purgeSchemaInstance() {
    // Since the unload function only cares about the keyspace and table name,
    // the partition key and class don't matter (however, creating the CFMetaData
    // will fail unless something is passed in
    CFMetaData cfm = CFMetaData.Builder.create( getKeyspace(), getTable() ).withPartitioner(
      getPartitionerClassInstance( getPartitionerClass() ) ).addPartitionKey(
      getPartitionKey(), UTF8Type.instance ).build();
    Schema.instance.unload( cfm );
  }

  CQLSSTableWriter getCQLSSTableWriter() {
    return CQLSSTableWriter.builder().inDirectory( getDirectory() ).forTable( buildCreateTableCQLStatement() )
      .using( buildInsertCQLStatement() ).withBufferSizeInMB( getBufferSize() ).build();
  }

  @Override
  public void processRow( Map<String, Object> record ) throws Exception {
    writer.addRow( record );
  }

  @Override
  public void close() throws Exception {
    if ( writer != null ) {
      writer.close();
    }
  }

  public void setRowMeta( RowMetaInterface rowMeta ) {
    this.rowMeta = rowMeta;
  }

  String buildCreateTableCQLStatement() {
    StringBuilder tableColumnsSpecification = new StringBuilder();
    for ( ValueMetaInterface valueMeta : rowMeta.getValueMetaList() ) {
      tableColumnsSpecification.append( cql3MixedCaseQuote( valueMeta.getName() ) ).append( " " )
        .append( getCQLTypeForValueMeta( valueMeta ) ).append( "," );
    }

    tableColumnsSpecification.append( "PRIMARY KEY (\"" ).append( getPrimaryKey().replaceAll( ",", "\",\"" ) ).append(
      "\" )" );

    return String.format( "CREATE TABLE %s.%s (%s);", getKeyspace(), getTable(), tableColumnsSpecification );
  }

  String buildInsertCQLStatement() {
    Joiner columnsJoiner = Joiner.on( "\",\"" ).skipNulls();
    Joiner valuesJoiner = Joiner.on( "," ).skipNulls();
    String[] columnNames = rowMeta.getFieldNames();
    String[] valuePlaceholders = new String[columnNames.length];
    Arrays.fill( valuePlaceholders, "?" );
    return String.format( "INSERT INTO %s.%s (\"%s\") VALUES (%s);", getKeyspace(), getTable(), columnsJoiner
      .join( columnNames ), valuesJoiner.join( valuePlaceholders ) );
  }

  /**
   * Return the Cassandra CQL column/key type for the given Kettle column. We use this type for CQL create table
   * statements since, for some reason, the internal type isn't recognized for the key. Internal types *are* recognized
   * for column definitions. The CQL reference guide states that fully qualified (or relative to
   * org.apache.cassandra.db.marshal) class names can be used instead of CQL types - however, using these when defining
   * the key type always results in BytesType getting set for the key for some reason.
   *
   * @param vm the ValueMetaInterface for the Kettle column
   * @return the corresponding CQL type
   */
  public static String getCQLTypeForValueMeta( ValueMetaInterface vm ) {
    switch ( vm.getType() ) {
      case ValueMetaInterface.TYPE_STRING:
        return "varchar"; //$NON-NLS-1$
      case ValueMetaInterface.TYPE_BIGNUMBER:
        return "decimal"; //$NON-NLS-1$
      case ValueMetaInterface.TYPE_BOOLEAN:
        return "boolean"; //$NON-NLS-1$
      case ValueMetaInterface.TYPE_INTEGER:
        return "bigint"; //$NON-NLS-1$
      case ValueMetaInterface.TYPE_NUMBER:
        return "double"; //$NON-NLS-1$
      case ValueMetaInterface.TYPE_DATE:
      case ValueMetaInterface.TYPE_TIMESTAMP:
        return "timestamp"; //$NON-NLS-1$
      case ValueMetaInterface.TYPE_BINARY:
      case ValueMetaInterface.TYPE_SERIALIZABLE:
        return "blob"; //$NON-NLS-1$
    }

    return "blob"; //$NON-NLS-1$
  }

  /**
   * Quotes an identifier (for CQL 3) if it contains mixed case
   *
   * @param source the source string
   * @return the quoted string
   */
  public static String cql3MixedCaseQuote( String source ) {
    if ( source.toLowerCase().equals( source ) || ( source.startsWith( "\"" ) && source.endsWith( "\"" ) ) ) {
      // no need for quotes
      return source;
    }

    return "\"" + source + "\"";
  }

  /**
   * Translates string literal to partitioner class instance
   *
   * @param partitionerClass the string name of the partitioner class
   * @return the partitioner class instance
   */
  public static IPartitioner getPartitionerClassInstance( String partitionerClass ) {
    switch ( partitionerClass ) {
      case "org.apache.cassandra.dht.Murmur3Partitioner":
        return Murmur3Partitioner.instance;
      case "org.apache.cassandra.dht.ByteOrderedPartitioner":
        return ByteOrderedPartitioner.instance;
      case "org.apache.cassandra.dht.RandomPartitioner":
        return RandomPartitioner.instance;
      case "org.apache.cassandra.dht.OrderPreservingPartitioner":
        return OrderPreservingPartitioner.instance;
      default:
        return null;
    }
  }
}
