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

import java.util.Arrays;
import java.util.Map;

import org.apache.cassandra.config.CFMetaData;
import org.apache.cassandra.config.Schema;
import org.apache.cassandra.db.marshal.UTF8Type;
import org.apache.cassandra.io.sstable.CQLSSTableWriter;
import org.pentaho.cassandra.util.CassandraUtils;
import org.pentaho.di.core.row.RowMetaInterface;
import org.pentaho.di.core.row.ValueMetaInterface;

import com.google.common.base.Joiner;

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
        CassandraUtils.getPartitionerClassInstance( getPartitionerClass() ) ).addPartitionKey(
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
      tableColumnsSpecification.append( CassandraUtils.cql3MixedCaseQuote( valueMeta.getName() ) ).append( " " )
          .append( CassandraUtils.getCQLTypeForValueMeta( valueMeta ) ).append( "," );
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
}
