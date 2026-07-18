/*! ******************************************************************************
 *
 * Pentaho
 *
 * Copyright (C) 2018 - 2026 by Pentaho Canada Inc. : http://www.pentaho.com
 *
 * Use of this software is governed by the Business Source License included
 * in the LICENSE.TXT file.
 *
 * Change Date: 2030-06-15
 ******************************************************************************/


package org.pentaho.di.trans.steps.cassandrasstableoutput.writer;

import org.junit.Test;

import java.util.Map;

import static org.junit.Assert.assertEquals;

public class AbstractSSTableWriterTest extends AbstractSSTableWriter {

  @Test
  public void testGetDirectory() throws Exception {
    AbstractSSTableWriter writer = new AbstractSSTableWriterTest();
    assertEquals( System.getProperty( "java.io.tmpdir" ), writer.getDirectory() );
    writer.setDirectory( "some_dir" );
    assertEquals( "some_dir", writer.getDirectory() );
  }

  @Test
  public void testGetKeyspace() throws Exception {
    AbstractSSTableWriter writer = new AbstractSSTableWriterTest();
    assertEquals( null, writer.getKeyspace() );
    writer.setKeyspace( "some_keyspace" );
    assertEquals( "some_keyspace", writer.getKeyspace() );
  }

  @Test
  public void testGetTable() throws Exception {
    AbstractSSTableWriter writer = new AbstractSSTableWriterTest();
    assertEquals( null, writer.getTable() );
    writer.setTable( "some_table" );
    assertEquals( "some_table", writer.getTable() );
  }

  @Test
  public void testGetBufferSize() throws Exception {
    AbstractSSTableWriter writer = new AbstractSSTableWriterTest();
    assertEquals( 16, writer.getBufferSize() );
    writer.setBufferSize( 10 );
    assertEquals( 10, writer.getBufferSize() );
  }

  @Test
  public void testGetKeyField() throws Exception {
    AbstractSSTableWriter writer = new AbstractSSTableWriterTest();
    assertEquals( null, writer.getPrimaryKey() );
    writer.setPrimaryKey( "some_keyField" );
    assertEquals( "some_keyField", writer.getPrimaryKey() );
  }

  @Override public void init() throws Exception {

  }

  @Override public void processRow( Map<String, Object> record ) throws Exception {

  }

  @Override public void close() throws Exception {

  }
}
