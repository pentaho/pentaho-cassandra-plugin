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

import org.apache.cassandra.io.sstable.CQLSSTableWriter;
import org.junit.Test;
import org.mockito.ArgumentMatchers;
import org.mockito.invocation.InvocationOnMock;
import org.mockito.stubbing.Answer;
import org.pentaho.di.core.row.RowMetaInterface;
import org.pentaho.di.core.row.ValueMetaInterface;
import org.pentaho.di.core.row.value.ValueMetaBase;

import java.io.IOException;
import java.util.ArrayList;
import java.util.HashMap;
import java.util.List;
import java.util.Map;
import java.util.concurrent.atomic.AtomicBoolean;

import static org.junit.Assert.assertEquals;
import static org.junit.Assert.assertFalse;
import static org.junit.Assert.assertTrue;
import static org.junit.Assert.fail;
import static org.mockito.Mockito.doAnswer;
import static org.mockito.Mockito.doReturn;
import static org.mockito.Mockito.mock;


public class CQL3SSTableWriterTest {

  public static final String KEY_FIELD = "KEY_FIELD";
  public static final String TABLE = "TABLE";
  public static final String KEY_SPACE = "KEY_SPACE";
  public static final String DIRECTORY_PATH = "directory_path";
  public static final int BUFFER_SIZE = 10;
  public static final AtomicBoolean checker = new AtomicBoolean( true );
  public static final String COLUMN = "someColumn";

  class CQL3SSTableWriterStub extends CQL3SSTableWriter {
    @Override CQLSSTableWriter getCQLSSTableWriter() {
      assertEquals( DIRECTORY_PATH, getDirectory() );
      assertEquals( KEY_SPACE, getKeyspace() );
      assertEquals( TABLE, getTable() );
      assertEquals( BUFFER_SIZE, getBufferSize() );
      assertEquals( KEY_FIELD, getPrimaryKey() );
      CQLSSTableWriter ssWriter = mock( CQLSSTableWriter.class );
      try {
        doAnswer( new Answer<Void>() {
          @Override public Void answer( InvocationOnMock invocation ) throws Throwable {
            checker.set( false );
            return null;
          }
        } ).when( ssWriter ).close();
      } catch ( IOException e ) {
        fail( e.toString() );
      }

      try {
        doAnswer( new Answer<Void>() {
          @Override public Void answer( InvocationOnMock invocation ) throws Throwable {
            checker.set( true );
            return null;
          }
      } ).when( ssWriter ).addRow( ArgumentMatchers.anyMap() );
      } catch ( Exception e ) {
        fail( e.toString() );
      }

      return ssWriter;
    }
  }

  @Test
  public void testInit() throws Exception {
    CQL3SSTableWriter writer = getCql3SSTableWriter();
    writer.init();
  }

  private CQL3SSTableWriter getCql3SSTableWriter() {
    CQL3SSTableWriter writer = new CQL3SSTableWriterStub();
    writer.setPrimaryKey( KEY_FIELD );
    RowMetaInterface rmi = mock( RowMetaInterface.class );
    ValueMetaInterface one = new ValueMetaBase( KEY_FIELD, ValueMetaBase.TYPE_INTEGER );
    ValueMetaInterface two = new ValueMetaBase( COLUMN, ValueMetaBase.TYPE_STRING );
    List<ValueMetaInterface> valueMetaList = new ArrayList<ValueMetaInterface>();
    valueMetaList.add( one );
    valueMetaList.add( two );
    String[] fieldNames = new String[] { "key", "two" };
    doReturn( valueMetaList ).when( rmi ).getValueMetaList();
    doReturn( fieldNames ).when( rmi ).getFieldNames();
    writer.setRowMeta( rmi );
    writer.setBufferSize( BUFFER_SIZE );
    writer.setTable( TABLE );
    writer.setKeyspace( KEY_SPACE );
    writer.setDirectory( DIRECTORY_PATH );
    writer.setPartitionerClass( "org.apache.cassandra.dht.Murmur3Partitioner" );
    return writer;
  }

  @Test
  public void testProcessRow() throws Exception {
    CQL3SSTableWriter writer = getCql3SSTableWriter();
    writer.init();
    Map<String, Object> input = new HashMap<String, Object>();
    input.put( KEY_FIELD, 1 );
    input.put( COLUMN, "someColumnValue" );
    checker.set( false );
    writer.processRow( input );
    assertTrue( checker.get() );
  }

  @Test
  public void testClose() throws Exception {
    CQL3SSTableWriter writer = getCql3SSTableWriter();
    writer.init();
    checker.set( true );
    writer.close();
    assertFalse( checker.get() );
  }

  @Test
  public void testBuildCreateTableCQLStatement() throws Exception {
    CQL3SSTableWriter writer = getCql3SSTableWriter();
    writer.init();
    assertEquals( "CREATE TABLE KEY_SPACE.TABLE (\"KEY_FIELD\" bigint,\"someColumn\" varchar,PRIMARY KEY "
      + "(\"KEY_FIELD\" ));", writer.buildCreateTableCQLStatement() );
  }

  @Test
  public void testBuildInsertCQLStatement() throws Exception {
    CQL3SSTableWriter writer = getCql3SSTableWriter();
    writer.init();
    assertEquals( "INSERT INTO KEY_SPACE.TABLE (\"key\",\"two\") VALUES (?,?);", writer.buildInsertCQLStatement() );
  }
}
