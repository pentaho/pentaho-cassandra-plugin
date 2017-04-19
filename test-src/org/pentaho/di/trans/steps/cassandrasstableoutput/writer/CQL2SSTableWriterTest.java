/*!
 * Copyright 2017 Pentaho Corporation.  All rights reserved.
 *
 * Licensed under the Apache License, Version 2.0 (the "License");
 * you may not use this file except in compliance with the License.
 * You may obtain a copy of the License at
 *
 * http://www.apache.org/licenses/LICENSE-2.0
 *
 * Unless required by applicable law or agreed to in writing, software
 * distributed under the License is distributed on an "AS IS" BASIS,
 * WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
 * See the License for the specific language governing permissions and
 * limitations under the License.
 *
 */

package org.pentaho.di.trans.steps.cassandrasstableoutput.writer;

import org.apache.cassandra.io.sstable.SSTableSimpleUnsortedWriter;
import org.junit.Test;
import org.mockito.invocation.InvocationOnMock;
import org.mockito.stubbing.Answer;

import java.io.File;
import java.io.IOException;
import java.nio.ByteBuffer;
import java.util.ArrayList;
import java.util.Date;
import java.util.HashMap;
import java.util.List;
import java.util.Map;
import java.util.concurrent.atomic.AtomicBoolean;
import java.util.concurrent.atomic.AtomicReference;

import static org.junit.Assert.*;
import static org.mockito.Matchers.anyLong;
import static org.mockito.Matchers.anyObject;
import static org.mockito.Mockito.doAnswer;
import static org.mockito.Mockito.mock;

public class CQL2SSTableWriterTest {

  public static final String KEY_FIELD = "KEY_FIELD";
  public static final String PARTITIONER_CLASSNAME = "java.lang.Integer";
  public static final String COLUMN_FAMILY = "COLUMN_FAMILY";
  public static final String KEY_SPACE = "KEY_SPACE";
  public static final String DIRECTORY_PATH = "directory_path";
  public static final int BUFFER_SIZE = 10;
  public static final AtomicBoolean checker = new AtomicBoolean( true );
  public static final AtomicReference result = new AtomicReference( new HashMap<Object, List<Object>>() );
  public static final AtomicReference workingKey = new AtomicReference( null );

  class CQL2SSTableWriterStub extends CQL2SSTableWriter {
    @Override SSTableSimpleUnsortedWriter getSsTableSimpleUnsortedWriter( File file,
                                                                          Class partitionerClass,
                                                                          String keyspace,
                                                                          String columnFamily,
                                                                          int bufferSize )
      throws InstantiationException, IllegalAccessException {
      assertEquals( DIRECTORY_PATH, file.getName() );
      assertEquals( KEY_SPACE, keyspace );
      assertEquals( COLUMN_FAMILY, columnFamily );
      assertEquals( BUFFER_SIZE, bufferSize );
      assertEquals( PARTITIONER_CLASSNAME, partitionerClass.getCanonicalName() );
      SSTableSimpleUnsortedWriter ssWriter = mock( SSTableSimpleUnsortedWriter.class );
      try {
        doAnswer( new Answer() {
          @Override public Object answer( InvocationOnMock invocation ) throws Throwable {
            checker.set( false );
            return null;
          }
        } ).when( ssWriter ).close();
      } catch ( IOException e ) {
        fail( e.toString() );
      }
      try {
        doAnswer( new Answer() {
          @Override public Object answer( InvocationOnMock invocation ) throws Throwable {
            ByteBuffer arg = (ByteBuffer) invocation.getArguments()[ 0 ];
            Map<ByteBuffer, List<ByteBuffer>> obj;
            Map<ByteBuffer, List<ByteBuffer>> objClone;
            do {
              obj = (Map<ByteBuffer, List<ByteBuffer>>) result.get();
              objClone = new HashMap<ByteBuffer, List<ByteBuffer>>( obj );
              List<ByteBuffer> cols = objClone.get( arg );
              if ( cols == null ) {
                objClone.put( arg, new ArrayList<ByteBuffer>() );
              } else {
                throw new IOException( "Such key is already used" );
              }
            } while ( !result.compareAndSet( obj, objClone ) );
            workingKey.set( arg );
            return null;
          }
        } ).when( ssWriter ).newRow( (ByteBuffer) anyObject() );
      } catch ( IOException e ) {
        fail( e.toString() );
      }


      doAnswer( new Answer() {
        @Override public Object answer( InvocationOnMock invocation ) throws Throwable {
          ByteBuffer name = (ByteBuffer) invocation.getArguments()[ 0 ];
          ByteBuffer value = (ByteBuffer) invocation.getArguments()[ 1 ];
          Object timestamp = invocation.getArguments()[ 2 ];
          ByteBuffer currentKey = (ByteBuffer) workingKey.get();
          assertNotNull( "adding columns without calling new row", currentKey );
          assertFalse( "key is the same as column", currentKey.equals( value ) );
          Map<ByteBuffer, List<ByteBuffer>> obj;
          Map<ByteBuffer, List<ByteBuffer>> objClone;
          do {
            obj = (Map<ByteBuffer, List<ByteBuffer>>) result.get();
            objClone = new HashMap<ByteBuffer, List<ByteBuffer>>( obj );
            List<ByteBuffer> cols = objClone.get( currentKey );
            assertNotNull( "adding columns without calling new row", cols );
            cols.add( value );
            objClone.put( currentKey, cols );
          } while ( !result.compareAndSet( obj, objClone ) );

          return null;
        }
      } ).when( ssWriter ).addColumn( (ByteBuffer) anyObject(), (ByteBuffer) anyObject(), anyLong() );

      return ssWriter;
    }
  }

  @Test
  public void testInit() throws Exception {
    CQL2SSTableWriter writer = getCql2SSTableWriter();
    writer.init();
  }

  private CQL2SSTableWriter getCql2SSTableWriter() {
    CQL2SSTableWriter writer = new CQL2SSTableWriterStub();
    writer.setKeyField( KEY_FIELD );
    writer.setPartitionerClassName( PARTITIONER_CLASSNAME );
    writer.setBufferSize( BUFFER_SIZE );
    writer.setColumnFamily( COLUMN_FAMILY );
    writer.setKeyspace( KEY_SPACE );
    writer.setDirectory( DIRECTORY_PATH );
    return writer;
  }

  @Test
  public void testProcessRow() throws Exception {
    CQL2SSTableWriter writer = getCql2SSTableWriter();
    writer.init();
    Map<String, Object> input = new HashMap<String, Object>();
    input.put( KEY_FIELD, 1 );
    input.put( "someColumn", "someColumnValue" );
    writer.processRow( input );
    Map<ByteBuffer, List<ByteBuffer>> expected = new HashMap<ByteBuffer, List<ByteBuffer>>();
    List<ByteBuffer> col = new ArrayList<ByteBuffer>();
    col.add( CQL2SSTableWriter.valueToBytes( "someColumnValue" ) );
    expected.put( CQL2SSTableWriter.valueToBytes( 1 ), col );
    assertEquals( expected, result.get() );
  }

  @Test
  public void testClose() throws Exception {
    CQL2SSTableWriter writer = getCql2SSTableWriter();
    writer.init();
    checker.set( true );
    writer.close();
    assertFalse( checker.get() );
  }

  @Test
  public void testBytes() throws Exception {
    assertEquals( ByteBuffer.wrap( new byte[] { } ), CQL2SSTableWriter.valueToBytes( null ) );
    assertEquals( ByteBuffer.wrap( new byte[] { } ), CQL2SSTableWriter.valueToBytes( "" ) );
    assertEquals( ByteBuffer.wrap( new byte[] { 115, 116, 114, 105, 110, 103 } ),
      CQL2SSTableWriter.valueToBytes( "string" ) );
    assertEquals( ByteBuffer.wrap( new byte[] { 0, 0, 0, 0 } ), CQL2SSTableWriter.valueToBytes( 0 ) );
    assertEquals( ByteBuffer.wrap( new byte[] { 0, 0, 1, 15 } ), CQL2SSTableWriter.valueToBytes( 271 ) );
    assertEquals( ByteBuffer.wrap( new byte[] { 0, 0, 0, 0 } ), CQL2SSTableWriter.valueToBytes( 0F ) );
    assertEquals( ByteBuffer.wrap( new byte[] { 65, 96, 0, 0 } ), CQL2SSTableWriter.valueToBytes( 14F ) );
    assertEquals( ByteBuffer.wrap( new byte[] { 116, 114, 117, 101 } ), CQL2SSTableWriter.valueToBytes( true ) );
    assertEquals( ByteBuffer.wrap( new byte[] { 102, 97, 108, 115, 101 } ), CQL2SSTableWriter.valueToBytes( false ) );
    assertEquals( ByteBuffer.wrap( new byte[] { 0, 0, 0, 0, 0, 0, 0, 0 } ), CQL2SSTableWriter.valueToBytes( 0L ) );
    assertEquals( ByteBuffer.wrap( new byte[] { 0, 0, 0, 0, 0, 0, 0, 16 } ), CQL2SSTableWriter.valueToBytes( 16L ) );
    assertEquals( ByteBuffer.wrap( new byte[] { 0, 0, 0, 0, 0, 0, 0, 0 } ), CQL2SSTableWriter.valueToBytes( 0.0 ) );
    assertEquals( ByteBuffer.wrap( new byte[] { 64, 54, -128, 0, 0, 0, 0, 0 } ),
      CQL2SSTableWriter.valueToBytes( 22.5 ) );
    assertEquals( ByteBuffer.wrap( new byte[] { 0, 0, 0, 0, 7, 91, -51, 21 } ),
      CQL2SSTableWriter.valueToBytes( new Date( 123456789 ) ) );
    assertEquals( ByteBuffer.wrap( new byte[] { } ), CQL2SSTableWriter.valueToBytes( new byte[] { } ) );
    assertEquals( ByteBuffer.wrap( new byte[] { 0, 0, 0, 0 } ),
      CQL2SSTableWriter.valueToBytes( new byte[] { 0, 0, 0, 0 } ) );
  }
}
