package org.pentaho.cassandra.cql;

import static org.junit.Assert.assertEquals;
import static org.junit.Assert.assertNull;

import org.junit.Test;

public class CQLFunctionsTest {

  @Test
  public void testGetFunctionsFromString() {
    String[] fString = new String[] { "TOKEN", "COUNT", "WRITETIME", "TTL", "DATEOF", "UNIXTIMESTAMPOF" };
    for ( int i = 0; i < CQLFunctions.values().length; i++ ) {
      CQLFunctions actualF = CQLFunctions.getFromString( fString[i] );
      assertEquals( CQLFunctions.values()[i], actualF );
    }
  }

  @Test
  public void testGetFunctionsValidators() {
    String[] expectedValidators =
        new String[] { "org.apache.cassandra.db.marshal.LongType", "org.apache.cassandra.db.marshal.LongType",
          "org.apache.cassandra.db.marshal.LongType", "org.apache.cassandra.db.marshal.Int32Type",
          "org.apache.cassandra.db.marshal.TimestampType", "org.apache.cassandra.db.marshal.LongType" };
    assertEquals( expectedValidators.length, CQLFunctions.values().length );
    for ( int i = 0; i < expectedValidators.length; i++ ) {
      assertEquals( "Incorrect validator for the function: " + CQLFunctions.values()[i].name(), expectedValidators[i],
          CQLFunctions.values()[i].getValidator() );
    }
  }

  @Test
  public void testGetNull_IfInputIsUnknownFunction() {
    CQLFunctions actualP = CQLFunctions.getFromString( "UnknownFunction" );
    assertNull( actualP );
  }

  @Test
  public void testGetNull_IfInputIsNull() {
    CQLFunctions actualP = CQLFunctions.getFromString( null );
    assertNull( actualP );
  }
}
