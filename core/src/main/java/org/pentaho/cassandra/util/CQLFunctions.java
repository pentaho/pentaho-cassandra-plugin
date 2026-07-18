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

package org.pentaho.cassandra.util;

/**
 * @author Tatsiana_Kasiankova
 * 
 */
public enum CQLFunctions {
  TOKEN( false, "org.apache.cassandra.db.marshal.LongType" ), COUNT( false, "org.apache.cassandra.db.marshal.LongType" ), WRITETIME(
      false, "org.apache.cassandra.db.marshal.LongType" ), TTL( false, "org.apache.cassandra.db.marshal.Int32Type" ), DATEOF(
      true, "org.apache.cassandra.db.marshal.TimestampType" ), UNIXTIMESTAMPOF( true,
      "org.apache.cassandra.db.marshal.LongType" );

  private final boolean isCaseSensitive;
  private final String validator;

  private CQLFunctions( boolean isCaseSensetive, String validator ) {
    this.isCaseSensitive = isCaseSensetive;
    this.validator = validator;
  }

  /**
   * Indicates if the name of the function should be processed as case sensitive or not.
   * 
   * @return isCaseSensitive true if the name of the function should be processed as case sensitive or not.
   */
  public boolean isCaseSensitive() {
    return isCaseSensitive;
  }

  /**
   * Returns the Cassandra validator class for the function.
   * 
   * @return the Cassandra validator class for the function.
   */
  public String getValidator() {
    return validator;
  }

  /**
   * Returns CQLFunction by the string representation of it.
   * 
   * @param input
   *          the string representation of CQLFunction
   * @return the CQLFunction if the input string contains this one, otherwise null.
   */
  public static CQLFunctions getFromString( String input ) {
    if ( input != null ) {
      input = input.trim().toUpperCase();
      for ( CQLFunctions fs : CQLFunctions.values() ) {
        if ( isFunction( fs, input ) ) {
          return fs;
        }
      }
    }
    return null;
  }

  private static boolean isFunction( CQLFunctions fs, String input ) {
    return input.equals( fs.name() );
  }

}
