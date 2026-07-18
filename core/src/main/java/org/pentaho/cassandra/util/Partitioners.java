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
public enum Partitioners {
  MURMUR3( "Murmur3Partitioner", "org.apache.cassandra.db.marshal.LongType" ), RANDOM( "RandomPartitioner",
      "org.apache.cassandra.db.marshal.IntegerType" ), BYTEORDERED( "ByteOrderedPartitioner",
      "org.apache.cassandra.db.marshal.BytesType" );

  private final String name;

  private final String type;

  /**
   * @param name
   * @param type
   */
  private Partitioners( String name, String type ) {
    this.name = name;
    this.type = type;
  }

  /**
   * @return the name
   */
  public String getName() {
    return name;
  }

  /**
   * @return the type
   */
  public String getType() {
    return type;
  }

  public static Partitioners getFromString( String string ) {
    if ( string == null ) {
      return MURMUR3;
    }

    for ( Partitioners prs : Partitioners.values() ) {
      if ( string.endsWith( prs.getName() ) ) {
        return prs;
      }
    }
    return MURMUR3;
  }

}
