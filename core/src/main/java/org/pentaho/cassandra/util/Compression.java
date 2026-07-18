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

public enum Compression {
  NONE( 0 ),
  GZIP( 1 );

  private int value;

  Compression( int value ) {
    this.value = value;
  }

  public int getValue() {
    return this.value;
  }
}
