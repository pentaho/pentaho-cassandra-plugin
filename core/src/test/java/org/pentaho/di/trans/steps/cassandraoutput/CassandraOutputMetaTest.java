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


package org.pentaho.di.trans.steps.cassandraoutput;

import org.junit.Assert;
import org.junit.Test;

public class CassandraOutputMetaTest {
  @Test
  public void validateConvertToSecondsWithNONETTLUnits() {
    CassandraOutputMeta.TTLUnits ttlUnit = CassandraOutputMeta.TTLUnits.NONE;
    int value = 1;
    value = ttlUnit.convertToSeconds( value );

    Assert.assertEquals( -1, value );

  }

  @Test
  public void validateConvertToSecondsWithSecondsTTLUnits() {
    CassandraOutputMeta.TTLUnits ttlUnit = CassandraOutputMeta.TTLUnits.SECONDS;
    int value = 1;
    value = ttlUnit.convertToSeconds( value );

    Assert.assertEquals( 1, value );

  }

  @Test
  public void validateConvertToSecondsWithMinutesTTLUnits() {
    CassandraOutputMeta.TTLUnits ttlUnit = CassandraOutputMeta.TTLUnits.MINUTES;
    int value = 1;
    value = ttlUnit.convertToSeconds( value );

    Assert.assertEquals( 60, value );

  }

  @Test
  public void validateConvertToSecondsWithHOURSTTLUnits() {
    CassandraOutputMeta.TTLUnits ttlUnit = CassandraOutputMeta.TTLUnits.HOURS;
    int value = 1;
    value = ttlUnit.convertToSeconds( value );

    Assert.assertEquals( 3600, value );

  }

  @Test
  public void validateConvertToSecondsWithDAYSTTLUnits() {
    CassandraOutputMeta.TTLUnits ttlUnit = CassandraOutputMeta.TTLUnits.DAYS;
    int value = 1;
    value = ttlUnit.convertToSeconds( value );

    Assert.assertEquals( 86400, value );

  }
}
