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

import org.junit.Test;
import org.mockito.Mockito;
import org.pentaho.cassandra.driver.datastax.DriverCQLRowHandler;
import org.pentaho.cassandra.util.CassandraUtils;


import java.util.Map;

import static org.mockito.ArgumentMatchers.any;
import static org.mockito.ArgumentMatchers.anyInt;
import static org.mockito.ArgumentMatchers.anyString;
import static org.mockito.Mockito.doCallRealMethod;
import static org.mockito.Mockito.mock;
import static org.mockito.Mockito.times;
import static org.mockito.Mockito.verify;
import static org.mockito.Mockito.when;


public class CassandraOutputTest {

  @Test
  public void validateInvalidTtlFieldTest() {
    DriverCQLRowHandler handler = mock( DriverCQLRowHandler.class );
    CassandraOutput co = mock( CassandraOutput.class );

    doCallRealMethod().when( co ).validateTtlField( any(), any() );
    doCallRealMethod().when( handler ).setTtlSec( anyInt() );

    String ttl = "a";

    co.validateTtlField( handler, ttl );

    verify( handler, times(0) ).setTtlSec( anyInt() );
    verify( co, times(1) ).logDebug( any() );
  }

  @Test
  public void validateEmptyTtlFieldTest() {
    DriverCQLRowHandler handler = mock( DriverCQLRowHandler.class );
    CassandraOutput co = mock( CassandraOutput.class );

    doCallRealMethod().when( co ).validateTtlField( any(), any() );
    doCallRealMethod().when( handler ).setTtlSec( anyInt() );

    String ttl = "";

    co.validateTtlField( handler, ttl );

    verify( handler, times(0) ).setTtlSec( anyInt() );
    verify( co, times(0) ).logDebug( any() );
  }

  @Test
  public void validateCorrectTtlFieldTest() {
    DriverCQLRowHandler handler = mock( DriverCQLRowHandler.class );
    CassandraOutput co = mock( CassandraOutput.class );

    doCallRealMethod().when( co ).validateTtlField( any(), any() );
    doCallRealMethod().when( handler ).setTtlSec( anyInt() );

    String ttl = "120";

    co.validateTtlField( handler, ttl );

    verify( handler, times(1) ).setTtlSec( anyInt() );
    verify( co, times(0) ).logDebug( any() );
  }

  @Test
  public void validateSetTTLIfSpecifiedTestWithOptionNone() {
    CassandraOutput co = mock( CassandraOutput.class );
    co.m_meta = mock( CassandraOutputMeta.class );
    co.m_opts = mock( Map.class );


    String ttlEnvironmentSubstituteValue= "1"; // none option, this value is ignored default will be -1
    String ttlOption =  "None";
    int expectedValue = -1;

    when( co.environmentSubstitute( Mockito.<String>any() ) ).thenReturn( ttlEnvironmentSubstituteValue );
    when( co.m_meta.getTTLUnit() ).thenReturn( ttlOption );
    when( co.m_opts.put( any(), any()) ).thenReturn( "dummy" );


    doCallRealMethod().when( co ).setTTLIfSpecified();
    co.setTTLIfSpecified();

    verify( co.m_opts, times(1)).put( CassandraUtils.BatchOptions.TTL, "" + expectedValue );
  }

  @Test
  public void validateSetTTLIfSpecifiedTestWithOptionSeconds() {
    CassandraOutput co = mock( CassandraOutput.class );
    co.m_meta = mock( CassandraOutputMeta.class );
    co.m_opts = mock( Map.class );

    String ttlEnvironmentSubstituteValue= "1"; // 1 second
    String ttlOption =  "Seconds";
    int expectedValue = 1;

    when( co.environmentSubstitute( Mockito.<String>any() ) ).thenReturn( ttlEnvironmentSubstituteValue );
    when( co.m_meta.getTTLUnit() ).thenReturn( ttlOption );
    when( co.m_opts.put( any(), any()) ).thenReturn( "dummy" );


    doCallRealMethod().when( co ).setTTLIfSpecified();
    co.setTTLIfSpecified();

    verify( co.m_opts, times(1)).put( CassandraUtils.BatchOptions.TTL, "" + expectedValue );
  }

  @Test
  public void validateSetTTLIfSpecifiedTestWithOptionMinutes() {
    CassandraOutput co = mock( CassandraOutput.class );
    co.m_meta = mock( CassandraOutputMeta.class );
    co.m_opts = mock( Map.class );

    String ttlEnvironmentSubstituteValue= "1"; //1 minute
    String ttlOption =  "Minutes";
    int expectedValue = 60;

    when( co.environmentSubstitute( Mockito.<String>any() ) ).thenReturn( ttlEnvironmentSubstituteValue );
    when( co.m_meta.getTTLUnit() ).thenReturn( ttlOption );
    when( co.m_opts.put( any(), any()) ).thenReturn( "dummy" );


    doCallRealMethod().when( co ).setTTLIfSpecified();
    co.setTTLIfSpecified();

    verify( co.m_opts, times(1)).put( CassandraUtils.BatchOptions.TTL, "" + expectedValue );
  }

  @Test
  public void validateSetTTLIfSpecifiedTestWithOptionHours() {
    CassandraOutput co = mock( CassandraOutput.class );
    co.m_meta = mock( CassandraOutputMeta.class );
    co.m_opts = mock( Map.class );

    String ttlEnvironmentSubstituteValue= "1"; //1 hour
    String ttlOption =  "Hours";
    int expectedValue = 3600;

    when( co.environmentSubstitute( Mockito.<String>any() ) ).thenReturn( ttlEnvironmentSubstituteValue );
    when( co.m_meta.getTTLUnit() ).thenReturn( ttlOption );
    when( co.m_opts.put( any(), any()) ).thenReturn( "dummy" );


    doCallRealMethod().when( co ).setTTLIfSpecified();
    co.setTTLIfSpecified();

    verify( co.m_opts, times(1)).put( CassandraUtils.BatchOptions.TTL, "" + expectedValue );
  }

  @Test
  public void validateSetTTLIfSpecifiedTestWithOptionDays() {
    CassandraOutput co = mock( CassandraOutput.class );
    co.m_meta = mock( CassandraOutputMeta.class );
    co.m_opts = mock( Map.class );

    String ttlEnvironmentSubstituteValue= "1"; // 1 day
    String ttlOption =  "Days";
    int expectedValue = 86400;

    when( co.environmentSubstitute( Mockito.<String>any() ) ).thenReturn( ttlEnvironmentSubstituteValue );
    when( co.m_meta.getTTLUnit() ).thenReturn( ttlOption );
    when( co.m_opts.put( any(), any()) ).thenReturn( "dummy" );


    doCallRealMethod().when( co ).setTTLIfSpecified();
    co.setTTLIfSpecified();

    verify( co.m_opts, times(1)).put( CassandraUtils.BatchOptions.TTL, "" + expectedValue );
  }


}
