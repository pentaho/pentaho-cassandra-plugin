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


package org.pentaho.di.trans.steps.cassandrasstableoutput;

import org.junit.After;
import org.junit.AfterClass;
import org.junit.BeforeClass;
import org.junit.Test;
import org.pentaho.di.core.exception.KettleException;
import org.pentaho.di.core.logging.LoggingObjectInterface;
import org.pentaho.di.trans.steps.mock.StepMockHelper;

import static org.mockito.ArgumentMatchers.any;
import static org.mockito.Mockito.when;

public class SSTableOutputTest {
  private static StepMockHelper<SSTableOutputMeta, SSTableOutputData> helper;
  private static final SecurityManager sm = System.getSecurityManager();

  @BeforeClass
  public static void setUp() throws KettleException {
    //KettleEnvironment.init();
    helper =
      new StepMockHelper<SSTableOutputMeta, SSTableOutputData>( "SSTableOutputIT", SSTableOutputMeta.class,
        SSTableOutputData.class );
    when( helper.logChannelInterfaceFactory.create( any(), any( LoggingObjectInterface.class ) ) ).thenReturn(
      helper.logChannelInterface );
    when( helper.trans.isRunning() ).thenReturn( true );
  }

  @AfterClass
  public static void classTearDown() {
    //Cleanup class setup
    helper.cleanUp();
  }

  @After
  public void tearDown() throws Exception {
    // Restore original security manager if needed
    if ( System.getSecurityManager() != sm ) {
      System.setSecurityManager( sm );
    }
  }

  @Test( expected = SecurityException.class )
  public void testDisableSystemExit() throws Exception {
    SSTableOutput ssTableOutput =
      new SSTableOutput( helper.stepMeta, helper.stepDataInterface, 0, helper.transMeta, helper.trans );
    ssTableOutput.disableSystemExit( sm, helper.logChannelInterface );
    System.exit( 1 );
  }
}
