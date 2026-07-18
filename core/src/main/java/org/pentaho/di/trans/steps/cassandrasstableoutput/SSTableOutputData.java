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


package org.pentaho.di.trans.steps.cassandrasstableoutput;

import org.pentaho.di.core.row.RowMetaInterface;
import org.pentaho.di.trans.step.BaseStepData;
import org.pentaho.di.trans.step.StepDataInterface;

/**
 * Data class for SSTablesOutput step.
 * 
 * @author Rob Turner (robert{[at]}robertturner{[dot]}com{[dot]}au)
 * @author Mark Hall (mhall{[at]}pentaho{[dot]}com)
 */
public class SSTableOutputData extends BaseStepData implements StepDataInterface {

  /** The output data format */
  protected RowMetaInterface outputRowMeta;

  /**
   * Get the output row format
   * 
   * @return the output row format
   */
  public RowMetaInterface getOutputRowMeta() {
    return outputRowMeta;
  }

  /**
   * Set the output row format
   * 
   * @param rmi
   *          the output row format
   */
  public void setOutputRowMeta( RowMetaInterface rmi ) {
    outputRowMeta = rmi;
  }
}
