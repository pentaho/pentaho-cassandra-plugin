/*******************************************************************************
*
* Pentaho Big Data
*
* Copyright (C) 2002-2020 by Hitachi Vantara : http://www.pentaho.com
*
*******************************************************************************
*
* Licensed under the Apache License, Version 2.0 (the "License");
* you may not use this file except in compliance with
* the License. You may obtain a copy of the License at
*
*    http://www.apache.org/licenses/LICENSE-2.0
*
* Unless required by applicable law or agreed to in writing, software
* distributed under the License is distributed on an "AS IS" BASIS,
* WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
* See the License for the specific language governing permissions and
* limitations under the License.
*
******************************************************************************/

package org.pentaho.di.trans.steps.cassandrainput;

import org.pentaho.cassandra.ConnectionFactory;
import org.pentaho.cassandra.driver.datastax.DriverConnection;
import org.pentaho.cassandra.util.CassandraUtils;
import org.pentaho.di.core.exception.KettleException;
import org.pentaho.di.core.logging.LogChannelInterface;
import org.pentaho.di.core.row.RowMetaInterface;
import org.pentaho.di.core.util.Utils;
import org.pentaho.di.core.variables.VariableSpace;
import org.pentaho.di.i18n.BaseMessages;
import org.pentaho.di.trans.step.BaseStepData;
import org.pentaho.di.trans.step.StepDataInterface;

import java.util.HashMap;
import java.util.Map;

/**
 * Data class for the CassandraInput step. Contains some utility methods for obtaining a connection to cassandra,
 * translating a row from cassandra to Kettle and for compressing a query.
 * 
 * @author Mark Hall (mhall{[at]}pentaho{[dot]}com)
 * @version $Revision$
 */
public class CassandraInputData extends BaseStepData implements
    StepDataInterface {

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

  public static DriverConnection getCassandraConnection( CassandraInputMeta meta, VariableSpace varSpace,
      LogChannelInterface logChannel )
    throws Exception {
    return getCassandraConnection( meta, varSpace, logChannel, false );
  }

  public static DriverConnection getCassandraConnection( CassandraInputMeta meta, VariableSpace varSpace,
                                                         LogChannelInterface logChannel, boolean logBasic )
    throws Exception {

    // Get the connection to Cassandra
    String hostS = varSpace.environmentSubstitute( meta.getCassandraHost() );
    String portS = varSpace.environmentSubstitute( meta.getCassandraPort() );
    String timeoutS = varSpace.environmentSubstitute( meta.getSocketTimeout() );
    String readTimeoutS = varSpace.environmentSubstitute( meta.getReadTimeout() );
    String rowFetchSizeS = varSpace.environmentSubstitute( meta.getRowFetchSize() );
    String localDataCenter = varSpace.environmentSubstitute( meta.getLocalDataCenter() );
    String userS = meta.getUsername();
    String passS = meta.getPassword();
    String applicationConf = varSpace.environmentSubstitute( meta.getApplicationConfFile() );
    if ( !Utils.isEmpty( userS ) && !Utils.isEmpty( passS ) ) {
      userS = varSpace.environmentSubstitute( userS );
      passS = varSpace.environmentSubstitute( passS );
    }
    String keyspaceS = varSpace.environmentSubstitute( meta.getCassandraKeyspace() );

    if ( Utils.isEmpty( applicationConf ) && ( Utils.isEmpty( hostS ) || Utils.isEmpty( portS ) || Utils.isEmpty( keyspaceS ) ) ) {
      throw new KettleException( "Some connection details are missing!!" ); //$NON-NLS-1$
    }

    if ( logBasic ) {
      logChannel.logBasic( BaseMessages.getString( CassandraInputMeta.PKG,
        "CassandraInput.Info.Connecting", hostS, portS, keyspaceS ) ); //$NON-NLS-1$
    }
    Map<String, String> opts = new HashMap<String, String>();

    if ( meta.getUseCompression() ) {
      opts.put( CassandraUtils.ConnectionOptions.COMPRESSION, Boolean.TRUE.toString() );
    }

    if ( Utils.isEmpty( applicationConf ) ) {
      if ( !Utils.isEmpty( timeoutS ) ) {
        opts.put( CassandraUtils.ConnectionOptions.SOCKET_TIMEOUT, timeoutS );
      }

      if ( !Utils.isEmpty( readTimeoutS ) ) {
        opts.put( CassandraUtils.ConnectionOptions.READ_TIMEOUT, readTimeoutS );
      }

      if ( !Utils.isEmpty( rowFetchSizeS ) ) {
        opts.put( CassandraUtils.ConnectionOptions.ROW_FETCH_SIZE, rowFetchSizeS );
      }

      if ( meta.isSsl() ) {
        opts.put( CassandraUtils.ConnectionOptions.SSL, "Y" );
      }

    } else {
      opts.put( CassandraUtils.ConnectionOptions.APPLICATION_CONF, applicationConf );
    }

    opts.put( CassandraUtils.CQLOptions.DATASTAX_DRIVER_VERSION, CassandraUtils.CQLOptions.CQL3_STRING );


    if ( opts.size() > 0 && logBasic ) {
      logChannel.logBasic( BaseMessages.getString( CassandraInputMeta.PKG, "CassandraInput.Info.UsingConnectionOptions", //$NON-NLS-1$
        CassandraUtils.optionsToString( opts ) ) );
    }

    return CassandraUtils.getCassandraConnection( hostS, portS, userS, passS,
      ConnectionFactory.Driver.BINARY_CQL3_PROTOCOL, opts, localDataCenter );

  }

}
