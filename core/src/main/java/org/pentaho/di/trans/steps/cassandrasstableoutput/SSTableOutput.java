/*******************************************************************************
 *
 * Pentaho Big Data
 *
 * Copyright (C) 2002-2018 by Hitachi Vantara : http://www.pentaho.com
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

package org.pentaho.di.trans.steps.cassandrasstableoutput;

import java.io.File;
import java.net.URI;
import java.security.Permission;
import java.util.HashMap;
import java.util.Map;

import org.pentaho.di.core.exception.KettleException;
import org.pentaho.di.core.logging.LogChannelInterface;
import org.pentaho.di.core.row.RowMetaInterface;
import org.pentaho.di.core.util.Utils;
import org.pentaho.di.i18n.BaseMessages;
import org.pentaho.di.trans.Trans;
import org.pentaho.di.trans.TransMeta;
import org.pentaho.di.trans.step.BaseStep;
import org.pentaho.di.trans.step.StepDataInterface;
import org.pentaho.di.trans.step.StepInterface;
import org.pentaho.di.trans.step.StepMeta;
import org.pentaho.di.trans.step.StepMetaInterface;
import org.pentaho.di.trans.steps.cassandrasstableoutput.writer.AbstractSSTableWriter;
import org.pentaho.di.trans.steps.cassandrasstableoutput.writer.SSTableWriterBuilder;

/**
 * Output step for writing Cassandra SSTables (sorted-string tables).
 *
 * @author Rob Turner (robert{[at]}robertturner{[dot]}com{[dot]}au)
 * @author Mark Hall (mhall{[at]}pentaho{[dot]}com)
 */
public class SSTableOutput extends BaseStep implements StepInterface {
  private static final SecurityManager sm = System.getSecurityManager();
  /** The number of rows seen so far for this batch */
  protected int rowsSeen;
  /** Writes the SSTable output */
  protected AbstractSSTableWriter writer;
  /** Used to determine input fields */
  protected RowMetaInterface inputMetadata;
  /** List of field names (optimization) */
  private String[] fieldNames;
  /** List of field indices (optimization) */
  private int[] fieldValueIndices;

  public SSTableOutput( StepMeta stepMeta, StepDataInterface stepDataInterface, int copyNr, TransMeta transMeta,
      Trans trans ) {

    super( stepMeta, stepDataInterface, copyNr, transMeta, trans );
  }

  private void initialize( SSTableOutputMeta smi ) throws Exception {
    first = false;
    rowsSeen = 0;
    inputMetadata = getInputRowMeta();

    String yamlPath = environmentSubstitute( smi.getYamlPath() );
    String directory = environmentSubstitute( smi.getDirectory() );
    String keyspace = environmentSubstitute( smi.getCassandraKeyspace() );
    String table = environmentSubstitute( smi.getTableName() );
    String keyField = environmentSubstitute( smi.getKeyField() );
    String bufferSize = environmentSubstitute( smi.getBufferSize() );

    if ( Utils.isEmpty( yamlPath ) ) {
      throw new Exception( BaseMessages.getString( SSTableOutputMeta.PKG, "SSTableOutput.Error.NoPathToYAML" ) );
    }
    logBasic( BaseMessages.getString( SSTableOutputMeta.PKG, "SSTableOutput.Message.YAMLPath", yamlPath ) );

    File outputDir;
    if ( Utils.isEmpty( directory ) ) {
      outputDir = new File( System.getProperty( "java.io.tmpdir" ) );
    } else {
      outputDir = new File( new URI( directory ) );
    }

    if ( !outputDir.exists() ) {
      if ( !outputDir.mkdirs() ) {
        throw new KettleException( BaseMessages.getString( SSTableOutputMeta.PKG,
            "SSTableOutput.Error.OutputDirDoesntExist" ) );
      }
    }

    if ( Utils.isEmpty( table ) ) {
      throw new KettleException( BaseMessages.getString( SSTableOutputMeta.PKG,
          "SSTableOutput.Error.NoTableSpecified" ) );
    }

    if ( Utils.isEmpty( keyField ) ) {
      throw new KettleException( BaseMessages.getString( SSTableOutputMeta.PKG, "SSTableOutput.Error.NoKeySpecified" ) );
    }

    // what are the fields? where are they?
    fieldNames = inputMetadata.getFieldNames();
    fieldValueIndices = new int[fieldNames.length];
    for ( int i = 0; i < fieldNames.length; i++ ) {
      fieldValueIndices[i] = inputMetadata.indexOfValue( fieldNames[i] );
    }
    // create/init writer
    if ( writer != null ) {
      writer.close();
    }

    SSTableWriterBuilder builder =
        new SSTableWriterBuilder().withConfig( yamlPath ).withDirectory( outputDir.getAbsolutePath() ).withKeyspace(
            keyspace ).withTable( table ).withRowMeta( getInputRowMeta() ).withPrimaryKey( keyField )
            .withCqlVersion( smi.getUseCQL3() ? 3 : 2 );
    try {
      builder.withBufferSize( Integer.parseInt( bufferSize ) );
    } catch ( NumberFormatException nfe ) {
      logBasic( BaseMessages.getString( SSTableOutputMeta.PKG, "SSTableOutput.Message.DefaultBufferSize" ) );
    }

    writer = builder.build();
    try {
      disableSystemExit( sm, log );
      writer.init();
    } catch ( Exception e ) {
      throw new RuntimeException( BaseMessages.getString( SSTableOutputMeta.PKG, "SSTableOutput.Error.InvalidConfig" ),
        e );
    } finally {
      // Restore original security manager if needed
      if ( System.getSecurityManager() != sm ) {
        System.setSecurityManager( sm );
      }
    }
  }

  void disableSystemExit( SecurityManager sm, LogChannelInterface log ) {
    // Workaround JVM exit caused by org.apache.cassandra.config.DatabaseDescriptor in case of any issue with
    // cassandra config. Do this by preventing JVM from exit for writer initialization time or give user a clue at
    // least.
    try {
      System.setSecurityManager( new NoSystemExitDelegatingSecurityManager( sm ) );
    } catch ( SecurityException se ) {
      log.logError( BaseMessages.getString( SSTableOutputMeta.PKG, "SSTableOutput.Error.JVMExitProtection" ), se );
    }
  }

  @Override
  public boolean processRow( StepMetaInterface smi, StepDataInterface sdi ) throws KettleException {
    // still processing?
    if ( isStopped() ) {
      return false;
    }

    Object[] r = getRow();

    if ( first ) {
      try {
        initialize( (SSTableOutputMeta) smi );
      } catch ( Exception e ) {
        throw new KettleException(
          BaseMessages.getString( SSTableOutputMeta.PKG, "SSTableOutput.Error.WriterInitFailed" ), e );
      }
    }

    try {
      if ( r == null ) {
        // no more output - clean up/close connections
        setOutputDone();
        closeWriter();
        return false;
      }
      // create record
      Map<String, Object> record = new HashMap<String, Object>();
      for ( int i = 0; i < fieldNames.length; i++ ) {
        Object value = r[fieldValueIndices[i]];
        if ( value == null || "".equals( value ) ) {
          continue;
        }
        record.put( fieldNames[i], value );
      }
      // write it
      writer.processRow( record );
      incrementLinesWritten();
    } catch ( Exception e ) {
      logError( BaseMessages.getString( SSTableOutputMeta.PKG, "SSTableOutput.Error.FailedToProcessRow" ), e );
      // single error row - found it!
      putError( getInputRowMeta(), r, 1L, e.getMessage(), null, "ERR_SSTABLE_OUTPUT_01" );
      incrementLinesRejected();
    }

    // error will occur after adding it
    return true;
  }

  @Override
  public void setStopped( boolean stopped ) {
    super.setStopped( stopped );
    if ( stopped ) {
      closeWriter();
    }
  }

  public void closeWriter() {
    if ( writer != null ) {
      try {
        writer.close();
        writer = null;
      } catch ( Exception e ) {
        // YUM!!
        logError( BaseMessages.getString( SSTableOutputMeta.PKG, "SSTableOutput.Error.FailedToCloseWriter" ), e );
      }
    }
  }

  private class JVMShutdownAttemptedException extends SecurityException {
  }

  private class NoSystemExitDelegatingSecurityManager extends SecurityManager {
    private SecurityManager delegate;

    NoSystemExitDelegatingSecurityManager( SecurityManager delegate ) {
      this.delegate = delegate;
    }

    @Override
    public void checkPermission( Permission perm ) {
      if ( delegate != null ) {
        delegate.checkPermission( perm );
      }
    }

    @Override
    public void checkPermission( Permission perm, Object context ) {
      if ( delegate != null ) {
        delegate.checkPermission( perm, context );
      }
    }

    @Override
    public void checkExit( int status ) {
      throw new JVMShutdownAttemptedException();
    }
  }
}
