/*******************************************************************************
 *
 * Pentaho Big Data
 *
 * Copyright (C) 2002-2019 by Hitachi Vantara : http://www.pentaho.com
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

import java.util.List;
import java.util.Map;

import org.pentaho.di.core.CheckResult;
import org.pentaho.di.core.CheckResultInterface;
import org.pentaho.di.core.Counter;
import org.pentaho.di.core.annotations.Step;
import org.pentaho.di.core.database.DatabaseMeta;
import org.pentaho.di.core.exception.KettleException;
import org.pentaho.di.core.exception.KettleXMLException;
import org.pentaho.di.core.injection.Injection;
import org.pentaho.di.core.injection.InjectionSupported;
import org.pentaho.di.core.plugins.ParentFirst;
import org.pentaho.di.core.row.RowMetaInterface;
import org.pentaho.di.core.util.Utils;
import org.pentaho.di.core.xml.XMLHandler;
import org.pentaho.di.repository.ObjectId;
import org.pentaho.di.repository.Repository;
import org.pentaho.di.trans.Trans;
import org.pentaho.di.trans.TransMeta;
import org.pentaho.di.trans.step.BaseStepMeta;
import org.pentaho.di.trans.step.StepDataInterface;
import org.pentaho.di.trans.step.StepInterface;
import org.pentaho.di.trans.step.StepMeta;
import org.pentaho.di.trans.step.StepMetaInterface;
import org.w3c.dom.Node;

/**
 * Provides metadata for the Cassandra SSTable output step.
 */
@Step( id = "SSTableOutput", image = "Cassandra.svg", name = "SSTable output",
    documentationUrl = "Products/SSTable_Output",
    description = "Writes to a filesystem directory as a Cassandra SSTable", categoryDescription = "Big Data" )
@InjectionSupported( localizationPrefix = "SSTableOutput.Injection." )
@ParentFirst( patterns = { ".*" } )
public class SSTableOutputMeta extends BaseStepMeta implements StepMetaInterface {

  protected static final Class<?> PKG = SSTableOutputMeta.class;

  /** The path to the yaml file */
  @Injection( name = "YAML_FILE_PATH" )
  protected String m_yamlPath;

  /** The directory to output to */
  @Injection( name = "DIRECTORY" )
  protected String directory;

  /** The keyspace (database) to use */
  @Injection( name = "CASSANDRA_KEYSPACE" )
  protected String cassandraKeyspace;

  /** The table to write to */
  @Injection( name = "TABLE" )
  protected String table = "";

  /** The field in the incoming data to use as the key for inserts */
  @Injection( name = "KEY_FIELD" )
  protected String keyField = "";

  /** Size (MB) of write buffer */
  @Injection( name = "BUFFER_SIZE" )
  protected String bufferSize = "16";

  /**
   * Whether to use CQL version 3
   */
  protected boolean m_useCQL3 = true;

  /**
   * Get the path the the yaml file
   *
   * @return the path to the yaml file
   */
  public String getYamlPath() {
    return m_yamlPath;
  }

  /**
   * Set the path the the yaml file
   *
   * @param path
   *          the path to the yaml file
   */
  public void setYamlPath( String path ) {
    m_yamlPath = path;
  }

  /**
   * Where the SSTables are written to
   *
   * @return String directory
   */
  public String getDirectory() {
    return directory;
  }

  /**
   * Where the SSTables are written to
   *
   * @param directory
   *          String
   */
  public void setDirectory( String directory ) {
    this.directory = directory;
  }

  /**
   * Set the keyspace (db) to use
   *
   * @param keyspace
   *          the keyspace to use
   */
  public void setCassandraKeyspace( String keyspace ) {
    cassandraKeyspace = keyspace;
  }

  /**
   * Get the keyspace (db) to use
   *
   * @return the keyspace (db) to use
   */
  public String getCassandraKeyspace() {
    return cassandraKeyspace;
  }

  /**
   * Set the table to write to
   *
   * @param table
   *          the name of the table to write to
   */
  public void setTableName( String table ) {
    this.table = table;
  }

  /**
   * Get the name of the table to write to
   *
   * @return the name of the table to write to
   */
  public String getTableName() {
    return table;
  }

  /**
   * Set the incoming field to use as the key for inserts
   *
   * @param keyField
   *          the name of the incoming field to use as the key
   */
  public void setKeyField( String keyField ) {
    this.keyField = keyField;
  }

  /**
   * Get the name of the incoming field to use as the key for inserts
   *
   * @return the name of the incoming field to use as the key for inserts
   */
  public String getKeyField() {
    return keyField;
  }

  /**
   * Size (MB) of write buffer
   *
   * @return String
   */
  public String getBufferSize() {
    return bufferSize;
  }

  /**
   * Size (MB) of write buffer
   *
   * @param bufferSize
   *          String
   */
  public void setBufferSize( String bufferSize ) {
    this.bufferSize = bufferSize;
  }

  /**
   * Set whether to use CQL version 3 is to be used for CQL IO mode
   *
   * @param cql3 true if CQL version 3 is to be used
   */
  public void setUseCQL3( boolean cql3 ) {
    m_useCQL3 = cql3;
  }

  /**
   * Get whether to use CQL version 3 is to be used for CQL IO mode
   *
   * @return true if CQL version 3 is to be used
   */
  public boolean getUseCQL3() {
    return m_useCQL3;
  }

  @Override
  public boolean supportsErrorHandling() {
    // enable define error handling option
    return true;
  }

  @Override
  public String getXML() {
    StringBuffer retval = new StringBuffer();

    if ( !Utils.isEmpty( m_yamlPath ) ) {
      retval.append( "\n    " ).append(
        XMLHandler.addTagValue( "yaml_path", m_yamlPath ) );
    }

    if ( !Utils.isEmpty( directory ) ) {
      retval.append( "\n    " ).append(
        XMLHandler.addTagValue( "output_directory", directory ) );
    }

    if ( !Utils.isEmpty( cassandraKeyspace ) ) {
      retval.append( "\n    " ).append(
        XMLHandler.addTagValue( "cassandra_keyspace", cassandraKeyspace ) );
    }

    if ( !Utils.isEmpty( table ) ) {
      retval.append( "\n    " ).append(
        XMLHandler.addTagValue( "table", table ) );
    }

    if ( !Utils.isEmpty( keyField ) ) {
      retval.append( "\n    " ).append(
        XMLHandler.addTagValue( "key_field", keyField ) );
    }

    if ( !Utils.isEmpty( bufferSize ) ) {
      retval.append( "\n    " ).append(
        XMLHandler.addTagValue( "buffer_size_mb", bufferSize ) );
    }

    retval.append( "\n    " ).append( //$NON-NLS-1$
      XMLHandler.addTagValue( "use_cql3", m_useCQL3 ) ); //$NON-NLS-1$

    return retval.toString();
  }

  public void loadXML( Node stepnode, List<DatabaseMeta> databases,
                       Map<String, Counter> counters ) throws KettleXMLException {
    m_yamlPath = XMLHandler.getTagValue( stepnode, "yaml_path" );
    directory = XMLHandler.getTagValue( stepnode, "output_directory" );
    cassandraKeyspace = XMLHandler.getTagValue( stepnode, "cassandra_keyspace" );
    table = XMLHandler.getTagValue( stepnode, "table" );
    keyField = XMLHandler.getTagValue( stepnode, "key_field" );
    bufferSize = XMLHandler.getTagValue( stepnode, "buffer_size_mb" );

    String useCQL3 = XMLHandler.getTagValue( stepnode, "use_cql3" ); //$NON-NLS-1$
    if ( !Utils.isEmpty( useCQL3 ) ) {
      m_useCQL3 = useCQL3.equalsIgnoreCase( "Y" ); //$NON-NLS-1$
    }
  }

  public void readRep( Repository rep, ObjectId id_step,
                       List<DatabaseMeta> databases, Map<String, Counter> counters )
    throws KettleException {
    m_yamlPath = rep.getStepAttributeString( id_step, 0, "yaml_path" );
    directory = rep.getStepAttributeString( id_step, 0, "output_directory" );
    cassandraKeyspace = rep.getStepAttributeString( id_step, 0,
      "cassandra_keyspace" );
    table = rep.getStepAttributeString( id_step, 0, "table" );
    keyField = rep.getStepAttributeString( id_step, 0, "key_field" );
    bufferSize = rep.getStepAttributeString( id_step, 0, "buffer_size_mb" );
    m_useCQL3 = rep.getStepAttributeBoolean( id_step, 0, "use_cql3" ); //$NON-NLS-1$
  }

  public void saveRep( Repository rep, ObjectId id_transformation,
                       ObjectId id_step ) throws KettleException {

    if ( !Utils.isEmpty( m_yamlPath ) ) {
      rep.saveStepAttribute( id_transformation, id_step, "yaml_path", m_yamlPath );
    }

    if ( !Utils.isEmpty( directory ) ) {
      rep.saveStepAttribute( id_transformation, id_step, "output_directory",
        directory );
    }

    if ( !Utils.isEmpty( cassandraKeyspace ) ) {
      rep.saveStepAttribute( id_transformation, id_step, "cassandra_keyspace",
        cassandraKeyspace );
    }

    if ( !Utils.isEmpty( table ) ) {
      rep.saveStepAttribute( id_transformation, id_step, "table",
        table );
    }

    if ( !Utils.isEmpty( keyField ) ) {
      rep.saveStepAttribute( id_transformation, id_step, "key_field", keyField );
    }

    if ( !Utils.isEmpty( bufferSize ) ) {
      rep.saveStepAttribute( id_transformation, id_step, "buffer_size_mb",
        bufferSize );
    }

    rep.saveStepAttribute( id_transformation, id_step, 0, "use_cql3", m_useCQL3 ); //$NON-NLS-1$
  }

  public void check( List<CheckResultInterface> remarks, TransMeta transMeta,
                     StepMeta stepMeta, RowMetaInterface prev, String[] input,
                     String[] output, RowMetaInterface info ) {

    CheckResult cr;

    if ( ( prev == null ) || ( prev.size() == 0 ) ) {
      cr = new CheckResult( CheckResult.TYPE_RESULT_WARNING,
        "Not receiving any fields from previous steps!", stepMeta );
      remarks.add( cr );
    } else {
      cr = new CheckResult( CheckResult.TYPE_RESULT_OK,
        "Step is connected to previous one, receiving " + prev.size()
          + " fields", stepMeta );
      remarks.add( cr );
    }

    // See if we have input streams leading to this step!
    if ( input.length > 0 ) {
      cr = new CheckResult( CheckResult.TYPE_RESULT_OK,
        "Step is receiving info from other steps.", stepMeta );
      remarks.add( cr );
    } else {
      cr = new CheckResult( CheckResult.TYPE_RESULT_ERROR,
        "No input received from other steps!", stepMeta );
      remarks.add( cr );
    }
  }

  public StepInterface getStep( StepMeta stepMeta,
                                StepDataInterface stepDataInterface, int copyNr, TransMeta transMeta,
                                Trans trans ) {

    return new SSTableOutput( stepMeta, stepDataInterface, copyNr, transMeta,
      trans );
  }

  public StepDataInterface getStepData() {
    return new SSTableOutputData();
  }

  public void setDefault() {
    directory = System.getProperty( "java.io.tmpdir" );
    bufferSize = "16";
    table = "";
  }

  /*
   * (non-Javadoc)
   *
   * @see org.pentaho.di.trans.step.BaseStepMeta#getDialogClassName()
   */
  @Override
  public String getDialogClassName() {
    return "org.pentaho.di.trans.steps.cassandrasstableoutput.SSTableOutputDialog";
  }
}
