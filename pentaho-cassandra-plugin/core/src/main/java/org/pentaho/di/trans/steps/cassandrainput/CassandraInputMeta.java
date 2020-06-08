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

import org.eclipse.swt.widgets.Shell;
import org.pentaho.cassandra.driver.datastax.DriverConnection;
import org.pentaho.cassandra.spi.IQueryMetaData;
import org.pentaho.cassandra.spi.Keyspace;
import org.pentaho.di.core.Counter;
import org.pentaho.di.core.annotations.Step;
import org.pentaho.di.core.database.DatabaseMeta;
import org.pentaho.di.core.encryption.Encr;
import org.pentaho.di.core.exception.KettleException;
import org.pentaho.di.core.exception.KettleStepException;
import org.pentaho.di.core.exception.KettleXMLException;
import org.pentaho.di.core.injection.Injection;
import org.pentaho.di.core.injection.InjectionSupported;
import org.pentaho.di.core.row.RowMetaInterface;
import org.pentaho.di.core.row.ValueMetaInterface;
import org.pentaho.di.core.util.Utils;
import org.pentaho.di.core.variables.VariableSpace;
import org.pentaho.di.core.xml.XMLHandler;
import org.pentaho.di.repository.ObjectId;
import org.pentaho.di.repository.Repository;
import org.pentaho.di.trans.Trans;
import org.pentaho.di.trans.TransMeta;
import org.pentaho.di.trans.step.BaseStepMeta;
import org.pentaho.di.trans.step.StepDataInterface;
import org.pentaho.di.trans.step.StepDialogInterface;
import org.pentaho.di.trans.step.StepInterface;
import org.pentaho.di.trans.step.StepMeta;
import org.pentaho.di.trans.step.StepMetaInterface;
import org.pentaho.metastore.api.IMetaStore;
import org.w3c.dom.Node;

import java.util.List;
import java.util.Map;

/**
 * Class providing an input step for reading data from an Cassandra table
 */
@Step( id = "CassandraInput", image = "Cassandrain.svg", name = "Cassandra input",
    description = "Reads data from a Cassandra table",
    documentationUrl = "Products/Cassandra_Input",
    categoryDescription = "Big Data" )
@InjectionSupported( localizationPrefix = "CassandraInput.Injection." )
public class CassandraInputMeta extends BaseStepMeta implements StepMetaInterface {

  protected static final Class<?> PKG = CassandraInputMeta.class;

  /**
   * The host to contact
   */
  @Injection( name = "CASSANDRA_HOST" )
  protected String cassandraHost = "localhost"; //$NON-NLS-1$

  /**
   * The port that cassandra is listening on
   */
  @Injection( name = "CASSANDRA_PORT" )
  protected String cassandraPort = "9042"; //$NON-NLS-1$

  /**
   * Username for authentication
   */
  @Injection( name = "USER_NAME" )
  protected String username;

  /**
   * Password for authentication
   */
  @Injection( name = "PASSWORD" )
  protected String password;

  /**
   * The keyspace (database) to use
   */
  @Injection( name = "CASSANDRA_KEYSPACE" )
  protected String cassandraKeyspace;

  /**
   * Whether to use GZIP compression of CQL queries
   */
  @Injection( name = "USE_QUERY_COMPRESSION" )
  protected boolean useCompression;

  /**
   * The select query to execute
   */
  @Injection( name = "CQL_QUERY" )
  protected String cqlSelectQuery = "SELECT <fields> FROM <table> WHERE <condition>;"; //$NON-NLS-1$

  /**
   * Whether to execute the query for each incoming row
   */
  @Injection( name = "EXECUTE_FOR_EACH_ROW" )
  protected boolean executeForEachIncomingRow;

  /**
   * Timeout (milliseconds) to use for socket connections - blank means use cluster default This is the timeout used
   * when connecting to the cluster with the driver. The default value is 5 seconds.
   */
  @Injection( name = "SOCKET_TIMEOUT" )
  protected String socketTimeout = ""; //$NON-NLS-1$

  @Injection( name = "SSL" )
  protected boolean ssl = false;

  @Injection( name = "LOCAL_DATACENTER" )
  protected String localDataCenter;

  /**
   * The number of rows to fetch when paging results. The default is ~5k, but that may not work based on the read
   * timeout, memory, or for other reasons on very wide tables
   */
  @Injection( name = "ROW_FETCH_SIZE" )
  protected String rowFetchSize = "";

  /**
   * The read timeout to use. The default is 12 seconds for the Cassandra driver, but that might be too slow and receive
   * OperationTimeoutException depending on connection or result size
   */
  @Injection( name = "READ_TIMEOUT" )
  protected String readTimeout = "";

  @Injection( name = "APPLICATION_CONF" )
  protected String applicationConfFile = null;

  @Injection( name = "NOT_EXPANDING_MAPS" )
  protected boolean notExpandingMaps = false;

  @Injection( name = "EXPAND_COLLECTION" )
  protected boolean expandComplex = true;

  /**
   * Set the timeout (milliseconds) to use for socket comms
   *
   * @param t
   *          the timeout to use in milliseconds
   */
  public void setSocketTimeout( String t ) {
    socketTimeout = t;
  }

  /**
   * Get the timeout (milliseconds) to use for socket comms
   *
   * @return the timeout to use in milliseconds
   */
  public String getSocketTimeout() {
    return socketTimeout;
  }

  public boolean isSsl() {
    return ssl;
  }

  public void setSsl( boolean ssl ) {
    this.ssl = ssl;
  }

  public String getRowFetchSize() {
    return rowFetchSize;
  }

  public void setRowFetchSize( String rowFetchSizerowFetchSize ) {
    this.rowFetchSize = rowFetchSizerowFetchSize;
  }

  public String getReadTimeout() {
    return readTimeout;
  }

  public void setReadTimeout( String readTimeout ) {
    this.readTimeout = readTimeout;
  }

  public String getApplicationConfFile() {
    return applicationConfFile;
  }

  public void setApplicationConfFile( String applicationConfFile ) {
    this.applicationConfFile = applicationConfFile;
  }

  /**
   * Set the cassandra node hostname to connect to
   *
   * @param host
   *          the host to connect to
   */
  public void setCassandraHost( String host ) {
    cassandraHost = host;
  }

  /**
   * Get the name of the cassandra node to connect to
   *
   * @return the name of the cassandra node to connect to
   */
  public String getCassandraHost() {
    return cassandraHost;
  }

  /**
   * Set the port that cassandra is listening on
   *
   * @param port
   *          the port that cassandra is listening on
   */
  public void setCassandraPort( String port ) {
    cassandraPort = port;
  }

  /**
   * Get the port that cassandra is listening on
   *
   * @return the port that cassandra is listening on
   */
  public String getCassandraPort() {
    return cassandraPort;
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
   * Set whether to compress (GZIP) CQL queries when transmitting them to the server
   *
   * @param c
   *          true if CQL queries are to be compressed
   */
  public void setUseCompression( boolean c ) {
    useCompression = c;
  }

  /**
   * Get whether CQL queries will be compressed (GZIP) or not
   *
   * @return true if CQL queries will be compressed when sending to the server
   */
  public boolean getUseCompression() {
    return useCompression;
  }

  /**
   * Set the CQL SELECT query to execute.
   *
   * @param query
   *          the query to execute
   */
  public void setCQLSelectQuery( String query ) {
    cqlSelectQuery = query;
  }

  /**
   * Get the CQL SELECT query to execute
   *
   * @return the query to execute
   */
  public String getCQLSelectQuery() {
    return cqlSelectQuery;
  }

  /**
   * Set the username to authenticate with
   *
   * @param un
   *          the username to authenticate with
   */
  public void setUsername( String un ) {
    username = un;
  }

  /**
   * Get the username to authenticate with
   *
   * @return the username to authenticate with
   */
  public String getUsername() {
    return username;
  }

  /**
   * Set the password to authenticate with
   *
   * @param pass
   *          the password to authenticate with
   */
  public void setPassword( String pass ) {
    password = pass;
  }

  /**
   * Get the password to authenticate with
   *
   * @return the password to authenticate with
   */
  public String getPassword() {
    return password;
  }

  /**
   * Set whether the query should be executed for each incoming row
   *
   * @param e
   *          true if the query should be executed for each incoming row
   */
  public void setExecuteForEachIncomingRow( boolean e ) {
    executeForEachIncomingRow = e;
  }

  /**
   * Get whether the query should be executed for each incoming row
   *
   * @return true if the query should be executed for each incoming row
   */
  public boolean getExecuteForEachIncomingRow() {
    return executeForEachIncomingRow;
  }

  public String getLocalDataCenter() {
    return localDataCenter;
  }

  public void setLocalDataCenter( String localDataCenter ) {
    this.localDataCenter = localDataCenter;
  }

  public boolean isNotExpandingMaps() {
    return notExpandingMaps;
  }

  public void setNotExpandingMaps( boolean notExpandingMaps ) {
    this.notExpandingMaps = notExpandingMaps;
  }

  public boolean isExpandComplex() {
    return expandComplex;
  }

  public void setExpandComplex( boolean expandComplex ) {
    this.expandComplex = expandComplex;
  }

  @Override
  public String getXML() {
    StringBuffer retval = new StringBuffer();

    if ( !Utils.isEmpty( cassandraHost ) ) {
      retval.append( "\n    " )
          .append( XMLHandler.addTagValue( "cassandra_host", cassandraHost ) ); //$NON-NLS-1$ //$NON-NLS-2$
    }

    if ( !Utils.isEmpty( cassandraPort ) ) {
      retval.append( "\n    " )
          .append( XMLHandler.addTagValue( "cassandra_port", cassandraPort ) ); //$NON-NLS-1$ //$NON-NLS-2$
    }

    if ( !Utils.isEmpty( username ) ) {
      retval.append( "\n    " ).append( XMLHandler.addTagValue( "username", username ) ); //$NON-NLS-1$ //$NON-NLS-2$
    }

    if ( !Utils.isEmpty( password ) ) {
      retval.append( "\n    " ).append( //$NON-NLS-1$
        XMLHandler.addTagValue( "password", Encr.encryptPasswordIfNotUsingVariables( password ) ) ); //$NON-NLS-1$
    }

    if ( !Utils.isEmpty( cassandraKeyspace ) ) {
      retval.append( "\n    " )
          .append( XMLHandler.addTagValue( "cassandra_keyspace", cassandraKeyspace ) ); //$NON-NLS-1$ //$NON-NLS-2$
    }

    retval.append( "\n    " )
        .append( XMLHandler.addTagValue( "use_compression", useCompression ) ); //$NON-NLS-1$ //$NON-NLS-2$

    if ( !Utils.isEmpty( cqlSelectQuery ) ) {
      retval.append( "\n    " )
          .append( XMLHandler.addTagValue( "cql_select_query", cqlSelectQuery ) ); //$NON-NLS-1$ //$NON-NLS-2$
    }

    if ( !Utils.isEmpty( socketTimeout ) ) {
      retval.append( "\n    " )
          .append( XMLHandler.addTagValue( "socket_timeout", socketTimeout ) ); //$NON-NLS-1$ //$NON-NLS-2$
    }

    if ( !Utils.isEmpty( readTimeout ) ) {
      retval.append( "\n    " )
          .append( XMLHandler.addTagValue( "read_timeout", readTimeout ) ); //$NON-NLS-1$ //$NON-NLS-2$
    }

    if ( !Utils.isEmpty( rowFetchSize ) ) {
      retval.append( "\n    " )
          .append( XMLHandler.addTagValue( "row_fetch_size", rowFetchSize ) ); //$NON-NLS-1$ //$NON-NLS-2$
    }

    retval.append( "\n    " )
        .append( XMLHandler.addTagValue( "ssl", ssl ) ); //$NON-NLS-1$ //$NON-NLS-2$

    retval.append( "\n    " )
      .append( XMLHandler.addTagValue( "application_conf_file", applicationConfFile ) ); //$NON-NLS-1$ //$NON-NLS-2$

    retval.append( "    " ).append( //$NON-NLS-1$
      XMLHandler.addTagValue( "execute_for_each_row", executeForEachIncomingRow ) ); //$NON-NLS-1$

    retval.append( "    " ).append( //$NON-NLS-1$
      XMLHandler.addTagValue( "local_datacenter", localDataCenter ) ); //$NON-NLS-1$

    retval.append( "    " ).append( //$NON-NLS-1$
      XMLHandler.addTagValue( "not_backward_compatible", notExpandingMaps ) ); //$NON-NLS-1$

    retval.append( "    " ).append( //$NON-NLS-1$
      XMLHandler.addTagValue( "expand_complex", expandComplex ) ); //$NON-NLS-1$

    return retval.toString();
  }

  @Override
  public void loadXML( Node stepnode, List<DatabaseMeta> databases, Map<String, Counter> counters )
    throws KettleXMLException {
    cassandraHost = XMLHandler.getTagValue( stepnode, "cassandra_host" ); //$NON-NLS-1$
    cassandraPort = XMLHandler.getTagValue( stepnode, "cassandra_port" ); //$NON-NLS-1$
    username = XMLHandler.getTagValue( stepnode, "username" ); //$NON-NLS-1$
    password = XMLHandler.getTagValue( stepnode, "password" ); //$NON-NLS-1$
    if ( !Utils.isEmpty( password ) ) {
      password = Encr.decryptPasswordOptionallyEncrypted( password );
    }
    cassandraKeyspace = XMLHandler.getTagValue( stepnode, "cassandra_keyspace" ); //$NON-NLS-1$
    cqlSelectQuery = XMLHandler.getTagValue( stepnode, "cql_select_query" ); //$NON-NLS-1$
    useCompression =
        XMLHandler.getTagValue( stepnode, "use_compression" ).equalsIgnoreCase( "Y" ); //$NON-NLS-1$ //$NON-NLS-2$
    ssl = "Y".equalsIgnoreCase( XMLHandler.getTagValue( stepnode, "ssl" ) );

    String executeForEachR = XMLHandler.getTagValue( stepnode, "execute_for_each_row" ); //$NON-NLS-1$
    if ( !Utils.isEmpty( executeForEachR ) ) {
      executeForEachIncomingRow = executeForEachR.equalsIgnoreCase( "Y" ); //$NON-NLS-1$
    }

    socketTimeout = XMLHandler.getTagValue( stepnode, "socket_timeout" ); //$NON-NLS-1$

    rowFetchSize = XMLHandler.getTagValue( stepnode, "row_fetch_size" ); //$NON-NLS-1$
    readTimeout = XMLHandler.getTagValue( stepnode, "read_timeout" ); //$NON-NLS-1$
    localDataCenter = XMLHandler.getTagValue( stepnode, "local_datacenter" );
    applicationConfFile = XMLHandler.getTagValue( stepnode, "application_conf_file" );

    notExpandingMaps = "Y".equalsIgnoreCase( XMLHandler.getTagValue( stepnode, "not_backward_compatible" ) );
    if ( !Utils.isEmpty( XMLHandler.getTagValue( stepnode, "expand_complex" ) ) ) {
      expandComplex = "Y".equalsIgnoreCase( XMLHandler.getTagValue( stepnode, "expand_complex" ) );
    }

  }

  @Override
  public void readRep( Repository rep, ObjectId id_step, List<DatabaseMeta> databases, Map<String, Counter> counters )
    throws KettleException {
    cassandraHost = rep.getStepAttributeString( id_step, 0, "cassandra_host" ); //$NON-NLS-1$
    cassandraPort = rep.getStepAttributeString( id_step, 0, "cassandra_port" ); //$NON-NLS-1$
    username = rep.getStepAttributeString( id_step, 0, "username" ); //$NON-NLS-1$
    password = rep.getStepAttributeString( id_step, 0, "password" ); //$NON-NLS-1$
    if ( !Utils.isEmpty( password ) ) {
      password = Encr.decryptPasswordOptionallyEncrypted( password );
    }
    cassandraKeyspace = rep.getStepAttributeString( id_step, 0, "cassandra_keyspace" ); //$NON-NLS-1$
    cqlSelectQuery = rep.getStepAttributeString( id_step, 0, "cql_select_query" ); //$NON-NLS-1$
    useCompression = rep.getStepAttributeBoolean( id_step, 0, "use_compression" ); //$NON-NLS-1$
    executeForEachIncomingRow = rep.getStepAttributeBoolean( id_step, "execute_for_each_row" ); //$NON-NLS-1$

    socketTimeout = rep.getStepAttributeString( id_step, 0, "socket_timeout" ); //$NON-NLS-1$

    rowFetchSize = rep.getStepAttributeString( id_step, 0, "row_fetch_size" ); //$NON-NLS-1$
    readTimeout = rep.getStepAttributeString( id_step, 0, "read_timeout" ); //$NON-NLS-1$
    ssl = rep.getStepAttributeBoolean( id_step, 0, "ssl" );
    localDataCenter = rep.getStepAttributeString( id_step, 0, "local_datacenter" );
    applicationConfFile = rep.getStepAttributeString( id_step, 0, "applicationConfFile" );
    notExpandingMaps = rep.getStepAttributeBoolean( id_step, 0, "not_backward_compatible" );
    expandComplex = rep.getStepAttributeBoolean( id_step, 0, "expand_complex", true );
  }

  @Override
  public void saveRep( Repository rep, ObjectId id_transformation, ObjectId id_step ) throws KettleException {
    if ( !Utils.isEmpty( cassandraHost ) ) {
      rep.saveStepAttribute( id_transformation, id_step, 0, "cassandra_host", cassandraHost ); //$NON-NLS-1$
    }

    if ( !Utils.isEmpty( cassandraPort ) ) {
      rep.saveStepAttribute( id_transformation, id_step, 0, "cassandra_port", cassandraPort ); //$NON-NLS-1$
    }

    if ( !Utils.isEmpty( username ) ) {
      rep.saveStepAttribute( id_transformation, id_step, 0, "username", username ); //$NON-NLS-1$
    }

    if ( !Utils.isEmpty( password ) ) {
      rep.saveStepAttribute( id_transformation, id_step, 0, "password", Encr //$NON-NLS-1$
          .encryptPasswordIfNotUsingVariables( password ) );
    }

    if ( !Utils.isEmpty( cassandraKeyspace ) ) {
      rep.saveStepAttribute( id_transformation, id_step, 0, "cassandra_keyspace", cassandraKeyspace ); //$NON-NLS-1$
    }

    rep.saveStepAttribute( id_transformation, id_step, 0, "use_compression", useCompression ); //$NON-NLS-1$
    rep.saveStepAttribute( id_transformation, id_step, 0, "ssl", ssl );

    if ( !Utils.isEmpty( cqlSelectQuery ) ) {
      rep.saveStepAttribute( id_transformation, id_step, 0, "cql_select_query", cqlSelectQuery ); //$NON-NLS-1$
    }

    rep.saveStepAttribute( id_transformation, id_step, 0, "execute_for_each_row", //$NON-NLS-1$
      executeForEachIncomingRow );

    if ( !Utils.isEmpty( socketTimeout ) ) {
      rep.saveStepAttribute( id_transformation, id_step, 0, "socket_timeout", socketTimeout ); //$NON-NLS-1$
    }

    if ( !Utils.isEmpty( readTimeout ) ) {
      rep.saveStepAttribute( id_transformation, id_step, 0, "read_timeout", readTimeout ); //$NON-NLS-1$
    }

    if ( !Utils.isEmpty( rowFetchSize ) ) {
      rep.saveStepAttribute( id_transformation, id_step, 0, "row_fetch_size", rowFetchSize ); //$NON-NLS-1$
    }

    rep.saveStepAttribute( id_transformation, id_step, 0, "local_datacenter", localDataCenter );

    rep.saveStepAttribute( id_transformation, id_step, 0, "application_conf_file", applicationConfFile );

    rep.saveStepAttribute( id_transformation, id_step, 0, "expand_complex", expandComplex );

    rep.saveStepAttribute( id_transformation, id_step, 0, "not_backward_compatible", notExpandingMaps );

  }

  @Override
  public StepInterface getStep( StepMeta stepMeta, StepDataInterface stepDataInterface, int copyNr,
      TransMeta transMeta, Trans trans ) {

    return new CassandraInput( stepMeta, stepDataInterface, copyNr, transMeta, trans );
  }

  @Override
  public StepDataInterface getStepData() {
    return new CassandraInputData();
  }

  @Override
  public void setDefault() {
    cassandraHost = "localhost"; //$NON-NLS-1$
    cassandraPort = "9042"; //$NON-NLS-1$
    cqlSelectQuery = "SELECT <fields> FROM <table> WHERE <condition>;"; //$NON-NLS-1$
    useCompression = false;
    socketTimeout = ""; //$NON-NLS-1$
    ssl = false;
    localDataCenter = "datacenter1";
    expandComplex = true;
    notExpandingMaps = false;
  }

  @Override
  public void getFields( RowMetaInterface inputRowMeta, String name, RowMetaInterface[] info, StepMeta nextStep,
                         VariableSpace space, Repository repository, IMetaStore metaStore ) throws KettleStepException {

    inputRowMeta.clear(); // start afresh - eats the input

    String keyspaceS = space.environmentSubstitute( cassandraKeyspace );
    String queryS = space.environmentSubstitute( cqlSelectQuery );

    if ( Utils.isEmpty( keyspaceS ) || Utils.isEmpty( queryS ) ) {
      // no keyspace or query!
      return;
    }

    if ( executeForEachIncomingRow ) {
      // We can't do field substitution here, so need to convert the field subsitution to something
      // Cassandra is actually OK with.  Search for ?{field_name} and replace with ?
      queryS = queryS.replaceAll( "\\?\\{[^\\}]+\\}", "?" );
    }

    DriverConnection conn = null;

    try {

      conn = CassandraInputData.getCassandraConnection( this, space, getLog() );
      conn.setExpandCollection( expandComplex );
      Keyspace ks = conn.getKeyspace( keyspaceS );
      IQueryMetaData qm = ks.getQueryMetaData( queryS, expandComplex, notExpandingMaps );

      List<ValueMetaInterface> vmis = qm.getValueMetasForQuery();
      vmis.stream().forEach( vmi -> {
        vmi.setOrigin( name );
        inputRowMeta.addValueMeta( vmi );
      } );

    } catch ( Exception ex ) {
      ex.printStackTrace();
      logError( ex.getMessage(), ex );

    } finally {
      try {
        if ( conn != null ) {
          conn.closeConnection();
        }
      } catch ( Exception e ) {
        logError( e.getMessage(), e );
      }
    }
  }

  public StepDialogInterface getDialog( Shell shell, StepMetaInterface meta, TransMeta transMeta, String name ) {

    return new CassandraInputDialog( shell, meta, transMeta, name );
  }

}
