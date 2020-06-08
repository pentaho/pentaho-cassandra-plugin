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

package org.pentaho.di.trans.steps.cassandraoutput;

import java.util.List;
import java.util.Map;

import org.pentaho.di.core.CheckResult;
import org.pentaho.di.core.CheckResultInterface;
import org.pentaho.di.core.Counter;
import org.pentaho.di.core.annotations.Step;
import org.pentaho.di.core.database.DatabaseMeta;
import org.pentaho.di.core.encryption.Encr;
import org.pentaho.di.core.exception.KettleException;
import org.pentaho.di.core.exception.KettleXMLException;
import org.pentaho.di.core.injection.Injection;
import org.pentaho.di.core.injection.InjectionSupported;
import org.pentaho.di.core.row.RowMetaInterface;
import org.pentaho.di.core.util.Utils;
import org.pentaho.di.core.xml.XMLHandler;
import org.pentaho.di.i18n.BaseMessages;
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
 * Class providing an output step for writing data to a cassandra table. Can create the specified
 * table (if it doesn't already exist) and can update table meta data.
 */
@Step( id = "CassandraOutput", image = "Cassandraout.svg", name = "Cassandra output",
    description = "Writes to a Cassandra table",
    documentationUrl = "Products/Cassandra_Output",
    categoryDescription = "Big Data" )
@InjectionSupported( localizationPrefix = "CassandraOutput.Injection." )
public class CassandraOutputMeta extends BaseStepMeta implements StepMetaInterface {

  public static final Class<?> PKG = CassandraOutputMeta.class;

  /** The host to contact */
  @Injection( name = "CASSANDRA_HOST" )
  protected String cassandraHost = "localhost"; //$NON-NLS-1$

  /** The port that cassandra is listening on */
  @Injection( name = "CASSANDRA_PORT" )
  protected String cassandraPort = "9042"; //$NON-NLS-1$

  /** The username to use for authentication */
  @Injection( name = "USER_NAME" )
  protected String username;

  /** The password to use for authentication */
  @Injection( name = "PASSWORD" )
  protected String password;

  /** The keyspace (database) to use */
  @Injection( name = "CASSANDRA_KEYSPACE" )
  protected String cassandraKeyspace;

  /** The cassandra node to put schema updates through */
  @Injection( name = "SCHEMA_HOST" )
  protected String schemaHost;

  /** The port of the cassandra node for schema updates */
  @Injection( name = "SCHEMA_PORT" )
  protected String schemaPort;

  /** The table to write to */
  @Injection( name = "TABLE" )
  protected String table = ""; //$NON-NLS-1$

  /** The consistency level to use - null or empty string result in the default */
  @Injection( name = "CONSISTENCY_LEVEL" )
  protected String consistency = ""; //$NON-NLS-1$

  /**
   * The batch size - i.e. how many rows to collect before inserting them via a batch CQL statement
   */
  @Injection( name = "BATCH_SIZE" )
  protected String batchSize = "100"; //$NON-NLS-1$

  /** True if unlogged (i.e. non atomic) batch writes are to be used. CQL 3 only */
  @Injection( name = "USE_UNLOGGED_BATCH" )
  protected boolean unloggedBatch = false;

  /** Whether to use GZIP compression of CQL queries */
  @Injection( name = "USE_QUERY_COMPRESSION" )
  protected boolean useCompression = false;

  /** Whether to create the specified table if it doesn't exist */
  @Injection( name = "CREATE_TABLE" )
  protected boolean createTable = true;

  /** Anything to include in the WITH clause at table creation time? */
  @Injection( name = "CREATE_TABLE_WITH_CLAUSE" )
  protected String createTableWithClause;

  /** The field in the incoming data to use as the key for inserts */
  @Injection( name = "KEY_FIELD" )
  protected String keyField = ""; //$NON-NLS-1$

  /**
   * Timeout (milliseconds) to use for socket connections - blank means use cluster default
   */
  @Injection( name = "SOCKET_TIMEOUT" )
  protected String socketTimeout = ""; //$NON-NLS-1$

  @Injection( name = "SSL" )
  protected boolean ssl = false;

  @Injection( name = "LOCAL_DATACENTER" )
  protected String localDataCenter;

  /**
   * Timeout (milliseconds) to use for CQL batch inserts. If blank, no timeout is used. Otherwise, whent the timeout
   * occurs the step will try to kill the insert and re-try after splitting the batch according to the batch split
   * factor
   */
  @Injection( name = "BATCH_TIMEOUT" )
  protected String cqlBatchTimeout = ""; //$NON-NLS-1$

  /**
   * Default batch split size - only comes into play if cql batch timeout has been specified. Specifies the size of the
   * sub-batches to split the batch into if a timeout occurs.
   */
  @Injection( name = "SUB_BATCH_SIZE" )
  protected String subBatchSize = "10"; //$NON-NLS-1$

  /**
   * Whether or not to insert incoming fields that are not in the cassandra table's meta data. Has no affect if the user
   * has opted to update the meta data for unknown incoming fields
   */
  @Injection( name = "INSERT_FIELDS_NOT_IN_META" )
  protected boolean insertFieldsNotInMeta = false;

  /**
   * Whether or not to initially update the table meta data with any unknown incoming fields
   */
  @Injection( name = "UPDATE_CASSANDRA_META" )
  protected boolean updateCassandraMeta = false;

  /** Whether to truncate the table before inserting */
  @Injection( name = "TRUNCATE_TABLE" )
  protected boolean truncateTable = false;

  /**
   * Any CQL statements to execute before inserting the first row. Can be used, for example, to create secondary indexes
   * on columns in a table.
   */
  @Injection( name = "APRIORI_CQL" )
  protected String aprioriCql = ""; //$NON-NLS-1$

  /**
   * Whether or not an exception generated when executing apriori CQL statements should stop the step
   */
  @Injection( name = "DONT_COMPLAIN_IF_APRIORI_CQL_FAILS" )
  protected boolean dontComplainAboutAprioriCqlFailing;

  /** Time to live (TTL) for inserts (affects all fields inserted) */
  @Injection( name = "TTL" )
  protected String ttl = ""; //$NON-NLS-1$

  @Injection( name = "TTL_UNIT" )
  protected String ttlUnit = TTLUnits.NONE.toString();

  @Injection( name = "APPLICATION_CONF" )
  protected String applicationConf = null;

  public enum TTLUnits {
    NONE( BaseMessages.getString( PKG, "CassandraOutput.TTLUnit.None" ) ) { //$NON-NLS-1$
      @Override
      int convertToSeconds( int value ) {
        return -1;
      }
    },
    SECONDS( BaseMessages.getString( PKG, "CassandraOutput.TTLUnit.Seconds" ) ) { //$NON-NLS-1$
      @Override
      int convertToSeconds( int value ) {
        return value;
      }
    },
    MINUTES( BaseMessages.getString( PKG, "CassandraOutput.TTLUnit.Minutes" ) ) { //$NON-NLS-1$
      @Override
      int convertToSeconds( int value ) {
        return value * 60;
      }
    },
    HOURS( BaseMessages.getString( PKG, "CassandraOutput.TTLUnit.Hours" ) ) { //$NON-NLS-1$
      @Override
      int convertToSeconds( int value ) {
        return value * 60 * 60;
      }
    },
    DAYS( BaseMessages.getString( PKG, "CassandraOutput.TTLUnit.Days" ) ) { //$NON-NLS-1$
      @Override
      int convertToSeconds( int value ) {
        return value * 60 * 60 * 24;
      }
    };

    private final String stringVal;

    TTLUnits( String name ) {
      stringVal = name;
    }

    @Override
    public String toString() {
      return stringVal;
    }

    abstract int convertToSeconds( int value );
  }

  public String getApplicationConf() {
    return applicationConf;
  }

  public void setApplicationConf( String applicationConf ) {
    this.applicationConf = applicationConf;
  }

  /**
   * Set the host for sending schema updates to
   *
   * @param s
   *          the host for sending schema updates to
   */
  public void setSchemaHost( String s ) {
    schemaHost = s;
  }

  /**
   * Set the host for sending schema updates to
   *
   * @return the host for sending schema updates to
   */
  public String getSchemaHost() {
    return schemaHost;
  }

  /**
   * Set the port for the schema update host
   *
   * @param p
   *          port for the schema update host
   */
  public void setSchemaPort( String p ) {
    schemaPort = p;
  }

  /**
   * Get the port for the schema update host
   *
   * @return port for the schema update host
   */
  public String getSchemaPort() {
    return schemaPort;
  }

  /**
   * Set how many sub-batches a batch should be split into when an insert times out.
   *
   * @param f
   *          the number of sub-batches to create when an insert times out.
   */
  public void setCQLSubBatchSize( String f ) {
    subBatchSize = f;
  }


  public boolean isSsl() {
    return ssl;
  }

  public void setSsl( boolean ssl ) {
    this.ssl = ssl;
  }

  public String getLocalDataCenter() {
    return localDataCenter;
  }

  public void setLocalDataCenter( String localDataCenter ) {
    this.localDataCenter = localDataCenter;
  }

  /**
   * Get how many sub-batches a batch should be split into when an insert times out.
   *
   * @return the number of sub-batches to create when an insert times out.
   */
  public String getCQLSubBatchSize() {
    return subBatchSize;
  }

  /**
   * Set the timeout for failing a batch insert attempt.
   *
   * @param t
   *          the time (milliseconds) to wait for a batch insert to succeed.
   */
  public void setCQLBatchInsertTimeout( String t ) {
    cqlBatchTimeout = t;
  }

  /**
   * Get the timeout for failing a batch insert attempt.
   *
   * @return the time (milliseconds) to wait for a batch insert to succeed.
   */
  public String getCQLBatchInsertTimeout() {
    return cqlBatchTimeout;
  }

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
   * Set whether to create the specified table if it doesn't already exist
   *
   * @param create
   *          true if the specified table is to be created if it doesn't already exist
   */
  public void setCreateTable( boolean create ) {
    createTable = create;
  }

  /**
   * Get whether to create the specified table if it doesn't already exist
   *
   * @return true if the specified table is to be created if it doesn't already exist
   */
  public boolean getCreateTable() {
    return createTable;
  }

  public void setCreateTableClause( String w ) {
    createTableWithClause = w;
  }

  public String getCreateTableWithClause() {
    return createTableWithClause;
  }

  /**
   * Set the consistency to use (e.g. ONE, QUORUM etc).
   *
   * @param consistency
   *          the consistency to use
   */
  public void setConsistency( String consistency ) {
    this.consistency = consistency;
  }

  /**
   * Get the consistency to use
   *
   * @return the consistency
   */
  public String getConsistency() {
    return consistency;
  }

  /**
   * Set the batch size to use (i.e. max rows to send via a CQL batch insert statement)
   *
   * @param batchSize
   *          the max number of rows to send in each CQL batch insert
   */
  public void setBatchSize( String batchSize ) {
    this.batchSize = batchSize;
  }

  /**
   * Get the batch size to use (i.e. max rows to send via a CQL batch insert statement)
   *
   * @return the batch size.
   */
  public String getBatchSize() {
    return batchSize;
  }

  /**
   * Set whether unlogged batch writes (non-atomic) are to be used
   *
   * @param u
   *          true if unlogged batch operations are to be used
   */
  public void setUseUnloggedBatches( boolean u ) {
    unloggedBatch = u;
  }

  /**
   * Get whether unlogged batch writes (non-atomic) are to be used
   *
   * @return true if unlogged batch operations are to be used
   */
  public boolean getUseUnloggedBatch() {
    return unloggedBatch;
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
   * Set whether or not to insert any incoming fields that are not in the Cassandra table's column meta data. This has
   * no affect if the user has opted to first update the meta data with any unknown columns.
   *
   * @param insert
   *          true if incoming fields not found in the table's meta data are to be inserted (and validated according to
   *          the default validator for the table)
   */
  public void setInsertFieldsNotInMeta( boolean insert ) {
    insertFieldsNotInMeta = insert;
  }

  /**
   * Get whether or not to insert any incoming fields that are not in the Cassandra table's column meta data. This has
   * no affect if the user has opted to first update the meta data with any unknown columns.
   *
   * @return true if incoming fields not found in the table's meta data are to be inserted (and validated according to
   *         the default validator for the table)
   */
  public boolean getInsertFieldsNotInMeta() {
    return insertFieldsNotInMeta;
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
   * Set whether to update the table meta data with any unknown incoming columns
   *
   * @param u
   *          true if the meta data is to be updated with any unknown incoming columns
   */
  public void setUpdateCassandraMeta( boolean u ) {
    updateCassandraMeta = u;
  }

  /**
   * Get whether to update the table meta data with any unknown incoming columns
   *
   * @return true if the meta data is to be updated with any unknown incoming columns
   */
  public boolean getUpdateCassandraMeta() {
    return updateCassandraMeta;
  }

  /**
   * Set whether to first truncate (remove all data) the table before inserting.
   *
   * @param t
   *          true if the table is to be initially truncated.
   */
  public void setTruncateTable( boolean t ) {
    truncateTable = t;
  }

  /**
   * Get whether to first truncate (remove all data) the table before inserting.
   *
   * @return true if the table is to be initially truncated.
   */
  public boolean getTruncateTable() {
    return truncateTable;
  }

  /**
   * Set any cql statements (separated by ;'s) to execute before inserting the first row into the table. Can be
   * used to do tasks like creating secondary indexes on columns in the table.
   *
   * @param cql
   *          cql statements (separated by ;'s) to execute
   */
  public void setAprioriCQL( String cql ) {
    aprioriCql = cql;
  }

  /**
   * Get any cql statements (separated by ;'s) to execute before inserting the first row into the table. Can be
   * used to do tasks like creating secondary indexes on columns in the table.
   *
   * @return cql statements (separated by ;'s) to execute
   */
  public String getAprioriCQL() {
    return aprioriCql;
  }

  /**
   * Set whether to complain or not if any apriori CQL statements fail
   *
   * @param c
   *          true if failures should be ignored (but logged)
   */
  public void setDontComplainAboutAprioriCQLFailing( boolean c ) {
    dontComplainAboutAprioriCqlFailing = c;
  }

  /**
   * Get whether to complain or not if any apriori CQL statements fail
   *
   * @return true if failures should be ignored (but logged)
   */
  public boolean getDontComplainAboutAprioriCQLFailing() {
    return dontComplainAboutAprioriCqlFailing;
  }

  /**
   * Set the time to live for fields inserted. Null or empty indicates no TTL (i.e. fields don't expire).
   *
   * @param ttl
   *          the time to live to use
   */
  public void setTTL( String ttl ) {
    this.ttl = ttl;
  }

  /**
   * Get the time to live for fields inserted. Null or empty indicates no TTL (i.e. fields don't expire).
   *
   * @return the time to live to use
   */
  public String getTTL() {
    return ttl;
  }

  /**
   * Set the unit for the ttl
   *
   * @param unit
   *          the unit for the ttl
   */
  public void setTTLUnit( String unit ) {
    ttlUnit = unit;
  }

  /**
   * Get the unit for the ttl
   *
   * @return the unit for the ttl
   */
  public String getTTLUnit() {
    return ttlUnit;
  }

  @Override
  public String getXML() {
    StringBuffer retval = new StringBuffer();

    if ( !Utils.isEmpty( cassandraHost ) ) {
      retval.append( "\n    " ).append( //$NON-NLS-1$
          XMLHandler.addTagValue( "cassandra_host", cassandraHost ) ); //$NON-NLS-1$
    }

    if ( !Utils.isEmpty( cassandraPort ) ) {
      retval.append( "\n    " ).append( //$NON-NLS-1$
          XMLHandler.addTagValue( "cassandra_port", cassandraPort ) ); //$NON-NLS-1$
    }

    if ( !Utils.isEmpty( schemaHost ) ) {
      retval.append( "\n    " ).append( //$NON-NLS-1$
          XMLHandler.addTagValue( "schema_host", schemaHost ) ); //$NON-NLS-1$
    }

    if ( !Utils.isEmpty( schemaPort ) ) {
      retval.append( "\n    " ).append( //$NON-NLS-1$
          XMLHandler.addTagValue( "schema_port", schemaPort ) ); //$NON-NLS-1$
    }

    if ( !Utils.isEmpty( socketTimeout ) ) {
      retval.append( "\n    " ).append( //$NON-NLS-1$
          XMLHandler.addTagValue( "socket_timeout", socketTimeout ) ); //$NON-NLS-1$
    }

    if ( !Utils.isEmpty( password ) ) {
      retval.append( "\n    " ).append( //$NON-NLS-1$
          XMLHandler.addTagValue( "password", //$NON-NLS-1$
              Encr.encryptPasswordIfNotUsingVariables( password ) ) );
    }

    if ( !Utils.isEmpty( username ) ) {
      retval.append( "\n    " ).append( //$NON-NLS-1$
          XMLHandler.addTagValue( "username", username ) ); //$NON-NLS-1$
    }

    if ( !Utils.isEmpty( cassandraKeyspace ) ) {
      retval.append( "\n    " ).append( //$NON-NLS-1$
          XMLHandler.addTagValue( "cassandra_keyspace", cassandraKeyspace ) ); //$NON-NLS-1$
    }

    if ( !Utils.isEmpty( cassandraKeyspace ) ) {
      retval.append( "\n    " ).append( //$NON-NLS-1$
          XMLHandler.addTagValue( "cassandra_keyspace", cassandraKeyspace ) ); //$NON-NLS-1$
    }

    if ( !Utils.isEmpty( table ) ) {
      retval.append( "\n    " ).append( //$NON-NLS-1$
          XMLHandler.addTagValue( "table", table ) ); //$NON-NLS-1$
    }

    if ( !Utils.isEmpty( keyField ) ) {
      retval.append( "\n    " ).append( //$NON-NLS-1$
          XMLHandler.addTagValue( "key_field", keyField ) ); //$NON-NLS-1$
    }

    if ( !Utils.isEmpty( consistency ) ) {
      retval.append( "\n    " ).append( //$NON-NLS-1$
          XMLHandler.addTagValue( "consistency", consistency ) ); //$NON-NLS-1$
    }

    if ( !Utils.isEmpty( batchSize ) ) {
      retval.append( "\n    " ).append( //$NON-NLS-1$
          XMLHandler.addTagValue( "batch_size", batchSize ) ); //$NON-NLS-1$
    }

    if ( !Utils.isEmpty( cqlBatchTimeout ) ) {
      retval.append( "\n    " ).append( //$NON-NLS-1$
          XMLHandler.addTagValue( "cql_batch_timeout", cqlBatchTimeout ) ); //$NON-NLS-1$
    }

    if ( !Utils.isEmpty( subBatchSize ) ) {
      retval.append( "\n    " ).append( //$NON-NLS-1$
          XMLHandler.addTagValue( "cql_sub_batch_size", subBatchSize ) ); //$NON-NLS-1$
    }

    retval.append( "\n    " ).append( //$NON-NLS-1$
        XMLHandler.addTagValue( "create_table", createTable ) ); //$NON-NLS-1$

    retval.append( "\n    " ).append( //$NON-NLS-1$
        XMLHandler.addTagValue( "use_compression", useCompression ) ); //$NON-NLS-1$

    retval.append( "\n    " ).append( //$NON-NLS-1$
        XMLHandler.addTagValue( "insert_fields_not_in_meta", //$NON-NLS-1$
          insertFieldsNotInMeta ) );

    retval.append( "\n    " ).append( //$NON-NLS-1$
      XMLHandler.addTagValue( "ssl", //$NON-NLS-1$
        ssl ) );

    retval.append( "\n    " ).append( //$NON-NLS-1$
        XMLHandler.addTagValue( "update_cassandra_meta", updateCassandraMeta ) ); //$NON-NLS-1$

    retval.append( "\n    " ).append( //$NON-NLS-1$
        XMLHandler.addTagValue( "truncate_table", truncateTable ) ); //$NON-NLS-1$

    retval.append( "\n    " ).append( //$NON-NLS-1$
        XMLHandler.addTagValue( "unlogged_batch", unloggedBatch ) ); //$NON-NLS-1$

    retval.append( "\n    " ).append( //$NON-NLS-1$
        XMLHandler.addTagValue( "dont_complain_apriori_cql", //$NON-NLS-1$
          dontComplainAboutAprioriCqlFailing ) );

    retval.append( "\n    " ).append( //$NON-NLS-1$
      XMLHandler.addTagValue( "application_conf", //$NON-NLS-1$
        applicationConf ) );

    if ( !Utils.isEmpty( aprioriCql ) ) {
      retval.append( "\n    " ).append( //$NON-NLS-1$
          XMLHandler.addTagValue( "apriori_cql", aprioriCql ) ); //$NON-NLS-1$
    }

    if ( !Utils.isEmpty( createTableWithClause ) ) {
      retval.append( "\n    " ).append( //$NON-NLS-1$
          XMLHandler.addTagValue( "create_table_with_clause", //$NON-NLS-1$
            createTableWithClause ) );
    }

    if ( !Utils.isEmpty( ttl ) ) {
      retval.append( "\n    " ).append( XMLHandler.addTagValue( "ttl", ttl ) ); //$NON-NLS-1$ //$NON-NLS-2$
    }

    retval.append( "\n    " ).append( //$NON-NLS-1$
        XMLHandler.addTagValue( "ttl_unit", ttlUnit ) ); //$NON-NLS-1$

    retval.append( "\n    " ).append( //$NON-NLS-1$
      XMLHandler.addTagValue( "local_datacenter", localDataCenter ) ); //$NON-NLS-1$

    return retval.toString();
  }

  @Override
  public void loadXML( Node stepnode, List<DatabaseMeta> databases, Map<String, Counter> counters )
    throws KettleXMLException {
    cassandraHost = XMLHandler.getTagValue( stepnode, "cassandra_host" ); //$NON-NLS-1$
    cassandraPort = XMLHandler.getTagValue( stepnode, "cassandra_port" ); //$NON-NLS-1$
    schemaHost = XMLHandler.getTagValue( stepnode, "schema_host" ); //$NON-NLS-1$
    schemaPort = XMLHandler.getTagValue( stepnode, "schema_port" ); //$NON-NLS-1$
    socketTimeout = XMLHandler.getTagValue( stepnode, "socket_timeout" ); //$NON-NLS-1$
    username = XMLHandler.getTagValue( stepnode, "username" ); //$NON-NLS-1$
    password = XMLHandler.getTagValue( stepnode, "password" ); //$NON-NLS-1$
    applicationConf = XMLHandler.getTagValue( stepnode, "application_conf" ); //$NON-NLS-1$
    if ( !Utils.isEmpty( password ) ) {
      password = Encr.decryptPasswordOptionallyEncrypted( password );
    }
    ssl = "Y".equalsIgnoreCase( XMLHandler.getTagValue( stepnode, "ssl" ) );
    cassandraKeyspace = XMLHandler.getTagValue( stepnode, "cassandra_keyspace" ); //$NON-NLS-1$
    table = XMLHandler.getTagValue( stepnode, "table" ); //$NON-NLS-1$
    keyField = XMLHandler.getTagValue( stepnode, "key_field" ); //$NON-NLS-1$
    consistency = XMLHandler.getTagValue( stepnode, "consistency" ); //$NON-NLS-1$
    batchSize = XMLHandler.getTagValue( stepnode, "batch_size" ); //$NON-NLS-1$
    cqlBatchTimeout = XMLHandler.getTagValue( stepnode, "cql_batch_timeout" ); //$NON-NLS-1$
    subBatchSize = XMLHandler.getTagValue( stepnode, "cql_sub_batch_size" ); //$NON-NLS-1$

    // check legacy column family tag first
    String createColFamStr = XMLHandler.getTagValue( stepnode, "create_column_family" ); //$NON-NLS-1$ //$NON-NLS-2$
    createTable = !Utils.isEmpty( createColFamStr ) ? createColFamStr.equalsIgnoreCase( "Y" )
        : XMLHandler.getTagValue( stepnode, "create_table" ).equalsIgnoreCase( "Y" );

    useCompression = XMLHandler.getTagValue( stepnode, "use_compression" ) //$NON-NLS-1$
        .equalsIgnoreCase( "Y" ); //$NON-NLS-1$
    insertFieldsNotInMeta = XMLHandler.getTagValue( stepnode, "insert_fields_not_in_meta" ).equalsIgnoreCase( "Y" ); //$NON-NLS-1$ //$NON-NLS-2$
    updateCassandraMeta = XMLHandler.getTagValue( stepnode, "update_cassandra_meta" ).equalsIgnoreCase( "Y" ); //$NON-NLS-1$ //$NON-NLS-2$

    String truncateColFamStr = XMLHandler.getTagValue( stepnode, "truncate_column_family" ); //$NON-NLS-1$ //$NON-NLS-2$
    truncateTable = !Utils.isEmpty( truncateColFamStr ) ? truncateColFamStr.equalsIgnoreCase( "Y" )
        : XMLHandler.getTagValue( stepnode, "truncate_table" ).equalsIgnoreCase( "Y" ); //$NON-NLS-1$ //$NON-NLS-2$

    aprioriCql = XMLHandler.getTagValue( stepnode, "apriori_cql" ); //$NON-NLS-1$

    createTableWithClause = XMLHandler.getTagValue( stepnode, "create_table_with_clause" ); //$NON-NLS-1$

    String unloggedBatch = XMLHandler.getTagValue( stepnode, "unlogged_batch" ); //$NON-NLS-1$
    if ( !Utils.isEmpty( unloggedBatch ) ) {
      this.unloggedBatch = unloggedBatch.equalsIgnoreCase( "Y" ); //$NON-NLS-1$
    }

    String dontComplain = XMLHandler.getTagValue( stepnode, "dont_complain_apriori_cql" ); //$NON-NLS-1$
    if ( !Utils.isEmpty( dontComplain ) ) {
      dontComplainAboutAprioriCqlFailing = dontComplain.equalsIgnoreCase( "Y" ); //$NON-NLS-1$
    }

    ttl = XMLHandler.getTagValue( stepnode, "ttl" ); //$NON-NLS-1$
    ttlUnit = XMLHandler.getTagValue( stepnode, "ttl_unit" ); //$NON-NLS-1$

    if ( Utils.isEmpty( ttlUnit ) ) {
      ttlUnit = TTLUnits.NONE.toString();
    }

    localDataCenter = XMLHandler.getTagValue( stepnode, "local_datacenter" );
  }

  @Override
  public void readRep( Repository rep, ObjectId id_step, List<DatabaseMeta> databases, Map<String, Counter> counters )
    throws KettleException {
    cassandraHost = rep.getStepAttributeString( id_step, 0, "cassandra_host" ); //$NON-NLS-1$
    cassandraPort = rep.getStepAttributeString( id_step, 0, "cassandra_port" ); //$NON-NLS-1$
    schemaHost = rep.getStepAttributeString( id_step, 0, "schema_host" ); //$NON-NLS-1$
    schemaPort = rep.getStepAttributeString( id_step, 0, "schema_port" ); //$NON-NLS-1$
    socketTimeout = rep.getStepAttributeString( id_step, 0, "socket_timeout" ); //$NON-NLS-1$
    username = rep.getStepAttributeString( id_step, 0, "username" ); //$NON-NLS-1$
    password = rep.getStepAttributeString( id_step, 0, "password" ); //$NON-NLS-1$
    applicationConf = rep.getStepAttributeString( id_step, 0, "application_conf" ); //$NON-NLS-1$
    if ( !Utils.isEmpty( password ) ) {
      password = Encr.decryptPasswordOptionallyEncrypted( password );
    }
    ssl = rep.getStepAttributeBoolean( id_step, 0, "ssl" );
    cassandraKeyspace = rep.getStepAttributeString( id_step, 0, "cassandra_keyspace" ); //$NON-NLS-1$
    table = rep.getStepAttributeString( id_step, 0, "table" ); //$NON-NLS-1$
    // try legacy identifier if table does not work
    if ( Utils.isEmpty( table ) ) {
      table = rep.getStepAttributeString( id_step, 0, "column_family" ); //$NON-NLS-1$
    }
    keyField = rep.getStepAttributeString( id_step, 0, "key_field" ); //$NON-NLS-1$
    consistency = rep.getStepAttributeString( id_step, 0, "consistency" ); //$NON-NLS-1$
    batchSize = rep.getStepAttributeString( id_step, 0, "batch_size" ); //$NON-NLS-1$
    cqlBatchTimeout = rep.getStepAttributeString( id_step, 0, "cql_batch_timeout" ); //$NON-NLS-1$
    subBatchSize = rep.getStepAttributeString( id_step, 0, "cql_sub_batch_size" ); //$NON-NLS-1$

    try {
      createTable = rep.getStepAttributeBoolean( id_step, 0, "create_table" );
    } catch ( Exception e ) {
      // try legacy identifier
      createTable = rep.getStepAttributeBoolean( id_step, 0, "create_column_family" ); //$NON-NLS-1$
    }

    useCompression = rep.getStepAttributeBoolean( id_step, 0, "use_compression" ); //$NON-NLS-1$
    insertFieldsNotInMeta = rep.getStepAttributeBoolean( id_step, 0, "insert_fields_not_in_meta" ); //$NON-NLS-1$
    updateCassandraMeta = rep.getStepAttributeBoolean( id_step, 0, "update_cassandra_meta" ); //$NON-NLS-1$

    try {
      truncateTable = rep.getStepAttributeBoolean( id_step, 0, "truncate_table" ); //$NON-NLS-1$
    } catch ( Exception e ) {
      // try legacy identifier
      truncateTable = rep.getStepAttributeBoolean( id_step, 0, "truncate_column_family" ); //$NON-NLS-1$
    }

    unloggedBatch = rep.getStepAttributeBoolean( id_step, 0, "unlogged_batch" ); //$NON-NLS-1$

    aprioriCql = rep.getStepAttributeString( id_step, 0, "apriori_cql" ); //$NON-NLS-1$

    createTableWithClause = rep.getStepAttributeString( id_step, 0, "create_table_with_clause" ); //$NON-NLS-1$

    dontComplainAboutAprioriCqlFailing = rep.getStepAttributeBoolean( id_step, 0, "dont_complain_aprior_cql" ); //$NON-NLS-1$

    ttl = rep.getStepAttributeString( id_step, 0, "ttl" ); //$NON-NLS-1$
    ttlUnit = rep.getStepAttributeString( id_step, 0, "ttl_unit" ); //$NON-NLS-1$

    if ( Utils.isEmpty( ttlUnit ) ) {
      ttlUnit = TTLUnits.NONE.toString();
    }

    localDataCenter = rep.getStepAttributeString( id_step, 0, "local_datacenter" );
  }

  @Override
  public void saveRep( Repository rep, ObjectId id_transformation, ObjectId id_step ) throws KettleException {
    if ( !Utils.isEmpty( cassandraHost ) ) {
      rep.saveStepAttribute( id_transformation, id_step, 0, "cassandra_host", //$NON-NLS-1$
        cassandraHost );
    }

    if ( !Utils.isEmpty( cassandraPort ) ) {
      rep.saveStepAttribute( id_transformation, id_step, 0, "cassandra_port", //$NON-NLS-1$
        cassandraPort );
    }

    if ( !Utils.isEmpty( schemaHost ) ) {
      rep.saveStepAttribute( id_transformation, id_step, 0, "schema_host", //$NON-NLS-1$
        schemaHost );
    }

    if ( !Utils.isEmpty( schemaPort ) ) {
      rep.saveStepAttribute( id_transformation, id_step, 0, "schema_port", //$NON-NLS-1$
        schemaPort );
    }

    if ( !Utils.isEmpty( socketTimeout ) ) {
      rep.saveStepAttribute( id_transformation, id_step, 0, "socket_timeout", //$NON-NLS-1$
        socketTimeout );
    }

    if ( !Utils.isEmpty( username ) ) {
      rep.saveStepAttribute( id_transformation, id_step, 0, "username", //$NON-NLS-1$
        username );
    }

    if ( !Utils.isEmpty( password ) ) {
      rep.saveStepAttribute( id_transformation, id_step, 0, "password", //$NON-NLS-1$
          Encr.encryptPasswordIfNotUsingVariables( password ) );
    }

    if ( !Utils.isEmpty( cassandraKeyspace ) ) {
      rep.saveStepAttribute( id_transformation, id_step, 0, "cassandra_keyspace", cassandraKeyspace ); //$NON-NLS-1$
    }

    rep.saveStepAttribute( id_transformation, id_step, 0, "ssl", ssl );
    rep.saveStepAttribute( id_transformation, id_step, 0, "application_conf", applicationConf );

    if ( !Utils.isEmpty( table ) ) {
      rep.saveStepAttribute( id_transformation, id_step, 0, "table", //$NON-NLS-1$
        table );
    }

    if ( !Utils.isEmpty( keyField ) ) {
      rep.saveStepAttribute( id_transformation, id_step, 0, "key_field", //$NON-NLS-1$
        keyField );
    }

    if ( !Utils.isEmpty( consistency ) ) {
      rep.saveStepAttribute( id_transformation, id_step, 0, "consistency", //$NON-NLS-1$
        consistency );
    }

    if ( !Utils.isEmpty( batchSize ) ) {
      rep.saveStepAttribute( id_transformation, id_step, 0, "batch_size", //$NON-NLS-1$
        batchSize );
    }

    if ( !Utils.isEmpty( cqlBatchTimeout ) ) {
      rep.saveStepAttribute( id_transformation, id_step, 0, "cql_batch_timeout", //$NON-NLS-1$
        cqlBatchTimeout );
    }

    if ( !Utils.isEmpty( subBatchSize ) ) {
      rep.saveStepAttribute( id_transformation, id_step, 0, "cql_sub_batch_size", subBatchSize ); //$NON-NLS-1$
    }

    rep.saveStepAttribute( id_transformation, id_step, 0, "create_table", createTable ); //$NON-NLS-1$
    rep.saveStepAttribute( id_transformation, id_step, 0, "use_compression", //$NON-NLS-1$
      useCompression );
    rep.saveStepAttribute( id_transformation, id_step, 0, "insert_fields_not_in_meta", insertFieldsNotInMeta ); //$NON-NLS-1$
    rep.saveStepAttribute( id_transformation, id_step, 0, "update_cassandra_meta", updateCassandraMeta ); //$NON-NLS-1$
    rep.saveStepAttribute( id_transformation, id_step, 0, "truncate_table", truncateTable ); //$NON-NLS-1$
    rep.saveStepAttribute( id_transformation, id_step, 0, "unlogged_batch", //$NON-NLS-1$
      unloggedBatch );

    if ( !Utils.isEmpty( aprioriCql ) ) {
      rep.saveStepAttribute( id_transformation, id_step, 0, "apriori_cql", //$NON-NLS-1$
        aprioriCql );
    }

    if ( !Utils.isEmpty( createTableWithClause ) ) {
      rep.saveStepAttribute( id_transformation, id_step, 0, "create_table_with_clause", aprioriCql ); //$NON-NLS-1$
    }

    if ( !Utils.isEmpty( ttl ) ) {
      rep.saveStepAttribute( id_transformation, id_step, 0, "ttl", ttl ); //$NON-NLS-1$
    }

    rep.saveStepAttribute( id_transformation, id_step, 0, "ttl_unit", ttlUnit ); //$NON-NLS-1$

    rep.saveStepAttribute( id_transformation, id_step, 0,
        "dont_complain_apriori_cql", dontComplainAboutAprioriCqlFailing ); //$NON-NLS-1$

    rep.saveStepAttribute( id_transformation, id_step, 0, "local_datacenter", localDataCenter );
  }

  @Override
  public void check( List<CheckResultInterface> remarks, TransMeta transMeta, StepMeta stepMeta, RowMetaInterface prev,
      String[] input, String[] output, RowMetaInterface info ) {

    CheckResult cr;

    if ( ( prev == null ) || ( prev.size() == 0 ) ) {
      cr = new CheckResult( CheckResult.TYPE_RESULT_WARNING, "Not receiving any fields from previous steps!", stepMeta ); //$NON-NLS-1$
      remarks.add( cr );
    } else {
      cr = new CheckResult( CheckResult.TYPE_RESULT_OK, "Step is connected to previous one, receiving " + prev.size() //$NON-NLS-1$
          + " fields", stepMeta ); //$NON-NLS-1$
      remarks.add( cr );
    }

    // See if we have input streams leading to this step!
    if ( input.length > 0 ) {
      cr = new CheckResult( CheckResult.TYPE_RESULT_OK, "Step is receiving info from other steps.", stepMeta ); //$NON-NLS-1$
      remarks.add( cr );
    } else {
      cr = new CheckResult( CheckResult.TYPE_RESULT_ERROR, "No input received from other steps!", stepMeta ); //$NON-NLS-1$
      remarks.add( cr );
    }
  }

  @Override
  public StepInterface getStep( StepMeta stepMeta, StepDataInterface stepDataInterface, int copyNr,
      TransMeta transMeta, Trans trans ) {

    return new CassandraOutput( stepMeta, stepDataInterface, copyNr, transMeta, trans );
  }

  @Override
  public StepDataInterface getStepData() {
    return new CassandraOutputData();
  }

  @Override
  public void setDefault() {
    cassandraHost = "localhost"; //$NON-NLS-1$
    cassandraPort = "9042"; //$NON-NLS-1$
    schemaHost = "localhost"; //$NON-NLS-1$
    schemaPort = "9042"; //$NON-NLS-1$
    table = ""; //$NON-NLS-1$
    batchSize = "100"; //$NON-NLS-1$
    useCompression = false;
    insertFieldsNotInMeta = false;
    updateCassandraMeta = false;
    truncateTable = false;
    aprioriCql = ""; //$NON-NLS-1$
    ssl = false;
    localDataCenter = "datacenter1";
  }

  /*
   * (non-Javadoc)
   *
   * @see org.pentaho.di.trans.step.BaseStepMeta#getDialogClassName()
   */
  @Override
  public String getDialogClassName() {
    return "org.pentaho.di.trans.steps.cassandraoutput.CassandraOutputDialog"; //$NON-NLS-1$
  }

  @Override
  public boolean supportsErrorHandling() {
    return true;
  }
}
