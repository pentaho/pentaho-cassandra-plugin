/*******************************************************************************
 *
 * Pentaho Big Data
 *
 * Copyright (C) 2002-2017 by Pentaho : http://www.pentaho.com
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

package org.pentaho.cassandra.legacy;

import java.nio.ByteBuffer;
import java.nio.charset.Charset;
import java.util.ArrayList;
import java.util.List;
import java.util.Map;

import org.apache.cassandra.db.marshal.AbstractType;
import org.apache.cassandra.db.marshal.UTF8Type;
import org.apache.cassandra.thrift.CfDef;
import org.apache.cassandra.thrift.Column;
import org.apache.cassandra.thrift.ColumnDef;
import org.apache.cassandra.thrift.Compression;
import org.apache.cassandra.thrift.ConsistencyLevel;
import org.apache.cassandra.thrift.CqlResult;
import org.apache.cassandra.thrift.CqlRow;
import org.apache.cassandra.thrift.KsDef;
import org.pentaho.cassandra.CassandraUtils;
import org.pentaho.cassandra.spi.CQLRowHandler;
import org.pentaho.cassandra.spi.ColumnFamilyMetaData;
import org.pentaho.cassandra.spi.Connection;
import org.pentaho.cassandra.spi.Keyspace;
import org.pentaho.cassandra.spi.NonCQLRowHandler;
import org.pentaho.di.core.Const;
import org.pentaho.di.core.logging.LogChannelInterface;
import org.pentaho.di.core.row.RowMetaInterface;
import org.pentaho.di.core.row.ValueMetaInterface;
import org.pentaho.di.core.util.Utils;
import org.pentaho.di.i18n.BaseMessages;

/**
 * Implementation of Keyspace that wraps legacy routines
 * 
 * @author Mark Hall (mhall{[at]}pentaho{[dot]}com)
 */
public class LegacyKeyspace implements Keyspace {

  protected static final Class<?> PKG = LegacyKeyspace.class;

  protected CassandraConnection m_conn;
  protected Map<String, String> m_options;

  protected String m_currentKeyspace;

  protected boolean m_cql3 = false;

  /**
   * Set the connection for this keyspace to use
   * 
   * @param conn
   *          the connection to use
   * @throws Exception
   *           if a problem occurs
   */
  @Override
  public void setConnection( Connection conn ) {
    m_conn = (CassandraConnection) conn;

    m_currentKeyspace = m_conn.m_keyspaceName;
  }

  @Override
  public Connection getConnection() {
    return m_conn;
  }

  /**
   * Set the current keyspace
   * 
   * @param keyspaceName
   *          the name of the keyspace to use
   * @throws Exception
   *           if a problem occurs
   */
  @Override
  public void setKeyspace( String keyspaceName ) throws Exception {
    if ( m_conn != null ) {
      m_conn.setKeyspace( keyspaceName );
    }
    m_currentKeyspace = keyspaceName;

    // do a refresh on column family meta data
    // refresh();
  }

  /**
   * Set any special options for keyspace operations (e.g. compression of CQL)
   * 
   * @param options
   *          the options to use. Can be null.
   */
  @Override
  public void setOptions( Map<String, String> options ) {
    m_options = options;

    if ( m_options != null ) {
      for ( Map.Entry<String, String> e : m_options.entrySet() ) {
        if ( e.getKey().equalsIgnoreCase( CassandraUtils.CQLOptions.CQLVERSION_OPTION )
            && e.getValue().equals( CassandraUtils.CQLOptions.CQL3_STRING ) ) {
          m_cql3 = true;
        }
      }
    }
  }

  /**
   * Execute a CQL statement.
   * 
   * @param cql
   *          the CQL to execute
   * @param compression
   *          the compression to use (GZIP or NONE)
   * @param consistencyLevel
   *          the consistency level to use
   * @param log
   *          log to write to (may be null)
   * @throws UnsupportedOperationException
   *           if CQL is not supported by the underlying driver
   * @throws Exception
   *           if a problem occurs
   */
  @Override
  public void executeCQL( String cql, String compression, String consistencyLevel, LogChannelInterface log )
    throws UnsupportedOperationException, Exception {

    ConsistencyLevel c = ConsistencyLevel.ONE; // default for CQL
    Compression z = Compression.NONE;

    if ( !Const.isEmpty( consistencyLevel ) ) {
      c = ConsistencyLevel.valueOf( consistencyLevel );
    }
    if ( !Const.isEmpty( compression ) ) {
      if ( compression.equalsIgnoreCase( "gzip" ) ) { //$NON-NLS-1$
        z = Compression.GZIP;
      } else {
        z = Compression.NONE;
      }
    }

    byte[] queryBytes = CassandraUtils.compressCQLQuery( cql, z );

    if ( m_conn != null ) {
      if ( m_cql3 ) {
        // m_conn.getClient().set_cql_version(CQL3_VERSION);
        m_conn.getClient().execute_cql3_query( ByteBuffer.wrap( queryBytes ), z, c );
      } else {
        m_conn.getClient().execute_cql_query( ByteBuffer.wrap( queryBytes ), z );
      }
    }
  }

  /**
   * Get a list of the names of the column families in this keyspace
   * 
   * @return a list of column family names in the current keyspace
   * @throws Exception
   *           if a problem occurs
   */
  @Override
  public List<String> getColumnFamilyNames() throws Exception {

    if ( m_cql3 ) {
      return getColumnFamilyNamesCQL3();
    }

    KsDef keySpace = m_conn.describeKeyspace();
    List<CfDef> colFams = null;
    if ( keySpace != null ) {
      colFams = keySpace.getCf_defs();
    } else {
      throw new Exception( BaseMessages.getString( PKG,
          "LegacyKeyspace.Error.UnableToGetMetaDataForKeyspace", m_currentKeyspace ) ); //$NON-NLS-1$
    }

    List<String> colFamNames = new ArrayList<String>();
    for ( CfDef fam : colFams ) {
      colFamNames.add( fam.getName() );
    }

    return colFamNames;
  }

  /**
   * Queries CQL system.schema_columnfamilies table to retrieve all column families (including CQL tables that are not
   * exposed to Thrift)
   * 
   * @return a list of column family (table) names
   * @throws Exception
   *           if a problem occurs
   */
  protected List<String> getColumnFamilyNamesCQL3() throws Exception {
    ConsistencyLevel c = ConsistencyLevel.ONE; // default for CQL
    Compression z = Compression.NONE;
    List<String> colFamNames = new ArrayList<String>();

    String keyspaceName = m_currentKeyspace;

    String cqlQ = "select keyspace_name, columnfamily_name from system.schema_columnfamilies where keyspace_name='" //$NON-NLS-1$
        + keyspaceName + "';"; //$NON-NLS-1$
    byte[] data = cqlQ.getBytes( Charset.forName( "UTF-8" ) ); //$NON-NLS-1$

    CqlResult result = m_conn.m_client.execute_cql3_query( ByteBuffer.wrap( data ), z, c );

    List<CqlRow> rl = result.getRows();

    for ( CqlRow r : rl ) {
      List<Column> cols = r.getColumns();
      Column fN = cols.get( 1 );

      AbstractType deserializer = UTF8Type.instance;
      Object decodedFN = deserializer.compose( fN.bufferForValue() );

      colFamNames.add( decodedFN.toString() );
    }

    return colFamNames;
  }

  /**
   * Create a keyspace.
   * 
   * @param keyspaceName
   *          the name of the keyspace
   * @param options
   *          additional options (see http://www.datastax.com/docs/1.0/configuration /storage_configuration)
   * @param log
   *          log to write to (may be null)
   * @throws UnsupportedOperationException
   *           if the underlying driver does not support creating keyspaces
   * @throws Exception
   *           if a problem occurs
   */
  @Override
  public void createKeyspace( String keyspaceName, Map<String, Object> options, LogChannelInterface log )
    throws UnsupportedOperationException, Exception {
    throw new UnsupportedOperationException( "Legacy driver does not support keyspace creation" ); //$NON-NLS-1$
  }

  /**
   * Check to see if the named column family exists in the current keyspace
   * 
   * @param colFamName
   *          the column family name to check
   * @return true if the named column family exists in the current keyspace
   * @throws Exception
   *           if a problem occurs
   */
  @Override
  public boolean columnFamilyExists( String colFamName ) throws Exception {
    List<String> colFamNames = getColumnFamilyNames();
    return colFamNames.contains( m_cql3 ? CassandraUtils.removeQuotes( colFamName ) : colFamName );
  }

  /**
   * Get meta data for the named column family
   * 
   * @param familyName
   *          the name of the column family to get meta data for
   * @return the column family meta data
   * @throws Exception
   *           if the named column family does not exist in the keyspace or a problem occurs
   */
  @Override
  public ColumnFamilyMetaData getColumnFamilyMetaData( String familyName ) throws Exception {
    return new CassandraColumnMetaData( this, familyName, m_cql3 );
  }

  /**
   * Create a column family in the current keyspace.
   * 
   * @param colFamName
   *          the name of the column family to create
   * @param rowMeta
   *          the incoming fields to base the column family schema on
   * @param keyIndexes
   *          the index(es) of the incoming field(s) to use as the key
   * @param createTableWithClause
   *          any WITH clause to include when creating the table
   * @param log
   *          log to write to (may be null)
   * @return true if the column family was created successfully
   * @throws Exception
   *           if a problem occurs
   */
  @Override
  public boolean createColumnFamily( String colFamilyName, RowMetaInterface rowMeta, List<Integer> keyIndexes,
      String createTableWithClause, LogChannelInterface log ) throws Exception {

    if ( keyIndexes.size() > 1 && !m_cql3 ) {
      throw new Exception( BaseMessages.getString( PKG, "LegacyKeyspace.Error.OnlySingleColumnKeysAreSupported" ) ); //$NON-NLS-1$
    }

    String quoteChar = m_cql3 ? "\"" : "'"; //$NON-NLS-1$ //$NON-NLS-2$

    StringBuffer buff = new StringBuffer();
    buff.append( "CREATE TABLE " + ( m_cql3 ? CassandraUtils.cql3MixedCaseQuote( colFamilyName ) : colFamilyName ) ); //$NON-NLS-1$
    ValueMetaInterface kvm = rowMeta.getValueMeta( keyIndexes.get( 0 ) );
    buff.append( " (" ); //$NON-NLS-1$

    // apparently CQL2 can use the keyword KEY or an alias for the row key

    if ( !m_cql3 ) {
      buff.append( quoteChar + kvm.getName() + quoteChar ).append( " " + CassandraUtils.getCQLTypeForValueMeta( kvm ) ); //$NON-NLS-1$

      buff.append( " PRIMARY KEY" ); //$NON-NLS-1$
    }

    List<ValueMetaInterface> indexedVals = new ArrayList<ValueMetaInterface>();
    if ( rowMeta.size() > 1 ) {

      // boolean first = true;
      for ( int i = 0; i < rowMeta.size(); i++ ) {
        if ( i != keyIndexes.get( 0 ) || m_cql3 ) {
          ValueMetaInterface vm = rowMeta.getValueMeta( i );
          if ( vm.getStorageType() == ValueMetaInterface.STORAGE_TYPE_INDEXED ) {
            indexedVals.add( vm );
          }

          if ( log != null ) {
            log.logBasic( BaseMessages
                .getString(
                    PKG,
                    "LegacyKeyspace.Message.ValueMetaConversion", vm.getType(), CassandraUtils.getCQLTypeForValueMeta( vm ) ) ); //$NON-NLS-1$
          }

          String colName = vm.getName();
          String colType = CassandraUtils.getCQLTypeForValueMeta( vm );
          if ( !( i == 0 && m_cql3 ) ) {
            buff.append( ", " ); //$NON-NLS-1$
          }
          buff.append( quoteChar + colName + quoteChar ).append( " " ); //$NON-NLS-1$
          buff.append( colType );
        }
      }
    } else {
      if ( !m_cql3 ) {
        return false; // we can't insert any data if there is only the key
                      // coming
                      // into the step
      }
    }

    if ( m_cql3 ) {
      buff.append( ", PRIMARY KEY (" );
      for ( int i = 0; i < keyIndexes.size(); i++ ) {
        int ki = keyIndexes.get( i );
        ValueMetaInterface vm = rowMeta.getValueMeta( ki );
        buff.append( i == 0 ? "" : ", " ).append( quoteChar + vm.getName() + quoteChar );
      }
      buff.append( ")" );
    }

    if ( !Utils.isEmpty( createTableWithClause ) ) {
      buff.append( ") " );
      if ( !createTableWithClause.toLowerCase().trim().startsWith( "with" ) ) {
        buff.append( "WITH " );
      }

      buff.append( createTableWithClause );
    }

    // abuse the comment field (if we can) to store any indexed values :-)
    if ( indexedVals.size() == 0 ) {
      if ( Utils.isEmpty( createTableWithClause ) ) {
        buff.append( ");" ); //$NON-NLS-1$
      }
    } else {
      boolean ok = false;
      if ( Utils.isEmpty( createTableWithClause ) ) {
        buff.append( ") WITH comment = '@@@" ); //$NON-NLS-1$
        ok = true;
      } else if ( !createTableWithClause.toLowerCase().contains( "comment" ) ) {
        buff.append( " AND comment = '@@@" ); //$NON-NLS-1$
        ok = true;
      }

      if ( ok ) {
        int count = 0;
        for ( ValueMetaInterface vm : indexedVals ) {
          String colName = vm.getName();
          Object[] legalVals = vm.getIndex();
          buff.append( colName ).append( ":{" ); //$NON-NLS-1$
          for ( int i = 0; i < legalVals.length; i++ ) {
            buff.append( legalVals[i].toString() );
            if ( i != legalVals.length - 1 ) {
              buff.append( "," ); //$NON-NLS-1$
            }
          }
          buff.append( "}" ); //$NON-NLS-1$
          if ( count != indexedVals.size() - 1 ) {
            buff.append( ";" ); //$NON-NLS-1$
          }
          count++;
        }
        buff.append( "@@@';" ); //$NON-NLS-1$
      }
    }

    if ( !buff.toString().endsWith( ";" ) ) {
      buff.append( ";" );
    }

    if ( log != null ) {
      log.logBasic( BaseMessages.getString( PKG,
          "LegacyKeyspace.Message.CreatingColumnFamily", colFamilyName, buff.toString() ) ); //$NON-NLS-1$
    }

    executeCQL( buff.toString(), null, null, log );

    return true;
  }

  protected void updateColumnFamilyCQL3( String colFamName, RowMetaInterface rowMeta, List<Integer> keyIndexes,
      LogChannelInterface log ) throws Exception {

    // we should have this column family in our meta data
    CassandraColumnMetaData cassandraMeta = (CassandraColumnMetaData) getColumnFamilyMetaData( colFamName );

    if ( cassandraMeta == null ) {
      throw new Exception( BaseMessages.getString( PKG, "LegacyKeyspace.Error.CantUpdateMetaData", colFamName ) ); //$NON-NLS-1$
    }

    String alter = "ALTER TABLE " + colFamName + " ADD "; //$NON-NLS-1$ //$NON-NLS-2$

    for ( int i = 0; i < rowMeta.size(); i++ ) {
      ValueMetaInterface vm = rowMeta.getValueMeta( i );
      if ( !keyIndexes.contains( vm.getName() ) && !cassandraMeta.columnExistsInSchema( vm.getName() ) ) {
        String alter2 = alter + CassandraUtils.cql3MixedCaseQuote( vm.getName() ) + " " //$NON-NLS-1$
            + CassandraUtils.getCQLTypeForValueMeta( vm ) + ";"; //$NON-NLS-1$

        log.logBasic( "Exeucting: " + alter2 ); //$NON-NLS-1$
        this.executeCQL( alter2, null, null, log );
      }
    }
  }

  /**
   * Update the named column family with any incoming fields that are not present in its schema already
   * 
   * @param colFamName
   *          the name of the column family to update
   * @param rowMeta
   *          the incoming row meta data
   * @param keyIndexes
   *          the index(es) of the incoming field(s) that make up the key
   * @param log
   *          the log to write to (may be null)
   * @throws UnsupportedOperationException
   *           if the underlying driver does not support updating column family schema information
   * @throws Exception
   *           if a problem occurs
   */
  @Override
  public void updateColumnFamily( String colFamilyName, RowMetaInterface rowMeta, List<Integer> keyIndexes,
      LogChannelInterface log ) throws UnsupportedOperationException, Exception {

    if ( m_cql3 ) {
      updateColumnFamilyCQL3( colFamilyName, rowMeta, keyIndexes, log );
      return;
    }

    if ( keyIndexes.size() > 1 || keyIndexes.size() == 0 ) {
      throw new Exception( BaseMessages.getString( PKG, "LegacyKeyspace.Error.OnlySingleColumnKeysAreSupported" ) ); //$NON-NLS-1$
    }

    // column families
    KsDef keySpace = m_conn.describeKeyspace();
    List<CfDef> colFams = null;
    if ( keySpace != null ) {
      colFams = keySpace.getCf_defs();
    } else {
      throw new Exception( BaseMessages.getString( PKG,
          "LegacyKeyspace.Error.UnableToGetColumnFamilyMetaData", colFamilyName ) ); //$NON-NLS-1$
    }

    // we should have this column family in our meta data then
    CassandraColumnMetaData cassandraMeta = (CassandraColumnMetaData) getColumnFamilyMetaData( colFamilyName );

    if ( cassandraMeta == null ) {
      throw new Exception( BaseMessages.getString( PKG,
          "LegacyKeyspace.Error.UnableToGetColumnFamilyMetaData", colFamilyName ) ); //$NON-NLS-1$
    }

    // look for the requested column family
    CfDef colFamDefToUpdate = null;

    for ( CfDef fam : colFams ) {
      String columnFamilyName = fam.getName(); // table name
      if ( columnFamilyName.equals( colFamilyName ) ) {
        colFamDefToUpdate = fam;
        break;
      }
    }

    if ( colFamDefToUpdate == null ) {
      throw new Exception( BaseMessages.getString( PKG, "LegacyKeyspace.Error.CantUpdateMetaData", colFamilyName ) ); //$NON-NLS-1$
    }

    String comment = colFamDefToUpdate.getComment();

    List<ValueMetaInterface> indexedVals = new ArrayList<ValueMetaInterface>();
    for ( int i = 0; i < rowMeta.size(); i++ ) {
      if ( i != keyIndexes.get( 0 ) ) {
        ValueMetaInterface colMeta = rowMeta.getValueMeta( i );
        if ( colMeta.getStorageType() == ValueMetaInterface.STORAGE_TYPE_INDEXED ) {
          indexedVals.add( colMeta );
        }
        String colName = colMeta.getName();
        if ( !cassandraMeta.columnExistsInSchema( colName ) ) {
          String colType = CassandraColumnMetaData.getCassandraTypeForValueMeta( colMeta );

          ColumnDef newCol = new ColumnDef( ByteBuffer.wrap( colName.getBytes() ), colType );
          colFamDefToUpdate.addToColumn_metadata( newCol );
        }
      }
    }

    // update the comment fields for any new indexed vals
    if ( indexedVals.size() > 0 ) {
      String before = ""; //$NON-NLS-1$
      String after = ""; //$NON-NLS-1$
      String meta = ""; //$NON-NLS-1$
      if ( comment != null && comment.length() > 0 ) {
        // is there any indexed value meta data there already?
        if ( comment.indexOf( "@@@" ) >= 0 ) { //$NON-NLS-1$
          // have to strip out existing stuff
          before = comment.substring( 0, comment.indexOf( "@@@" ) ); //$NON-NLS-1$
          after = comment.substring( comment.lastIndexOf( "@@@" ) + 3, //$NON-NLS-1$
              comment.length() );
          meta = comment.substring( comment.indexOf( "@@@", //$NON-NLS-1$
              comment.lastIndexOf( "@@@" ) ) ); //$NON-NLS-1$
          meta = meta.replace( "@@@", "" ); //$NON-NLS-1$ //$NON-NLS-2$
        }
      }

      StringBuffer buff = new StringBuffer();
      buff.append( meta );
      for ( ValueMetaInterface vm : indexedVals ) {
        String colName = vm.getName();
        if ( meta.indexOf( colName ) < 0 ) {
          // add this one
          Object[] legalVals = vm.getIndex();
          if ( buff.length() > 0 ) {
            buff.append( ";" ).append( colName ).append( ":{" ); //$NON-NLS-1$ //$NON-NLS-2$
          } else {
            buff.append( colName ).append( ":{" ); //$NON-NLS-1$
          }
          for ( int i = 0; i < legalVals.length; i++ ) {
            buff.append( legalVals[i].toString() );
            if ( i != legalVals.length - 1 ) {
              buff.append( "," ); //$NON-NLS-1$
            }
          }
          buff.append( "}" ); //$NON-NLS-1$
        }
      }

      comment = before + "@@@" + buff.toString() + "@@@" + after; //$NON-NLS-1$ //$NON-NLS-2$
      colFamDefToUpdate.setComment( comment );
    }

    m_conn.getClient().system_update_column_family( colFamDefToUpdate );
  }

  /**
   * Truncate the named column family.
   * 
   * @param colFamName
   *          the name of the column family to truncate
   * @param log
   *          log to write to (may be null)
   * @throws UnsupportedOperationException
   *           if the underlying driver does not support truncating a column family
   * @throws Exception
   *           if a problem occurs
   */
  @Override
  public void truncateColumnFamily( String colFamName, LogChannelInterface log ) throws UnsupportedOperationException,
    Exception {

    String cqlCommand = "TRUNCATE " + colFamName; //$NON-NLS-1$
    if ( log != null ) {
      log.logBasic( BaseMessages.getString( PKG, "LegacyKeyspace.Message.TruncateColumnFamily", colFamName ) ); //$NON-NLS-1$
    }

    executeCQL( cqlCommand, null, null, log );
  }

  @Override
  public CQLRowHandler getCQLRowHandler() {
    LegacyCQLRowHandler rowHandler = new LegacyCQLRowHandler();

    rowHandler.setKeyspace( this );
    rowHandler.setOptions( m_options );

    return rowHandler;
  }

  @Override
  public NonCQLRowHandler getNonCQLRowHandler() {
    LegacyNonCQLRowHandler rowHandler = new LegacyNonCQLRowHandler();
    rowHandler.setKeyspace( this );
    rowHandler.setOptions( m_options );

    return rowHandler;
  }
}
