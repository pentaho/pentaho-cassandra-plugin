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

package org.pentaho.cassandra;

import java.io.ByteArrayOutputStream;
import java.math.BigDecimal;
import java.nio.ByteBuffer;
import java.nio.charset.Charset;
import java.util.ArrayList;
import java.util.Collection;
import java.util.Date;
import java.util.HashMap;
import java.util.LinkedHashSet;
import java.util.List;
import java.util.Map;
import java.util.zip.Deflater;

import org.apache.cassandra.db.marshal.BooleanType;
import org.apache.cassandra.db.marshal.DateType;
import org.apache.cassandra.db.marshal.DecimalType;
import org.apache.cassandra.db.marshal.DoubleType;
import org.apache.cassandra.db.marshal.LongType;
import org.apache.cassandra.db.marshal.UTF8Type;
import org.apache.cassandra.thrift.Compression;
import org.pentaho.cassandra.spi.ColumnFamilyMetaData;
import org.pentaho.cassandra.spi.Connection;
import org.pentaho.di.core.exception.KettleException;
import org.pentaho.di.core.exception.KettleValueException;
import org.pentaho.di.core.logging.LogChannelInterface;
import org.pentaho.di.core.row.RowMetaInterface;
import org.pentaho.di.core.row.ValueMetaInterface;
import org.pentaho.di.core.util.Utils;
import org.pentaho.di.i18n.BaseMessages;

import com.datastax.driver.core.DataType;
import com.google.common.base.Joiner;

/**
 * Static utility routines for various stuff
 * 
 * @author Mark Hall (mhall{[at]}pentaho{[dot]}com)
 */
public class CassandraUtils {

  protected static final Class<?> PKG = CassandraUtils.class;

  public static class ConnectionOptions {
    public static final String SOCKET_TIMEOUT = "socketTimeout"; //$NON-NLS-1$
    public static final String MAX_LENGTH = "maxLength"; //$NON-NLS-1$
    public static final String COMPRESSION = "compression";
  }

  public static class CQLOptions {
    public static final String CQLVERSION_OPTION = "cqlVersion"; //$NON-NLS-1$

    /**
     * The highest release of CQL 3 supported by Datastax Cassandra v1.2.3 at time of coding
     */
    public static final String CQL3_STRING = "3.0.1"; //$NON-NLS-1$

    public static final String CQL2_STRING = "2.0.0"; //$NON-NLS-1$
  }

  public static class BatchOptions {
    public static final String BATCH_TIMEOUT = "batchTimeout"; //$NON-NLS-1$
    public static final String TTL = "TTL";
  }

  /**
   * Return the Cassandra CQL column/key type for the given Kettle column. We use this type for CQL create column family
   * statements since, for some reason, the internal type isn't recognized for the key. Internal types *are* recognized
   * for column definitions. The CQL reference guide states that fully qualified (or relative to
   * org.apache.cassandra.db.marshal) class names can be used instead of CQL types - however, using these when defining
   * the key type always results in BytesType getting set for the key for some reason.
   * 
   * @param vm
   *          the ValueMetaInterface for the Kettle column
   * @return the corresponding CQL type
   */
  public static String getCQLTypeForValueMeta( ValueMetaInterface vm ) {
    switch ( vm.getType() ) {
      case ValueMetaInterface.TYPE_STRING:
        return "varchar"; //$NON-NLS-1$
      case ValueMetaInterface.TYPE_BIGNUMBER:
        return "decimal"; //$NON-NLS-1$
      case ValueMetaInterface.TYPE_BOOLEAN:
        return "boolean"; //$NON-NLS-1$
      case ValueMetaInterface.TYPE_INTEGER:
        return "bigint"; //$NON-NLS-1$
      case ValueMetaInterface.TYPE_NUMBER:
        return "double"; //$NON-NLS-1$
      case ValueMetaInterface.TYPE_DATE:
      case ValueMetaInterface.TYPE_TIMESTAMP:
        return "timestamp"; //$NON-NLS-1$
      case ValueMetaInterface.TYPE_BINARY:
      case ValueMetaInterface.TYPE_SERIALIZABLE:
        return "blob"; //$NON-NLS-1$
    }

    return "blob"; //$NON-NLS-1$
  }

  public static DataType getCassandraDataTypeFromValueMeta( ValueMetaInterface vm ) {
    switch ( vm.getType() ) {
      case ValueMetaInterface.TYPE_STRING:
        return DataType.varchar();
      case ValueMetaInterface.TYPE_BIGNUMBER:
        return DataType.decimal();
      case ValueMetaInterface.TYPE_BOOLEAN:
        return DataType.cboolean();
      case ValueMetaInterface.TYPE_INTEGER:
        return DataType.bigint();
      case ValueMetaInterface.TYPE_NUMBER:
        return DataType.cdouble();
      case ValueMetaInterface.TYPE_DATE:
      case ValueMetaInterface.TYPE_TIMESTAMP:
        return DataType.timestamp();
      case ValueMetaInterface.TYPE_BINARY:
      case ValueMetaInterface.TYPE_SERIALIZABLE:
      default:
        return DataType.blob();
    }
  }
  /**
   * Split a script containing one or more CQL statements (terminated by ;'s) into a list of individual statements.
   * 
   * @param source
   *          the source script
   * @return a list of individual CQL statements
   */
  public static List<String> splitCQLStatements( String source ) {
    String[] cqlStatements = source.split( ";" ); //$NON-NLS-1$
    List<String> individualStatements = new ArrayList<String>();

    if ( cqlStatements.length > 0 ) {
      for ( String cqlC : cqlStatements ) {
        cqlC = cqlC.trim();
        if ( !cqlC.endsWith( ";" ) ) { //$NON-NLS-1$
          cqlC += ";"; //$NON-NLS-1$
        }

        individualStatements.add( cqlC );
      }
    }

    return individualStatements;
  }

  /**
   * Compress a CQL query
   * 
   * @param queryStr
   *          the CQL query
   * @param compression
   *          compression option (GZIP is the only option - so far)
   * @return an array of bytes containing the compressed query
   */
  public static byte[] compressCQLQuery( String queryStr, Compression compression ) {
    byte[] data = queryStr.getBytes( Charset.forName( "UTF-8" ) ); //$NON-NLS-1$

    if ( compression != Compression.GZIP ) {
      return data;
    }

    Deflater compressor = new Deflater();
    compressor.setInput( data );
    compressor.finish();

    ByteArrayOutputStream byteArray = new ByteArrayOutputStream();
    byte[] buffer = new byte[1024];

    while ( !compressor.finished() ) {
      int size = compressor.deflate( buffer );
      byteArray.write( buffer, 0, size );
    }

    return byteArray.toByteArray();
  }

  /**
   * Extract the column family name (table name) from a CQL SELECT query. Assumes that any kettle variables have been
   * already substituted in the query
   * 
   * @param subQ
   *          the query with vars substituted
   * @return the column family name or null if the query is malformed
   */
  public static String getColumnFamilyNameFromCQLSelectQuery( String subQ ) {

    String result = null;

    if ( Utils.isEmpty( subQ ) ) {
      return null;
    }

    // assumes env variables already replaced in query!

    if ( !subQ.toLowerCase().startsWith( "select" ) ) { //$NON-NLS-1$
      // not a select statement!
      return null;
    }

    if ( subQ.indexOf( ';' ) < 0 ) {
      // query must end with a ';' or it will wait for more!
      return null;
    }

    // strip off where clause (if any)
    if ( subQ.toLowerCase().lastIndexOf( "where" ) > 0 ) { //$NON-NLS-1$
      subQ = subQ.substring( 0, subQ.toLowerCase().lastIndexOf( "where" ) ); //$NON-NLS-1$
    }

    // determine the source column family
    // look for a FROM that is surrounded by space
    int fromIndex = subQ.toLowerCase().indexOf( "from" ); //$NON-NLS-1$
    String tempS = subQ.toLowerCase();
    int offset = fromIndex;
    while ( fromIndex > 0 && tempS.charAt( fromIndex - 1 ) != ' ' && ( fromIndex + 4 < tempS.length() )
        && tempS.charAt( fromIndex + 4 ) != ' ' ) {
      tempS = tempS.substring( fromIndex + 4, tempS.length() );
      fromIndex = tempS.indexOf( "from" ); //$NON-NLS-1$
      offset += ( 4 + fromIndex );
    }

    fromIndex = offset;

    if ( fromIndex < 0 ) {
      return null; // no from clause
    }

    result = subQ.substring( fromIndex + 4, subQ.length() ).trim();
    if ( result.indexOf( ' ' ) > 0 ) {
      result = result.substring( 0, result.indexOf( ' ' ) );
    } else {
      result = result.replace( ";", "" ); //$NON-NLS-1$ //$NON-NLS-2$
    }

    if ( result.length() == 0 ) {
      return null; // no column family specified
    }

    return result;
  }

  /**
   * Return a string representation of a Kettle row
   * 
   * @param row
   *          the row to return as a string
   * @return a string representation of the row
   */
  public static String rowToStringRepresentation( RowMetaInterface inputMeta, Object[] row ) {
    StringBuilder buff = new StringBuilder();

    for ( int i = 0; i < inputMeta.size(); i++ ) {
      String sep = ( i > 0 ) ? "," : ""; //$NON-NLS-1$ //$NON-NLS-2$
      if ( row[i] == null ) {
        buff.append( sep ).append( "<null>" ); //$NON-NLS-1$
      } else {
        buff.append( sep ).append( row[i].toString() );
      }
    }

    return buff.toString();
  }

  /**
   * Checks for null row key and rows with no non-null values
   * 
   * @param inputMeta
   *          the input row meta
   * @param keyColNames
   *          the names of column(s) that are part of the row key
   * @param row
   *          the row to check
   * @param log
   *          logging
   * @return true if the row is OK
   * @throws KettleException
   *           if a problem occurs
   */
  protected static boolean preAddChecks( RowMetaInterface inputMeta, List<String> keyColNames, Object[] row,
      LogChannelInterface log ) throws KettleException {

    for ( String keyN : keyColNames ) {
      int keyIndex = inputMeta.indexOfValue( keyN );
      // check the key columns first
      ValueMetaInterface keyMeta = inputMeta.getValueMeta( keyIndex );
      if ( keyMeta.isNull( row[keyIndex] ) ) {
        log.logBasic( BaseMessages.getString( PKG,
            "CassandraUtils.Error.SkippingRowNullKey", rowToStringRepresentation( inputMeta, row ) ) ); //$NON-NLS-1$
        return false;
      }
    }

    StringBuilder fullKey = new StringBuilder();
    for ( String keyN : keyColNames ) {
      int keyIndex = inputMeta.indexOfValue( keyN );
      ValueMetaInterface keyMeta = inputMeta.getValueMeta( keyIndex );

      fullKey.append( keyMeta.getString( row[keyIndex] ) ).append( " " ); //$NON-NLS-1$
    }

    // quick scan to see if we have at least one non-null value apart from
    // the key
    if ( keyColNames.size() == 1 ) {
      boolean ok = false;
      for ( int i = 0; i < inputMeta.size(); i++ ) {
        String colName = inputMeta.getValueMeta( i ).getName();
        if ( !keyColNames.contains( colName ) ) {
          ValueMetaInterface v = inputMeta.getValueMeta( i );
          if ( !v.isNull( row[i] ) ) {
            ok = true;
            break;
          }
        }
      }
      if ( !ok ) {
        log.logBasic( BaseMessages.getString( PKG,
            "CassandraUtils.Error.SkippingRowNoNonNullValues", fullKey.toString() ) ); //$NON-NLS-1$
      }
      return ok;
    }

    return true;
  }

  /**
   * Creates a new batch for non-CQL based write operations
   * 
   * @param numRows
   *          the size of the batch in rows
   * @return the new batch
   */
  public static List<Object[]> newNonCQLBatch( int numRows ) {
    List<Object[]> newBatch = new ArrayList<Object[]>( numRows );

    return newBatch;
  }

  /**
   * Adds a row to the current non-CQL batch. Might not add a row if the row does not contain at least one non-null
   * value appart from the key.
   * 
   * @param batch
   *          the batch to add to
   * @param row
   *          the row to add to the batch
   * @param inputMeta
   *          the row format
   * @param familyMeta
   *          meta data on the columns in the cassandra column family (table)
   * @param insertFieldsNotInMetaData
   *          true if any Kettle fields that are not in the Cassandra column family (table) meta data are to be
   *          inserted. This is irrelevant if the user has opted to have the step initially update the Cassandra meta
   *          data for incoming fields that are not known about.
   * @param log
   *          for logging
   * @return true if the row was added to the batch
   * @throws Exception
   *           if a problem occurs
   */
  public static boolean addRowToNonCQLBatch( List<Object[]> batch, Object[] row, RowMetaInterface inputMeta,
      ColumnFamilyMetaData familyMeta, boolean insertFieldsNotInMetaData, LogChannelInterface log ) throws Exception {

    if ( !preAddChecks( inputMeta, familyMeta.getKeyColumnNames(), row, log ) ) {
      return false;
    }

    for ( int i = 0; i < inputMeta.size(); i++ ) {
      // if (i != keyIndex) {
      ValueMetaInterface colMeta = inputMeta.getValueMeta( i );
      String colName = colMeta.getName();
      if ( !familyMeta.columnExistsInSchema( colName ) && !insertFieldsNotInMetaData ) {
        // set this row value to null - nulls don't get inserted into
        // Cassandra
        row[i] = null;
      }
      // }
    }

    batch.add( row );

    return true;
  }

  /**
   * Begin a new batch cql statement
   * 
   * @param numRows
   *          the number of rows to be inserted in this batch
   * @param consistency
   *          the consistency (e.g. ONE, QUORUM etc.) to use, or null to use the default.
   * @param cql3
   *          true if this is a CQL 3 batch (CQL 3 does not use "WITH CONSISTENCY", and this is now set programatically
   *          at the driver level)
   * @param unloggedBatch
   *          true if this is to be an unlogged batch (CQL 3 only)
   * @return a StringBuilder initialized for the batch.
   */
  public static StringBuilder newCQLBatch( int numRows, String consistency, boolean cql3, boolean unloggedBatch ) {

    // make a stab at a reasonable initial capacity
    StringBuilder batch = new StringBuilder( numRows * 80 );
    if ( unloggedBatch ) {
      batch.append( "BEGIN UNLOGGED BATCH" ); //$NON-NLS-1$
    } else {
      batch.append( "BEGIN BATCH" ); //$NON-NLS-1$
    }

    if ( !cql3 && !Utils.isEmpty( consistency ) ) {
      batch.append( " USING CONSISTENCY " ).append( consistency ); //$NON-NLS-1$
    }

    batch.append( "\n" ); //$NON-NLS-1$

    return batch;
  }

  /**
   * Append the "APPLY BATCH" statement to complete the batch
   * 
   * @param batch
   *          the StringBuilder batch to complete
   */
  public static void completeCQLBatch( StringBuilder batch ) {
    batch.append( "APPLY BATCH" ); //$NON-NLS-1$
  }

  /**
   * Returns the quote character to use with a given major version of CQL
   * 
   * @param cqlMajVersion
   *          the major version of the CQL in use
   * @return the quote character that can be used to surround identifiers (e.g. column names).
   */
  public static String identifierQuoteChar( int cqlMajVersion ) {
    if ( cqlMajVersion >= 3 ) {
      return "\""; //$NON-NLS-1$
    }

    return "'"; //$NON-NLS-1$
  }

  /**
   * converts a kettle row to CQL insert statement and adds it to the batch
   * 
   * @param batch
   *          StringBuilder for collecting the batch CQL
   * @param colFamilyName
   *          the name of the column family (table) to insert into
   * @param inputMeta
   *          Kettle input row meta data inserting
   * @param row
   *          the Kettle row
   * @param familyMeta
   *          meta data on the columns in the cassandra column family (table)
   * @param insertFieldsNotInMetaData
   *          true if any Kettle fields that are not in the Cassandra column family (table) meta data are to be
   *          inserted. This is irrelevant if the user has opted to have the step initially update the Cassandra meta
   *          data for incoming fields that are not known about.
   * @param cqlMajVersion
   *          the major version number of the cql version to use
   * @param additionalOpts
   *          additional options for the insert statement
   * @param log
   *          for logging
   * @return true if the row was added to the batch
   * @throws Exception
   *           if a problem occurs
   */
  public static boolean addRowToCQLBatch( StringBuilder batch, String colFamilyName, RowMetaInterface inputMeta,
      Object[] row, ColumnFamilyMetaData familyMeta, boolean insertFieldsNotInMetaData, int cqlMajVersion,
      Map<String, String> additionalOpts, LogChannelInterface log ) throws Exception {

    if ( !preAddChecks( inputMeta, familyMeta.getKeyColumnNames(), row, log ) ) {
      return false;
    }

    // ValueMetaInterface keyMeta = inputMeta.getValueMeta(keyIndex);
    final String quoteChar = identifierQuoteChar( cqlMajVersion );
    List<String> keyColNames = familyMeta.getKeyColumnNames();

    Map<String, String> columnValues = new HashMap<String, String>();
    for ( int i = 0; i < inputMeta.size(); i++ ) {
      ValueMetaInterface colMeta = inputMeta.getValueMeta( i );
      String colName = colMeta.getName();
      if ( cqlMajVersion < 3 && colName.equals( "KEY" ) ) { //$NON-NLS-1$
        // key is a reserved work in CQL2 and is stored in lower case
        // in Cassandra's table metadata, but returned as upper case
        // in a thrift Column object. If our incoming Kettle field is
        // KEY then we need to lower case it or we won't find it in
        // our familyMeta
        if ( keyColNames.get( 0 ).equalsIgnoreCase( "key" ) ) {
          colName = "key";
        }
      }
      if ( !familyMeta.columnExistsInSchema( colName ) && !insertFieldsNotInMetaData ) {
        continue;
      }
      // don't insert if null!
      if ( colMeta.isNull( row[i] ) ) {
        continue;
      }

      columnValues.put( colName, kettleValueToCQL( colMeta, row[i], cqlMajVersion ) );
    }

    Collection<String> columnOrder;
    if ( cqlMajVersion >= 3 ) {
      // Quote column family name if version >=3 to enforce case sensitivity
      // http://www.datastax.com/documentation/cql/3.0/cql/cql_reference/ucase-lcase_r.html
      colFamilyName = cql3MixedCaseQuote( colFamilyName );
      // Column order does not matter
      columnOrder = columnValues.keySet();
    } else {
      // Key column has to be listed first for CQL 2
      columnOrder = new LinkedHashSet<String>();
      for ( String keyColName : keyColNames ) {
        // Add keys in given order
        if ( columnValues.containsKey( keyColName ) ) {
          columnOrder.add( keyColName );
        }
      }
      // Add remaining values
      columnOrder.addAll( columnValues.keySet() );
    }

    List<String> columns = new ArrayList<String>( columnOrder.size() );
    List<String> values = new ArrayList<String>( columnOrder.size() );
    for ( String column : columnOrder ) {
      columns.add( quoteChar + column + quoteChar );
      values.add( columnValues.get( column ) );
    }

    Joiner joiner = Joiner.on( ',' ).skipNulls();
    batch.append( "INSERT INTO " ).append( colFamilyName ).append( " (" );
    joiner.appendTo( batch, columns );
    batch.append( ") VALUES (" ); //$NON-NLS-1$
    joiner.appendTo( batch, values );
    batch.append( ")" ); //$NON-NLS-1$

    if ( containsInsertOptions( additionalOpts ) ) {
      batch.append( " USING " ); //$NON-NLS-1$

      boolean first = true;
      for ( Map.Entry<String, String> o : additionalOpts.entrySet() ) {
        if ( validInsertOption( o.getKey() ) ) {
          if ( first ) {
            batch.append( o.getKey() ).append( " " ).append( o.getValue() ); //$NON-NLS-1$
            first = false;
          } else {
            batch.append( " AND " ).append( o.getKey() ).append( " " ) //$NON-NLS-1$ //$NON-NLS-2$
                .append( o.getValue() );
          }
        }
      }
    }

    batch.append( "\n" ); //$NON-NLS-1$

    return true;
  }

  protected static boolean validInsertOption( String opt ) {
    return ( opt.equalsIgnoreCase( "ttl" ) || opt.equalsIgnoreCase( "timestamp" ) ); //$NON-NLS-1$ //$NON-NLS-2$
  }

  protected static boolean containsInsertOptions( Map<String, String> opts ) {
    for ( String opt : opts.keySet() ) {
      if ( validInsertOption( opt ) ) {
        return true;
      }
    }

    return false;
  }

  protected static String escapeSingleQuotes( String source ) {

    // escaped by doubling (as in SQL)
    return source.replace( "'", "''" ); //$NON-NLS-1$ //$NON-NLS-2$
  }

  /**
   * Remove enclosing quotes from a string. Useful for quoted mixed case CQL 3 identifiers where we want to remove the
   * quotes in order to match successfully against entries in various system tables
   * 
   * @param source
   *          the source string
   * @return the dequoted string
   */
  public static String removeQuotes( String source ) {
    String result = source;
    if ( source.startsWith( "\"" ) && source.endsWith( "\"" ) ) {
      result = result.substring( 1, result.length() - 1 );
    } else {

      // CQL3 is case insensitive unless quotes are used, so convert to lower case here
      // to match behavior
      result = result.toLowerCase();
    }

    return result;
  }

  /**
   * Quotes an identifier (for CQL 3) if it contains mixed case
   * 
   * @param source
   *          the source string
   * @return the quoted string
   */
  public static String cql3MixedCaseQuote( String source ) {
    if ( source.toLowerCase().equals( source ) || ( source.startsWith( "\"" ) && source.endsWith( "\"" ) ) ) {
      // no need for quotes
      return source;
    }

    return "\"" + source + "\"";
  }

  /**
   * Static utility method that converts a Kettle value into an appropriately encoded CQL string. Does not handle
   * collection types yet.
   * 
   * @param vm
   *          the ValueMeta for the Kettle value
   * @param value
   *          the actual Kettle value
   * @param cqlMajVersion
   *          the major version number of the CQL to use
   * @return an appropriately encoded CQL string representation of the value, suitable for using in an CQL query.
   * @throws KettleValueException
   *           if there is an error converting.
   */
  public static String kettleValueToCQL( ValueMetaInterface vm, Object value, int cqlMajVersion )
    throws KettleValueException {

    String quote = cqlMajVersion == 2 ? "'" : ""; //$NON-NLS-1$ //$NON-NLS-2$
    switch ( vm.getType() ) {
      case ValueMetaInterface.TYPE_STRING: {
        UTF8Type u = UTF8Type.instance;
        String toConvert = vm.getString( value );
        ByteBuffer decomposed = u.decompose( toConvert );
        String cassandraString = u.getString( decomposed );
        return "'" + escapeSingleQuotes( cassandraString ) + "'"; //$NON-NLS-1$ //$NON-NLS-2$
      }
      case ValueMetaInterface.TYPE_BIGNUMBER: {
        DecimalType dt = DecimalType.instance;
        BigDecimal toConvert = vm.getBigNumber( value );
        ByteBuffer decomposed = dt.decompose( toConvert );
        String cassandraString = dt.getString( decomposed );
        return quote + cassandraString + quote;
      }
      case ValueMetaInterface.TYPE_BOOLEAN: {
        BooleanType bt = BooleanType.instance;
        Boolean toConvert = vm.getBoolean( value );
        ByteBuffer decomposed = bt.decompose( toConvert );
        String cassandraString = bt.getString( decomposed );
        return quote + escapeSingleQuotes( cassandraString ) + quote;
      }
      case ValueMetaInterface.TYPE_INTEGER: {
        LongType lt = LongType.instance;
        Long toConvert = vm.getInteger( value );
        ByteBuffer decomposed = lt.decompose( toConvert );
        String cassandraString = lt.getString( decomposed );
        return quote + cassandraString + quote;
      }
      case ValueMetaInterface.TYPE_NUMBER: {
        DoubleType dt = DoubleType.instance;
        Double toConvert = vm.getNumber( value );
        ByteBuffer decomposed = dt.decompose( toConvert );
        String cassandraString = dt.getString( decomposed );
        return quote + cassandraString + quote;
      }
      case ValueMetaInterface.TYPE_DATE:
      case ValueMetaInterface.TYPE_TIMESTAMP:
        // milliseconds since epoch is supposedly OK
        // Date toConvert = vm.getDate(value);
        // LongType lt = LongType.instance;
        // Long ltC = new Long(toConvert.getTime());
        // ByteBuffer decomposed = lt.decompose(ltC);
        // String cassandraString = lt.getString(decomposed);
        // return quote + cassandraString + quote;

        // Use a formatted date string here instead of inserting the
        // long number of seconds since epoch. The reason for this is
        // that if we are inserting into a dynamic CQL2 column family
        // with default column validator that is text then we want the
        // actual date string stored rather than a long value
        DateType d = DateType.instance;
        Date toConvert = vm.getDate( value );
        ByteBuffer decomposed = d.decompose( toConvert );
        String cassandraFormattedDateString = d.getString( decomposed );
        return "'" + escapeSingleQuotes( cassandraFormattedDateString ) + "'"; //$NON-NLS-1$ //$NON-NLS-2$
      case ValueMetaInterface.TYPE_BINARY:
      case ValueMetaInterface.TYPE_SERIALIZABLE:

        // TODO blob constant (hex string) for TYPE_BINARY (see
        // http://cassandra.apache.org/doc/cql3/CQL.html)
        throw new KettleValueException( BaseMessages.getString( PKG, "CassandraUtils.Error.CantConvertBinaryToCQL" ) ); //$NON-NLS-1$
    }

    throw new KettleValueException( BaseMessages.getString( PKG,
        "CassandraUtils.Error.CantConvertType", vm.getName(), vm.getTypeDesc() ) ); //$NON-NLS-1$
  }

  /**
   * Return a one line string representation of an options map
   * 
   * @param opts
   *          the options to return as a string
   * @return a one line string representation of a map of options
   */
  public static String optionsToString( Map<String, String> opts ) {
    if ( opts.size() == 0 ) {
      return ""; //$NON-NLS-1$
    }

    StringBuilder optsBuilder = new StringBuilder();
    for ( Map.Entry<String, String> e : opts.entrySet() ) {
      optsBuilder.append( e.getKey() ).append( "=" ).append( e.getValue() ) //$NON-NLS-1$
          .append( " " ); //$NON-NLS-1$
    }

    return optsBuilder.toString();
  }

  /**
   * Returns how many fields (including the key) will be written given the incoming Kettle row format
   * 
   * @param inputMeta
   *          the incoming Kettle row format
   * @param keyIndex
   *          the index(es) of the key field in the incoming row format
   * @param cassandraMeta
   *          column family meta data
   * @param insertFieldsNotInMetaData
   *          true if incoming fields not explicitly defined in the column family schema are to be inserted
   * @return
   */
  public static int numFieldsToBeWritten( RowMetaInterface inputMeta, List<Integer> keyIndex,
      ColumnFamilyMetaData cassandraMeta, boolean insertFieldsNotInMetaData ) {

    // check how many fields will actually be inserted - we must insert at least
    // one field
    // apart from the key (CQL 2 only) or Cassandra will complain.

    int count = keyIndex.size(); // key(s)
    for ( int i = 0; i < inputMeta.size(); i++ ) {
      // if (i != keyIndex) {
      if ( !keyIndex.contains( i ) ) {
        ValueMetaInterface colMeta = inputMeta.getValueMeta( i );
        String colName = colMeta.getName();
        if ( !cassandraMeta.columnExistsInSchema( colName ) && !insertFieldsNotInMetaData ) {
          continue;
        }
        count++;
      }
    }

    return count;
  }

  /**
   * Get a connection to cassandra
   * 
   * @param host
   *          the hostname of a cassandra node
   * @param port
   *          the port that cassandra is listening on
   * @param username
   *          the username for (optional) authentication
   * @param password
   *          the password for (optional) authentication
   * @param driver
   *          the driver to use
   * @param opts
   *          the additional options to the driver
   * @return a connection to cassandra
   * @throws Exception
   *           if a problem occurs during connection
   */
  public static Connection getCassandraConnection( String host, int port, String username, String password,
      ConnectionFactory.Driver driver, Map<String, String> opts ) throws Exception {
    Connection conn = ConnectionFactory.getFactory().getConnection( driver );
    conn.setHosts( host );
    conn.setDefaultPort( port );
    conn.setUsername( username );
    conn.setPassword( password );
    conn.setAdditionalOptions( opts );

    return conn;
  }

  public static String[] getColumnNames( RowMetaInterface inputMeta ) {
    String[] columns = new String[inputMeta.size()];
    for ( int i = 0; i < inputMeta.size(); i++ ) {
      columns[i] = inputMeta.getValueMeta( i ).getName();
    }
    return columns;
  }

}
