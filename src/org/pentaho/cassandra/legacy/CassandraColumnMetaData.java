/*******************************************************************************
 *
 * Pentaho Big Data
 *
 * Copyright (C) 2002-2012 by Pentaho : http://www.pentaho.com
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
import java.util.HashMap;
import java.util.HashSet;
import java.util.Iterator;
import java.util.LinkedHashMap;
import java.util.List;
import java.util.Map;
import java.util.Set;
import java.util.UUID;

import org.apache.cassandra.db.marshal.AbstractType;
import org.apache.cassandra.db.marshal.AsciiType;
import org.apache.cassandra.db.marshal.BooleanType;
import org.apache.cassandra.db.marshal.CompositeType;
import org.apache.cassandra.db.marshal.DateType;
import org.apache.cassandra.db.marshal.DecimalType;
import org.apache.cassandra.db.marshal.DoubleType;
import org.apache.cassandra.db.marshal.DynamicCompositeType;
import org.apache.cassandra.db.marshal.FloatType;
import org.apache.cassandra.db.marshal.Int32Type;
import org.apache.cassandra.db.marshal.IntegerType;
import org.apache.cassandra.db.marshal.LexicalUUIDType;
import org.apache.cassandra.db.marshal.LongType;
import org.apache.cassandra.db.marshal.TypeParser;
import org.apache.cassandra.db.marshal.UTF8Type;
import org.apache.cassandra.db.marshal.UUIDType;
import org.apache.cassandra.thrift.CfDef;
import org.apache.cassandra.thrift.Column;
import org.apache.cassandra.thrift.ColumnDef;
import org.apache.cassandra.thrift.Compression;
import org.apache.cassandra.thrift.ConsistencyLevel;
import org.apache.cassandra.thrift.CqlResult;
import org.apache.cassandra.thrift.CqlRow;
import org.apache.cassandra.thrift.KeySlice;
import org.apache.cassandra.thrift.KsDef;
import org.pentaho.cassandra.spi.ColumnFamilyMetaData;
import org.pentaho.cassandra.spi.Keyspace;
import org.pentaho.di.core.Const;
import org.pentaho.di.core.exception.KettleException;
import org.pentaho.di.core.row.ValueMeta;
import org.pentaho.di.core.row.ValueMetaInterface;
import org.pentaho.di.i18n.BaseMessages;

/**
 * Class encapsulating read-only schema information for a column family. Has
 * utility routines for converting between Cassandra meta data and Kettle meta
 * data, and for deserializing values.
 * 
 * @author Mark Hall (mhall{[at]}pentaho{[dot]}com)
 */
public class CassandraColumnMetaData implements ColumnFamilyMetaData {

  protected static final Class<?> PKG = CassandraColumnMetaData.class;

  public static final String UTF8 = "UTF-8"; //$NON-NLS-1$

  /** Name of the column family this meta data refers to */
  protected String m_columnFamilyName;

  /** The name of the column(s) that make up the key */
  protected List<String> m_keyColumnNames;

  /** Type of the partition (first element of a compound key) */
  protected String m_keyValidator;

  /** Type of the column names (used for sorting columns) */
  protected String m_columnComparator;

  /** m_columnComparator converted to Charset encoding string */
  protected String m_columnNameEncoding;

  /**
   * Default validator for the column family (table) - we can use this as the
   * type for any columns specified in a SELECT clause which *arent* in the meta
   * data
   */
  protected String m_defaultValidationClass;

  /** Map of column names + cassandra types (decoder classes) */
  protected Map<String, String> m_columnMeta;

  /**
   * Convenience map that stores ValueMeta objects that correspond to the
   * columns
   */
  protected Map<String, ValueMetaInterface> m_kettleColumnMeta;

  /** Map of column names to indexed values (if any) */
  protected Map<String, HashSet<Object>> m_indexedVals;

  /** Holds the schema textual description */
  protected StringBuffer m_schemaDescription;

  /** The keypsace that this column family (table) belongs to */
  protected LegacyKeyspace m_keyspace;

  /**
   * True if CQL3 is in use, in which case meta data is gathered differently
   * than for CQL 2 tables
   */
  protected boolean m_cql3;

  /**
   * Constructor
   * 
   * @param keyspace the keypsace to use
   * @param columnFamily the name of the column family (table)
   * @param cql3 true if the table is a CQL 3 table
   * @throws Exception if a problem occurs
   */
  public CassandraColumnMetaData(LegacyKeyspace keyspace, String columnFamily,
      boolean cql3) throws Exception {
    m_cql3 = cql3;
    m_keyspace = keyspace;
    m_columnFamilyName = columnFamily;

    refresh((CassandraConnection) m_keyspace.getConnection());
  }

  /*
   * (non-Javadoc)
   * 
   * @see
   * org.pentaho.cassandra.spi.ColumnFamilyMetaData#setKeyspace(org.pentaho.
   * cassandra.spi.Keyspace)
   */
  public void setKeyspace(Keyspace k) {
    m_keyspace = (LegacyKeyspace) k;
  }

  /*
   * (non-Javadoc)
   * 
   * @see
   * org.pentaho.cassandra.spi.ColumnFamilyMetaData#setColumnFamilyName(java
   * .lang.String)
   */
  public void setColumnFamilyName(String colFamName) {
    m_columnFamilyName = colFamName;
  }

  /**
   * Get the default validator for this column family
   * 
   * @return the default validator
   */
  public String getDefaultValidationClass() {
    return m_defaultValidationClass;
  }

  // NOTE - CQL3 mode will not correctly read a column family that does not
  // match the CQL3 expected format - i.e. column names encode information from
  // compound keys. It will however read CQL 2 tables (equivalent to CQL 3 with
  // "COMPACT STORAGE")

  /**
   * Refresh the meta data for a CQL 3 table
   * 
   * @param conn the connection to use
   * @throws Exception if a problem occurs
   */
  protected void refreshCQL3(CassandraConnection conn) throws Exception {
    List<String> colFamNames = m_keyspace.getColumnFamilyNames();

    if (!colFamNames.contains(m_columnFamilyName)) {
      throw new Exception(BaseMessages.getString(PKG,
          "CassandraColumnMetaData.Error.UnableToFindRequestedColumnFamily", //$NON-NLS-1$
          m_columnFamilyName, conn.m_keyspaceName));
    }

    ConsistencyLevel c = ConsistencyLevel.ONE; // default for CQL
    Compression z = Compression.NONE;

    // our meta data map
    m_columnMeta = new LinkedHashMap<String, String>();
    m_indexedVals = new HashMap<String, HashSet<Object>>();
    m_keyColumnNames = new ArrayList<String>();

    m_schemaDescription = new StringBuffer();
    String columnFamNameAdditionalInfo = ""; //$NON-NLS-1$

    // first get key alias (if any), column aliases (tells us if
    // there is a composite key and what the additional parts to the
    // key are, since these are not listed as normal columns), key validator,
    // default validator and comparator (full composite key type that
    // allows us to get the decoders for the additional part(s) of the key

    String cqlQ = "select comparator, default_validator, column_aliases, " //$NON-NLS-1$
        + "key_aliases, key_validator, bloom_filter_fp_chance, caching, " //$NON-NLS-1$
        + "compaction_strategy_class, compaction_strategy_options, compression_parameters, " //$NON-NLS-1$
        + "default_read_consistency, default_write_consistency, gc_grace_seconds, " //$NON-NLS-1$
        + "local_read_repair_chance, max_compaction_threshold, min_compaction_threshold, " //$NON-NLS-1$
        + "populate_io_cache_on_flush, read_repair_chance, replicate_on_write, type, value_alias " //$NON-NLS-1$
        + "from system.schema_columnfamilies " + "where keyspace_name='" //$NON-NLS-1$ //$NON-NLS-2$
        + conn.m_keyspaceName + "' and columnfamily_name='" //$NON-NLS-1$
        + m_columnFamilyName + "';"; //$NON-NLS-1$

    byte[] data = cqlQ.getBytes(Charset.forName("UTF-8")); //$NON-NLS-1$

    CqlResult result = conn.m_client.execute_cql3_query(ByteBuffer.wrap(data),
        z, c);

    List<CqlRow> rl = result.getRows();

    if (rl.size() != 1) {
      throw new Exception(BaseMessages.getString(PKG,
          "CassandraColumnMetaData.Error.CQLQueryToObtainMetaData", //$NON-NLS-1$
          m_columnFamilyName));
    }

    // read the system.schema_columns table to get the rest of the columns
    cqlQ = "select column_name, validator, index_name from system.schema_columns where keyspace_name='" //$NON-NLS-1$
        + conn.m_keyspaceName + "' AND columnfamily_name='" //$NON-NLS-1$
        + m_columnFamilyName + "';"; //$NON-NLS-1$

    data = cqlQ.getBytes(Charset.forName("UTF-8")); //$NON-NLS-1$

    CqlResult result2 = conn.m_client.execute_cql3_query(ByteBuffer.wrap(data),
        z, c);

    List<CqlRow> rl2 = result2.getRows();

    AbstractType deserializer = UTF8Type.instance;

    // process first result
    CqlRow row = rl.get(0);
    List<Column> cols = row.getColumns();

    // now column aliases (if needed)
    Column keyCols = cols.get(2);
    Object keyColsS = deserializer.compose(keyCols.bufferForValue());
    String p = keyColsS.toString().trim();
    String[] colAliasParts = p.split(","); //$NON-NLS-1$
    int numColAliases = p.equals("[]") ? 0 : colAliasParts.length; //$NON-NLS-1$
    boolean compactStorage = false;

    // for dynamic column families - information in the
    // schema description about how the additional key components
    // are encoded in the column names (from the CQL 2 perspective)
    String compactStorageColumnComparator = null;

    // first the comparator - gets us the comparator (column name decoder) and
    // potentially the types of any additional parts of a compound key (cols
    // that wont be listed in the system.columns table)

    List<String> decodersForAdditionalCompoundKeyCols = new ArrayList<String>();
    List<String> namesOfAdditionalCompoundKeyCols = new ArrayList<String>();
    Column compCol = cols.get(0);
    Object decodedComp = deserializer.compose(compCol.bufferForValue());

    p = decodedComp.toString();
    String pOrig = p;
    if (p.indexOf('(') > 0) {
      // comparator always starts with CompositeType (CQL 3),
      // even if the row key is only made up of one column since
      // the column names are always made up of at least one key value
      // with the actual textual part of the name being last in the
      // composite. We will get the names
      // of the columns that form the additional parts of the key later
      p = p.substring(p.indexOf('(') + 1, p.length() - 1).trim(); // strip
                                                                  // brackets
      String[] parts = p.split(","); //$NON-NLS-1$

      // now - is it standard CQL 3 table or COMPACT STORAGE? The only way
      // I can tell (so far) is that the number of column aliases
      // listed (i.e. non-partition parts of the key) will always be
      // equal to (number of elements in the comparator - 1) for
      // a non COMPACT STORAGE CQL 3 table (-1 because the last element
      // listed in the comparator is always UTF8 type for the column
      // name). If this is not the case, then the CompositeType defining
      // the comparator must completely relate to the types that
      // make up the composite column name. If there are no column
      // aliases defined in this case then the table must have been
      // created via thrift, in which case for CQL 3 output the
      // parts of the composite will need to be given column names:
      // e.g "column1", "column2" etc.
      if (parts.length - 1 != numColAliases) {
        compactStorage = true;
      }

      // Actual textual part of a CQL 3 column name is always the last
      // element in the composite
      if (!compactStorage) {
        m_columnComparator = parts[parts.length - 1].trim();
      } else {
        // we need to manually set this to UTF8Type because actual
        // column names are always text in CQL 3 output
        m_columnComparator = "org.apache.cassandra.db.marshal.UTF8Type"; //$NON-NLS-1$
        compactStorageColumnComparator = pOrig.trim();
      }
      for (int i = 0; i < (compactStorage ? parts.length : parts.length - 1); i++) {
        decodersForAdditionalCompoundKeyCols.add(parts[i].trim());
      }
    } else {
      // CQL2/COMPACT STORAGE table - just lists the column comparator
      m_columnComparator = p.trim();
      compactStorageColumnComparator = pOrig.trim();
      compactStorage = true;
    }

    if (compactStorage) {
      columnFamNameAdditionalInfo = " (COMPACT STORAGE)"; //$NON-NLS-1$
    }

    // now the default column value validator
    Column defaultV = cols.get(1);
    Object decodedValidator = deserializer.compose(defaultV.bufferForValue());
    m_defaultValidationClass = decodedValidator.toString();

    if (decodersForAdditionalCompoundKeyCols.size() > 0 && !compactStorage) {

      // add these additional key cols to our meta data map
      for (int i = 0; i < colAliasParts.length; i++) {
        String colName = colAliasParts[i].replace("[", "").replace("]", "") //$NON-NLS-1$ //$NON-NLS-2$ //$NON-NLS-3$ //$NON-NLS-4$
            .replace("\"", "").trim(); //$NON-NLS-1$ //$NON-NLS-2$
        namesOfAdditionalCompoundKeyCols.add(colName);
        m_columnMeta.put(colName, decodersForAdditionalCompoundKeyCols.get(i));
      }
    } else if (compactStorage) {
      if (rl2.size() == 0) { // no additional columns indicates dynamic schema
        columnFamNameAdditionalInfo = " (COMPACT STORAGE dynamic column family)"; //$NON-NLS-1$

        // COMPACT STORAGE tables created with exactly 1 non-key column (i.e.
        // dynamic column family with wide
        // rows) may have column alias(es) listed. These columns hold the
        // part(s)
        // of the dynamic column name (in old CQL 2 speak) and its decoder is
        // the
        // comparator. It may also have a value_alias listed - this is the
        // name of the column that holds the cell value (in old CQL 2 speak)
        // and its decoder is the default validator

        // if there are no decoders split out from a CompositeType then it
        // means that it is just a single value column name, in which
        // case m_columnComparator contains the decoder for this
        if (decodersForAdditionalCompoundKeyCols.size() == 0) {

          // create a column name
          if (numColAliases == 0) {
            namesOfAdditionalCompoundKeyCols.add("column1"); //$NON-NLS-1$
            m_columnMeta.put("column1", m_columnComparator); //$NON-NLS-1$
          } else {
            String colName = colAliasParts[0].replace("[", "").replace("]", "") //$NON-NLS-1$ //$NON-NLS-2$ //$NON-NLS-3$ //$NON-NLS-4$
                .replace("\"", "").trim(); //$NON-NLS-1$ //$NON-NLS-2$
            namesOfAdditionalCompoundKeyCols.add(colName);
            m_columnMeta.put(colName, m_columnComparator);
          }

          Column valueAlias = cols.get(20);
          if (valueAlias != null && valueAlias.bufferForValue() != null) {
            Object valueAliasS = deserializer.compose(valueAlias
                .bufferForValue());

            m_columnMeta.put(valueAliasS.toString().trim(),
                m_defaultValidationClass);
          } else {
            m_columnMeta.put("value", m_defaultValidationClass); //$NON-NLS-1$
          }
        } else {

          for (int i = 0; i < decodersForAdditionalCompoundKeyCols.size(); i++) {
            String colName = ""; //$NON-NLS-1$
            if (numColAliases != 0) {
              colName = colAliasParts[i].replace("[", "").replace("]", "") //$NON-NLS-1$ //$NON-NLS-2$ //$NON-NLS-3$ //$NON-NLS-4$
                  .replace("\"", "").trim(); //$NON-NLS-1$ //$NON-NLS-2$
            } else {
              // the default column names that CQL 3 creates
              colName = "column" + (i + 1); //$NON-NLS-1$
            }
            namesOfAdditionalCompoundKeyCols.add(colName);

            m_columnMeta.put(colName,
                decodersForAdditionalCompoundKeyCols.get(i));
          }

          Column valueAlias = cols.get(20);
          if (valueAlias != null && valueAlias.bufferForValue() != null) {
            Object valueAliasS = deserializer.compose(valueAlias
                .bufferForValue());

            m_columnMeta.put(valueAliasS.toString().trim(),
                m_defaultValidationClass);
          } else {
            m_columnMeta.put("value", m_defaultValidationClass); //$NON-NLS-1$
          }
        }
        // now we have to change the column comparator to UTF8 type since
        // CQL 3 maps the wide row to essentially tuple mode with n fixed
        // columns key, column-name, column-name, ..., column-value (these can
        // be named
        // anything).
        // The original column comparator is set so that this COMPACT STORAGE
        // table is readable using CQL 2
        m_columnComparator = "org.apache.cassandra.db.marshal.UTF8Type"; //$NON-NLS-1$
      }
    }

    // Compound key is not to be confused with a composite partition key.
    // The partition key is the primary part of the key (used to split)
    // data amongst nodes in the ring. The primary part of the key may
    // be a composite type
    boolean compoundKey = namesOfAdditionalCompoundKeyCols.size() > 0;

    // key validation - checks for composite partition key
    String keyName = "key"; //$NON-NLS-1$
    List<String> partitionKeyNames = new ArrayList<String>();
    Column keyV = cols.get(4);
    Object kV = deserializer.compose(keyV.bufferForValue());
    m_keyValidator = kV.toString().trim();
    String keyValParts = m_keyValidator;
    List<String> decodersForPartitionKeyParts = new ArrayList<String>();
    if (keyValParts.indexOf("(") > 0) { //$NON-NLS-1$
      // This means a composite PARTITION key. Have to split
      // the parts out
      keyValParts = keyValParts.substring(keyValParts.indexOf('(') + 1,
          keyValParts.length() - 1).trim(); // strip brackets
      String[] kvp = keyValParts.split(","); //$NON-NLS-1$
      for (String k : kvp) {
        decodersForPartitionKeyParts.add(k.trim());
      }
    }

    // temp map so that we can ensure that the keys are listed first when a
    // select * is done
    Map<String, String> tempColMeta = new LinkedHashMap<String, String>();

    // now key alias(es)
    Column keyAlias = cols.get(3);
    if (keyAlias != null && keyAlias.bufferForValue() != null) {
      Object alias = deserializer.compose(keyAlias.bufferForValue());
      String aliasS = alias.toString();

      if (aliasS.equals("[]")) { //$NON-NLS-1$
        // no alias - possibly a table created via thrift
        if (decodersForPartitionKeyParts.size() == 0) {
          keyName = "key"; //$NON-NLS-1$
          tempColMeta.put(keyName, m_keyValidator);
          m_keyColumnNames.add(keyName);
          partitionKeyNames.add(keyName);
        } else {
          // composite key with no aliases. First part of an anonymous
          // composite key is always called key; additional parts are named
          // key2, key3, etc.
          keyName = "key"; //$NON-NLS-1$
          tempColMeta.put(keyName, decodersForPartitionKeyParts.get(0));
          m_keyColumnNames.add(keyName);
          partitionKeyNames.add(keyName);

          for (int i = 1; i < decodersForPartitionKeyParts.size(); i++) {
            keyName = "key" + (i + 1); //$NON-NLS-1$
            tempColMeta.put(keyName, decodersForPartitionKeyParts.get(i));
            m_keyColumnNames.add(keyName);
            partitionKeyNames.add(keyName);
          }
        }
      } else {
        colAliasParts = aliasS.trim().split(","); //$NON-NLS-1$

        if (decodersForPartitionKeyParts.size() == 0) {
          keyName = colAliasParts[0].replace("[", "").replace("]", "") //$NON-NLS-1$ //$NON-NLS-2$ //$NON-NLS-3$ //$NON-NLS-4$
              .replace("\"", "").trim(); //$NON-NLS-1$ //$NON-NLS-2$
          tempColMeta.put(keyName, m_keyValidator);
          m_keyColumnNames.add(keyName);
          partitionKeyNames.add(keyName);
        } else {
          // number of decoders should equal the number of aliases defined
          for (int i = 0; i < decodersForPartitionKeyParts.size(); i++) {
            keyName = colAliasParts[i].replace("[", "").replace("]", "") //$NON-NLS-1$ //$NON-NLS-2$ //$NON-NLS-3$ //$NON-NLS-4$
                .replace("\"", "").trim(); //$NON-NLS-1$ //$NON-NLS-2$
            tempColMeta.put(keyName, decodersForPartitionKeyParts.get(i));
            m_keyColumnNames.add(keyName);
            partitionKeyNames.add(keyName);
          }
        }
      }
    }

    tempColMeta.putAll(m_columnMeta);
    m_columnMeta = tempColMeta;

    m_schemaDescription.append("Column family: " + m_columnFamilyName //$NON-NLS-1$
        + columnFamNameAdditionalInfo);

    String keyDescription = "\n\n\tKey" //$NON-NLS-1$
        + (compoundKey ? (partitionKeyNames.size() > 1 ? " ((composite) compound): " : " (compound): ") : " : "); //$NON-NLS-1$ //$NON-NLS-2$
    m_schemaDescription.append(keyDescription);
    if (partitionKeyNames.size() == 1) {
      m_schemaDescription.append(partitionKeyNames.get(0));
    } else {
      m_schemaDescription.append("("); //$NON-NLS-1$
      for (int i = 0; i < partitionKeyNames.size(); i++) {
        m_schemaDescription.append(partitionKeyNames.get(i));
        m_schemaDescription.append(i == partitionKeyNames.size() - 1 ? "" //$NON-NLS-1$
            : ", "); //$NON-NLS-1$
      }
      m_schemaDescription.append(")"); //$NON-NLS-1$
    }
    for (int i = 0; i < namesOfAdditionalCompoundKeyCols.size(); i++) {
      if (i < namesOfAdditionalCompoundKeyCols.size()) {
        m_schemaDescription.append(", "); //$NON-NLS-1$
      }

      m_schemaDescription.append(namesOfAdditionalCompoundKeyCols.get(i));
      m_keyColumnNames.add(namesOfAdditionalCompoundKeyCols.get(i));
    }

    m_schemaDescription
        .append("\n\tPartition key validator: " + m_keyValidator); //$NON-NLS-1$
    m_schemaDescription
        .append("\n\tColumn comparator: " //$NON-NLS-1$
            + (compactStorageColumnComparator != null ? compactStorageColumnComparator
                : m_columnComparator));

    m_schemaDescription.append("\n\tDefault column validator: " //$NON-NLS-1$
        + m_defaultValidationClass);

    // read repair chance etc.
    AbstractType axDeserializer = null;

    // bloom filter fp chance
    Column bf = cols.get(5);
    axDeserializer = DoubleType.instance;
    if (bf != null && bf.bufferForValue() != null) {
      Object bfv = axDeserializer.compose(bf.bufferForValue());
      m_schemaDescription.append("\n\tBloom filter fp chance: " //$NON-NLS-1$
          + bfv.toString());
    }

    // caching
    Column caching = cols.get(6);
    if (caching != null && caching.bufferForValue() != null) {
      Object cachV = deserializer.compose(caching.bufferForValue());
      m_schemaDescription.append("\n\tCaching: " + cachV.toString()); //$NON-NLS-1$
    }

    // compaction strategy class
    Column compaction = cols.get(7);
    if (compaction != null && compaction.bufferForValue() != null) {
      Object compV = deserializer.compose(compaction.bufferForValue());
      m_schemaDescription
          .append("\n\tCompaction strategy: " + compV.toString()); //$NON-NLS-1$
    }

    // compaction strategy options
    compaction = cols.get(8);
    if (compaction != null && compaction.bufferForValue() != null) {
      Object compV = deserializer.compose(compaction.bufferForValue());
      m_schemaDescription.append("\n\tCompaction strategy options: " //$NON-NLS-1$
          + compV.toString());
    }

    // compression
    Column compression = cols.get(9);
    if (compression != null && compression.bufferForValue() != null) {
      Object compV = deserializer.compose(compression.bufferForValue());
      m_schemaDescription.append("\n\tCompression parameters: " //$NON-NLS-1$
          + compV.toString());
    }

    // default read consistency
    Column defRead = cols.get(10);
    if (defRead != null && defRead.bufferForValue() != null) {
      Object readV = deserializer.compose(defRead.bufferForValue());
      m_schemaDescription.append("\n\tDefault read consistency: " //$NON-NLS-1$
          + readV.toString());
    }

    // default write consistency
    Column defWrite = cols.get(11);
    if (defWrite != null && defWrite.bufferForValue() != null) {
      Object writeV = deserializer.compose(defWrite.bufferForValue());
      m_schemaDescription.append("\n\tDefault write consistency: " //$NON-NLS-1$
          + writeV.toString());
    }

    // gc grace seconds
    Column gc = cols.get(12);
    axDeserializer = IntegerType.instance;
    if (gc != null && gc.bufferForValue() != null) {
      Object gcV = axDeserializer.compose(gc.bufferForValue());
      m_schemaDescription.append("\n\tGC grace seconds: " + gcV.toString()); //$NON-NLS-1$
    }

    // local read repair chance
    Column localRead = cols.get(13);
    axDeserializer = DoubleType.instance;
    if (localRead != null && localRead.bufferForValue() != null) {
      Object localV = axDeserializer.compose(localRead.bufferForValue());
      m_schemaDescription.append("\n\tLocal read repair chance: " //$NON-NLS-1$
          + localV.toString());
    }

    // max compaction threshold
    Column maxComp = cols.get(14);
    axDeserializer = IntegerType.instance;
    if (maxComp != null && maxComp.bufferForValue() != null) {
      Object compV = axDeserializer.compose(maxComp.bufferForValue());
      m_schemaDescription.append("\n\tMax compaction threshold: " //$NON-NLS-1$
          + compV.toString());
    }

    // min compaction threshold
    Column minComp = cols.get(15);
    if (minComp != null && minComp.bufferForValue() != null) {
      Object compV = axDeserializer.compose(minComp.bufferForValue());
      m_schemaDescription.append("\n\tMin compaction threshold: " //$NON-NLS-1$
          + compV.toString());
    }

    // populate IO cache on flush
    Column pop = cols.get(16);
    axDeserializer = BooleanType.instance;
    if (pop != null && pop.bufferForValue() != null) {
      Object popV = axDeserializer.compose(pop.bufferForValue());
      m_schemaDescription.append("\n\tPopulate IO cache on flush: " //$NON-NLS-1$
          + popV.toString());
    }

    // read repair chance
    Column readRep = cols.get(17);
    axDeserializer = DoubleType.instance;
    if (readRep != null && readRep.bufferForValue() != null) {
      Object readV = axDeserializer.compose(readRep.bufferForValue());
      m_schemaDescription.append("\n\tRead repair chance: " + readV.toString()); //$NON-NLS-1$
    }

    // replicate on write
    Column repWrite = cols.get(18);
    axDeserializer = BooleanType.instance;
    if (repWrite != null && repWrite.bufferForValue() != null) {
      Object repV = axDeserializer.compose(repWrite.bufferForValue());
      m_schemaDescription.append("\n\tReplicate on write: " + repV.toString()); //$NON-NLS-1$
    }

    // type?
    Column type = cols.get(19);
    if (type != null && type.bufferForValue() != null) {
      Object typeV = deserializer.compose(type.bufferForValue());
      m_schemaDescription.append("\n\tType: " + typeV.toString()); //$NON-NLS-1$
    }

    m_schemaDescription.append("\n\n\tColumn metadata:"); //$NON-NLS-1$

    // dump what we have in the map so far into the schema description
    for (Map.Entry<String, String> e : m_columnMeta.entrySet()) {
      m_schemaDescription.append("\n\tColumn name: " + e.getKey()); //$NON-NLS-1$
      m_schemaDescription.append("\n\t\tColumn validator: " + e.getValue()); //$NON-NLS-1$
    }

    // additional columns
    for (CqlRow r : rl2) {
      cols = r.getColumns();

      Column colName = cols.get(0);
      Column colV = cols.get(1);
      Column indexN = cols.get(2);

      Object decodedColName = deserializer.compose(colName.bufferForValue());
      Object decodedColValidator = deserializer.compose(colV.bufferForValue());

      m_columnMeta.put(
          decodedColName.toString().replace("\"", "").replace("'", "").trim(), //$NON-NLS-1$ //$NON-NLS-2$ //$NON-NLS-3$ //$NON-NLS-4$
          decodedColValidator.toString().trim());

      m_schemaDescription.append("\n\tColumn name: " //$NON-NLS-1$
          + decodedColName.toString().replace("\"", "").replace("'", "") //$NON-NLS-1$ //$NON-NLS-2$ //$NON-NLS-3$ //$NON-NLS-4$
              .trim());

      m_schemaDescription.append("\n\t\tColumn validator: " //$NON-NLS-1$
          + decodedColValidator.toString().trim());

      if (indexN != null && indexN.bufferForValue() != null) {
        Object decodedIndexN = deserializer.compose(indexN.bufferForValue());
        if (decodedIndexN != null
            && decodedIndexN.toString().trim().length() > 0) {
          m_schemaDescription.append("\n\t\tIndex name: " //$NON-NLS-1$
              + decodedIndexN.toString().trim());
        }
      }
    }

    // populate the value meta lookup map
    m_kettleColumnMeta = new HashMap<String, ValueMetaInterface>();
    for (Map.Entry<String, String> e : m_columnMeta.entrySet()) {
      String colName = e.getKey();

      m_kettleColumnMeta.put(colName, getValueMetaForColumn(colName));
    }
  }

  /**
   * Refreshes the encapsulated meta data for the column family.
   * 
   * @param conn the connection to cassandra to use for refreshing the meta data
   * @throws Exception if a problem occurs during connection or when fetching
   *           meta data
   */
  public void refresh(CassandraConnection conn) throws Exception {

    if (m_cql3) {
      refreshCQL3(conn);
      return;
    }

    m_schemaDescription = new StringBuffer();

    // column families
    KsDef keySpace = conn.describeKeyspace();
    List<CfDef> colFams = null;
    if (keySpace != null) {
      colFams = keySpace.getCf_defs();
    } else {
      throw new Exception(BaseMessages.getString(PKG,
          "CassandraColumnMetaData.Error.UnableToGetMetaDataForKeyspace", //$NON-NLS-1$
          conn.m_keyspaceName));
    }

    // set up our meta data map
    m_columnMeta = new LinkedHashMap<String, String>();
    m_indexedVals = new HashMap<String, HashSet<Object>>();
    m_keyColumnNames = new ArrayList<String>();

    // look for the requested column family
    CfDef colDefs = null;
    for (CfDef fam : colFams) {
      String columnFamilyName = fam.getName(); // table name
      if (columnFamilyName.equals(m_columnFamilyName)) {
        m_schemaDescription.append("Column family: " + m_columnFamilyName); //$NON-NLS-1$
        m_keyValidator = fam.getKey_validation_class(); // key type

        m_columnComparator = fam.getComparator_type(); // column names encoded
                                                       // as

        ByteBuffer b = fam.key_alias;

        String keyName = "KEY"; //$NON-NLS-1$
        if (b != null) {
          String keyNameDecoder = "org.apache.cassandra.db.marshal.UTF8Type"; //$NON-NLS-1$
          keyName = getColumnValue(b, keyNameDecoder).toString();
          m_schemaDescription.append("\n\tKey name: " + keyName); //$NON-NLS-1$
        }

        // add in the key to the column meta data
        m_columnMeta.put(keyName, m_keyValidator);
        m_keyColumnNames.add(keyName);

        m_defaultValidationClass = fam.getDefault_validation_class(); // default
                                                                      // column
                                                                      // type
        m_schemaDescription.append("\n\tKey validator: " + m_keyValidator); //$NON-NLS-1$
        if (m_keyValidator.contains("CompositeType")) { //$NON-NLS-1$
          m_schemaDescription
              .append("\n\t\tWARNING: column family with composite key cannot" //$NON-NLS-1$
                  + "\n\t\tbe accessed from CQL 2 - turn on \"Use Thrift IO\""); //$NON-NLS-1$
        }

        m_schemaDescription.append("\n\tColumn comparator: " //$NON-NLS-1$
            + m_columnComparator);

        m_schemaDescription.append("\n\tDefault column validator: " //$NON-NLS-1$
            + m_defaultValidationClass);

        // these seem to have disappeared between 0.8.6 and 1.0.0!
        /*
         * m_schemaDescription.append("\n\tMemtable operations: " +
         * fam.getMemtable_operations_in_millions());
         * m_schemaDescription.append("\n\tMemtable throughput: " +
         * fam.getMemtable_throughput_in_mb());
         * m_schemaDescription.append("\n\tMemtable flush after: " +
         * fam.getMemtable_flush_after_mins());
         */

        // these have disappeared between 1.0.8 and 1.1.0!!
        // m_schemaDescription.append("\n\tRows cached: " +
        // fam.getRow_cache_size());
        // m_schemaDescription.append("\n\tRow cache save period: " +
        // fam.getRow_cache_save_period_in_seconds());
        // m_schemaDescription.append("\n\tKeys cached: " +
        // fam.getKey_cache_size());
        // m_schemaDescription.append("\n\tKey cached save period: " +
        // fam.getKey_cache_save_period_in_seconds());
        m_schemaDescription.append("\n\tRead repair chance: " //$NON-NLS-1$
            + fam.getRead_repair_chance());
        m_schemaDescription
            .append("\n\tGC grace: " + fam.getGc_grace_seconds()); //$NON-NLS-1$
        m_schemaDescription.append("\n\tMin compaction threshold: " //$NON-NLS-1$
            + fam.getMin_compaction_threshold());
        m_schemaDescription.append("\n\tMax compaction threshold: " //$NON-NLS-1$
            + fam.getMax_compaction_threshold());
        m_schemaDescription.append("\n\tReplicate on write: " //$NON-NLS-1$
            + fam.replicate_on_write);
        // String rowCacheP = fam.getRow_cache_provider();

        m_schemaDescription.append("\n\n\tColumn metadata:"); //$NON-NLS-1$

        colDefs = fam;
        break;
      }
    }

    if (colDefs == null) {
      throw new Exception(BaseMessages.getString(PKG,
          "CassandraColumnMetaData.Error.UnableToFindRequestedColumnFamily", //$NON-NLS-1$
          m_columnFamilyName, conn.m_keyspaceName));
    }

    m_columnNameEncoding = m_columnComparator;

    String comment = colDefs.getComment();
    if (comment != null && comment.length() > 0) {
      extractIndexedMeta(comment, m_indexedVals);
    }

    Iterator<ColumnDef> colMetaData = colDefs.getColumn_metadataIterator();
    if (colMetaData != null) {
      while (colMetaData.hasNext()) {
        ColumnDef currentDef = colMetaData.next();
        ByteBuffer b = ByteBuffer.wrap(currentDef.getName());

        String colName = getColumnValue(b, m_columnComparator).toString();

        String colType = currentDef.getValidation_class();
        m_columnMeta.put(colName, colType);

        m_schemaDescription.append("\n\tColumn name: " + colName); //$NON-NLS-1$

        m_schemaDescription.append("\n\t\tColumn validator: " + colType); //$NON-NLS-1$

        String indexName = currentDef.getIndex_name();
        if (!Const.isEmpty(indexName)) {
          m_schemaDescription.append("\n\t\tIndex name: " //$NON-NLS-1$
              + currentDef.getIndex_name());
        }

        if (m_indexedVals.containsKey(colName)) {
          HashSet<Object> indexedVals = m_indexedVals.get(colName);

          m_schemaDescription.append("\n\t\tLegal values: {"); //$NON-NLS-1$
          int count = 0;
          for (Object val : indexedVals) {
            m_schemaDescription.append(val.toString());
            count++;
            if (count != indexedVals.size()) {
              m_schemaDescription.append(","); //$NON-NLS-1$
            } else {
              m_schemaDescription.append("}"); //$NON-NLS-1$
            }
          }
        }
      }
    }

    // populate the value meta lookup map
    m_kettleColumnMeta = new HashMap<String, ValueMetaInterface>();
    for (Map.Entry<String, String> e : m_columnMeta.entrySet()) {
      String colName = e.getKey();

      m_kettleColumnMeta.put(colName, getValueMetaForColumn(colName));
    }
  }

  protected void extractIndexedMeta(String comment,
      Map<String, HashSet<Object>> indexedVals) {
    if (comment.indexOf("@@@") < 0) { //$NON-NLS-1$
      return;
    }

    String meta = comment.substring(comment.indexOf("@@@"), //$NON-NLS-1$
        comment.lastIndexOf("@@@")); //$NON-NLS-1$
    meta = meta.replace("@@@", ""); //$NON-NLS-1$ //$NON-NLS-2$
    String[] fields = meta.split(";"); //$NON-NLS-1$

    for (String field : fields) {
      field = field.trim();
      String[] parts = field.split(":"); //$NON-NLS-1$

      if (parts.length != 2) {
        continue;
      }

      String fieldName = parts[0].trim();
      String valsS = parts[1];
      valsS = valsS.replace("{", ""); //$NON-NLS-1$ //$NON-NLS-2$
      valsS = valsS.replace("}", ""); //$NON-NLS-1$ //$NON-NLS-2$

      String[] vals = valsS.split(","); //$NON-NLS-1$

      if (vals.length > 0) {
        HashSet<Object> valsSet = new HashSet<Object>();

        for (String aVal : vals) {
          valsSet.add(aVal.trim());
        }

        indexedVals.put(fieldName, valsSet);
      }
    }
  }

  /**
   * Get a textual description of the named column family
   * 
   * @param keyspace
   * @return
   * @throws Exception
   */
  public String describe() throws Exception {

    refresh((CassandraConnection) m_keyspace.getConnection());

    return getSchemaDescription();
  }

  /**
   * Return the schema overview information
   * 
   * @return the textual description of the schema
   */
  public String getSchemaDescription() {
    return m_schemaDescription.toString();
  }

  /**
   * Return the Cassandra column type (internal cassandra class name relative to
   * org.apache.cassandra.db.marshal) for the given Kettle column.
   * 
   * @param vm the ValueMetaInterface for the Kettle column
   * @return the corresponding internal cassandra type.
   */
  public static String getCassandraTypeForValueMeta(ValueMetaInterface vm) {
    switch (vm.getType()) {
    case ValueMetaInterface.TYPE_STRING:
      return "UTF8Type"; //$NON-NLS-1$
    case ValueMetaInterface.TYPE_BIGNUMBER:
      return "DecimalType"; //$NON-NLS-1$
    case ValueMetaInterface.TYPE_BOOLEAN:
      return "BooleanType"; //$NON-NLS-1$
    case ValueMetaInterface.TYPE_INTEGER:
      return "LongType"; //$NON-NLS-1$
    case ValueMetaInterface.TYPE_NUMBER:
      return "DoubleType"; //$NON-NLS-1$
    case ValueMetaInterface.TYPE_DATE:
      return "DateType"; //$NON-NLS-1$
    case ValueMetaInterface.TYPE_BINARY:
    case ValueMetaInterface.TYPE_SERIALIZABLE:
      return "BytesType"; //$NON-NLS-1$
    }

    return "UTF8Type"; //$NON-NLS-1$
  }

  /**
   * Return the Cassandra CQL column/key type for the given Kettle column. We
   * use this type for CQL create column family statements since, for some
   * reason, the internal type isn't recognized for the key. Internal types
   * *are* recognized for column definitions. The CQL reference guide states
   * that fully qualified (or relative to org.apache.cassandra.db.marshal) class
   * names can be used instead of CQL types - however, using these when defining
   * the key type always results in BytesType getting set for the key for some
   * reason.
   * 
   * @param vm the ValueMetaInterface for the Kettle column
   * @return the corresponding CQL type
   */
  public static String getCQLTypeForValueMeta(ValueMetaInterface vm) {
    switch (vm.getType()) {
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
      return "timestamp"; //$NON-NLS-1$
    case ValueMetaInterface.TYPE_BINARY:
    case ValueMetaInterface.TYPE_SERIALIZABLE:
      return "blob"; //$NON-NLS-1$
    }

    return "blob"; //$NON-NLS-1$
  }

  /**
   * Static utility to decompose a Kettle value to a ByteBuffer. Note - does not
   * check if the kettle value is null.
   * 
   * @param vm the ValueMeta for the Kettle value
   * @param value the actual Kettle value
   * @return a ByteBuffer encapsulating the bytes for the decomposed value
   * @throws KettleException if a problem occurs
   */
  public ByteBuffer kettleValueToByteBuffer(ValueMetaInterface vm,
      Object value, boolean isKey) throws KettleException {

    String fullTransCoder = m_defaultValidationClass;

    // check the key first
    if (isKey) {
      fullTransCoder = m_keyValidator;
    } else {
      fullTransCoder = m_columnMeta.get(vm.getName());
      if (fullTransCoder == null) {
        // use default if not in column meta data
        fullTransCoder = m_defaultValidationClass;
      }
    }

    String transCoder = fullTransCoder;

    // if it's a composite type make sure that we check only against the
    // primary type
    if (transCoder.indexOf('(') > 0) {
      transCoder = transCoder.substring(0, transCoder.indexOf('('));
    }

    ByteBuffer decomposed = null;
    if (transCoder.indexOf("UTF8Type") > 0) { //$NON-NLS-1$
      UTF8Type u = UTF8Type.instance;
      decomposed = u.decompose(vm.getString(value));
    } else if (transCoder.indexOf("AsciiType") > 0) { //$NON-NLS-1$
      AsciiType at = AsciiType.instance;
      decomposed = at.decompose(vm.getString(value));
    } else if (transCoder.indexOf("LongType") > 0) { //$NON-NLS-1$
      LongType lt = LongType.instance;
      decomposed = lt.decompose(vm.getInteger(value));
    } else if (transCoder.indexOf("DoubleType") > 0) { //$NON-NLS-1$
      DoubleType dt = DoubleType.instance;
      decomposed = dt.decompose(vm.getNumber(value));
    } else if (transCoder.indexOf("DateType") > 0) { //$NON-NLS-1$
      DateType dt = DateType.instance;
      decomposed = dt.decompose(vm.getDate(value));
    } else if (transCoder.indexOf("IntegerType") > 0) { //$NON-NLS-1$
      IntegerType it = IntegerType.instance;
      decomposed = it.decompose(vm.getBigNumber(value).toBigInteger());
    } else if (transCoder.indexOf("FloatType") > 0) { //$NON-NLS-1$
      FloatType ft = FloatType.instance;
      decomposed = ft.decompose(vm.getNumber(value).floatValue());
    } else if (transCoder.indexOf("LexicalUUIDType") > 0) { //$NON-NLS-1$
      LexicalUUIDType lt = LexicalUUIDType.instance;
      UUID uuid = UUID.fromString((vm.getString(value)));
      decomposed = lt.decompose(uuid);
    } else if (transCoder.indexOf("UUIDType") > 0) { //$NON-NLS-1$
      UUIDType ut = UUIDType.instance;
      UUID uuid = UUID.fromString((vm.getString(value)));
      decomposed = ut.decompose(uuid);
    } else if (transCoder.indexOf("BooleanType") > 0) { //$NON-NLS-1$
      BooleanType bt = BooleanType.instance;
      decomposed = bt.decompose(vm.getBoolean(value));
    } else if (transCoder.indexOf("Int32Type") > 0) { //$NON-NLS-1$
      Int32Type it = Int32Type.instance;
      decomposed = it.decompose(vm.getInteger(value).intValue());
    } else if (transCoder.indexOf("DecimalType") > 0) { //$NON-NLS-1$
      DecimalType dt = DecimalType.instance;
      decomposed = dt.decompose(vm.getBigNumber(value));
    } else if (transCoder.indexOf("DynamicCompositeType") > 0) { //$NON-NLS-1$
      AbstractType serializer = null;
      if (vm.isString()) {
        try {
          serializer = TypeParser.parse(fullTransCoder);
          decomposed = ((DynamicCompositeType) serializer).fromString(vm
              .getString(value));

        } catch (Exception e) {
          throw new KettleException(e.getMessage(), e);
        }
      } else {
        throw new KettleException(BaseMessages.getString(PKG,
            "CassandraColumnMetaData.Error.CantConvertTypeThrift", //$NON-NLS-1$
            vm.getTypeDesc(), fullTransCoder));
      }
    } else if (transCoder.indexOf("CompositeType") > 0) { //$NON-NLS-1$
      AbstractType serializer = null;
      if (vm.isString()) {
        try {
          serializer = TypeParser.parse(fullTransCoder);
          decomposed = ((CompositeType) serializer).fromString(vm.toString());
        } catch (Exception e) {
          throw new KettleException(e.getMessage(), e);
        }
      } else {
        throw new KettleException(BaseMessages.getString(PKG,
            "CassandraColumnMetaData.Error.CantConvertTypeThrift", //$NON-NLS-1$
            vm.getTypeDesc(), fullTransCoder));
      }
    }

    if (decomposed == null) {
      throw new KettleException(BaseMessages.getString(PKG,
          "CassandraColumnMetaData.Error.UnableToConvertValue", vm.getName())); //$NON-NLS-1$
    }

    return decomposed;
  }

  /**
   * Encode a string representation of a column name using the serializer for
   * the default comparator.
   * 
   * @param colName the textual column name to serialze
   * @return a ByteBuffer encapsulating the serialized column name
   * @throws KettleException if a problem occurs during serialization
   */
  public ByteBuffer columnNameToByteBuffer(String colName)
      throws KettleException {

    AbstractType serializer = null;
    String fullEncoder = m_columnComparator;
    String encoder = fullEncoder;

    // if it's a composite type make sure that we check only against the
    // primary type
    if (encoder.indexOf('(') > 0) {
      encoder = encoder.substring(0, encoder.indexOf('('));
    }

    if (encoder.indexOf("UTF8Type") > 0) { //$NON-NLS-1$
      serializer = UTF8Type.instance;
    } else if (encoder.indexOf("AsciiType") > 0) { //$NON-NLS-1$
      serializer = AsciiType.instance;
    } else if (encoder.indexOf("LongType") > 0) { //$NON-NLS-1$
      serializer = LongType.instance;
    } else if (encoder.indexOf("DoubleType") > 0) { //$NON-NLS-1$
      serializer = DoubleType.instance;
    } else if (encoder.indexOf("DateType") > 0) { //$NON-NLS-1$
      serializer = DateType.instance;
    } else if (encoder.indexOf("IntegerType") > 0) { //$NON-NLS-1$
      serializer = IntegerType.instance;
    } else if (encoder.indexOf("FloatType") > 0) { //$NON-NLS-1$
      serializer = FloatType.instance;
    } else if (encoder.indexOf("LexicalUUIDType") > 0) { //$NON-NLS-1$
      serializer = LexicalUUIDType.instance;
    } else if (encoder.indexOf("UUIDType") > 0) { //$NON-NLS-1$
      serializer = UUIDType.instance;
    } else if (encoder.indexOf("BooleanType") > 0) { //$NON-NLS-1$
      serializer = BooleanType.instance;
    } else if (encoder.indexOf("Int32Type") > 0) { //$NON-NLS-1$
      serializer = Int32Type.instance;
    } else if (encoder.indexOf("DecimalType") > 0) { //$NON-NLS-1$
      serializer = DecimalType.instance;
    } else if (encoder.indexOf("DynamicCompositeType") > 0) { //$NON-NLS-1$
      try {
        serializer = TypeParser.parse(fullEncoder);
      } catch (Exception e) {
        throw new KettleException(e.getMessage(), e);
      }
    } else if (encoder.indexOf("CompositeType") > 0) { //$NON-NLS-1$
      try {
        serializer = TypeParser.parse(fullEncoder);
      } catch (Exception e) {
        throw new KettleException(e.getMessage(), e);
      }
    }

    ByteBuffer result = serializer.fromString(colName);

    return result;
  }

  /**
   * Get the Kettle ValueMeta the corresponds to the type of the key for this
   * column family. Name of the kettle column is always set to "KEY" as it is
   * possible that the key may be a composite of several columns (this can be
   * true under access via CQL 2 as well as 3 if a table has been created via
   * the cassandra-cli or Thrift and explicitly set to have a composite key)
   * 
   * @return the key's ValueMeta
   */
  public ValueMetaInterface getValueMetaForKey() {
    int kettleType = cassandraTypeToKettleType(m_keyValidator);

    // if the key is a compound key (i.e. a CQL 3 table) then the Kettle type
    // for the key is String because we will need to concatenate the individual
    // values for tuple mode. Otherwise, the key may be a single column (in
    // which
    // case we can use its actual type as the Kettle type or it might be a
    // CompositeType (non CQL style table) - in which case cassandraTypeToKettle
    // will set the appropriate Kettle type
    if (getKeyColumnNames().size() > 0) {
      return new ValueMeta("KEY", ValueMetaInterface.TYPE_STRING); //$NON-NLS-1$
    }
    return new ValueMeta("KEY", kettleType); //$NON-NLS-1$
  }

  public ValueMetaInterface getValueMetaForDefaultValidator() {
    // non-existent column forces the type for the default validator to be
    // returned
    return getValueMetaForColumn(""); //$NON-NLS-1$
  }

  protected int cassandraTypeToKettleType(String type) {
    int kettleType = 0;
    if (type.indexOf("UTF8Type") > 0 || type.indexOf("AsciiType") > 0 //$NON-NLS-1$ //$NON-NLS-2$
        || type.indexOf("UUIDType") > 0 || type.indexOf("CompositeType") > 0) { //$NON-NLS-1$ //$NON-NLS-2$
      kettleType = ValueMetaInterface.TYPE_STRING;
    } else if (type.indexOf("LongType") > 0 || type.indexOf("IntegerType") > 0 //$NON-NLS-1$ //$NON-NLS-2$
        || type.indexOf("Int32Type") > 0) { //$NON-NLS-1$
      kettleType = ValueMetaInterface.TYPE_INTEGER;
    } else if (type.indexOf("DoubleType") > 0 || type.indexOf("FloatType") > 0) { //$NON-NLS-1$ //$NON-NLS-2$
      kettleType = ValueMetaInterface.TYPE_NUMBER;
    } else if (type.indexOf("DateType") > 0) { //$NON-NLS-1$
      kettleType = ValueMetaInterface.TYPE_DATE;
    } else if (type.indexOf("DecimalType") > 0) { //$NON-NLS-1$
      kettleType = ValueMetaInterface.TYPE_BIGNUMBER;
    } else if (type.indexOf("BytesType") > 0) { //$NON-NLS-1$
      kettleType = ValueMetaInterface.TYPE_BINARY;
    } else if (type.indexOf("BooleanType") > 0) { //$NON-NLS-1$
      kettleType = ValueMetaInterface.TYPE_BOOLEAN;
    }

    return kettleType;
  }

  /**
   * Get the Kettle ValueMeta that corresponds to the type of the supplied
   * cassandra column.
   * 
   * @param colName the name of the column to get a ValueMeta for
   * @return the ValueMeta that is appropriate for the type of the supplied
   *         column.
   */
  public ValueMetaInterface getValueMetaForColumn(String colName) {
    String type = null;

    type = m_columnMeta.get(colName);
    if (type == null) {
      type = m_defaultValidationClass;
    } else {
      // entry from lookup
      if (m_kettleColumnMeta.containsKey(colName)) {
        return m_kettleColumnMeta.get(colName);
      }
    }
    // }

    int kettleType = cassandraTypeToKettleType(type);

    ValueMetaInterface newVM = new ValueMeta(colName, kettleType);
    if (m_indexedVals.containsKey(colName)) {
      // make it indexed!
      newVM.setStorageType(ValueMetaInterface.STORAGE_TYPE_INDEXED);
      HashSet<Object> indexedV = m_indexedVals.get(colName);
      Object[] iv = indexedV.toArray();
      newVM.setIndex(iv);
    }

    return newVM;
  }

  /**
   * Get a list of ValueMetas corresponding to the columns in this schema
   * 
   * @return a list of ValueMetas
   */
  public List<ValueMetaInterface> getValueMetasForSchema() {
    List<ValueMetaInterface> newL = new ArrayList<ValueMetaInterface>();

    for (String colName : m_columnMeta.keySet()) {
      ValueMetaInterface colVM = getValueMetaForColumn(colName);
      newL.add(colVM);
    }

    return newL;
  }

  /**
   * Get a Set of column names that are defined in the meta data for this schema
   * 
   * @return a set of column names.
   */
  public Set<String> getColumnNames() {
    // only returns those column names that are defined in the schema!
    return m_columnMeta.keySet();
  }

  /**
   * Returns true if the supplied column name exists in this schema.
   * 
   * @param colName the name of the column to check.
   * @return true if the column exists in the meta data for this column family.
   */
  public boolean columnExistsInSchema(String colName) {

    return (m_columnMeta.get(colName) != null);
  }

  /**
   * Get the names of the columns that make up the key in this column family. If
   * there is a single key column and it does not have an explicit alias set
   * then this will use the string "KEY".
   * 
   * @return the name(s) of the columns that make up the key
   */
  public List<String> getKeyColumnNames() {
    return m_keyColumnNames;
  }

  /**
   * Return the name of this column family.
   * 
   * @return the name of this column family.
   */
  public String getColumnFamilyName() {
    return m_columnFamilyName;
  }

  /**
   * Return the decoded key value of a row. Assumes that the supplied row comes
   * from the column family that this meta data represents!! Note that getting
   * the key value from a CqlRow object only works for CQL 2; In CQL3
   * CqlRow.getKey() returns an array of length zero!
   * 
   * @param row a Cassandra row
   * @return the decoded key value
   * @throws KettleException if a deserializer can't be determined
   */
  public Object getKeyValue(CqlRow row) throws KettleException {

    ByteBuffer key = row.bufferForKey();

    if (!m_keyValidator.contains("CompositeType") //$NON-NLS-1$
        && m_keyValidator.indexOf("BytesType") > 0) { //$NON-NLS-1$
      return row.getKey();
    }

    return getColumnValue(key, m_keyValidator);
  }

  /**
   * Return the decoded key value of a row. Assumes that the supplied row comes
   * from the column family that this meta data represents!!
   * 
   * @param row a Cassandra row
   * @return the decoded key value
   * @throws KettleException if a deserializer can't be determined
   */
  public Object getKeyValue(KeySlice row) throws KettleException {
    ByteBuffer key = row.bufferForKey();

    if (!m_keyValidator.contains("CompositeType") //$NON-NLS-1$
        && m_keyValidator.indexOf("BytesType") > 0) { //$NON-NLS-1$
      return row.getKey();
    }

    return getColumnValue(key, m_keyValidator);
  }

  public String getColumnName(Column aCol) throws KettleException {
    ByteBuffer b = aCol.bufferForName();

    String decodedColName = getColumnValue(b, m_columnComparator).toString();
    return decodedColName;
  }

  private Object getColumnValue(ByteBuffer valueBuff, String decoder)
      throws KettleException {
    if (valueBuff == null) {
      return null;
    }

    Object result = null;
    AbstractType deserializer = null;
    String fullDecoder = decoder;

    // if it's a composite type make sure that we check only against the
    // primary type
    if (decoder.indexOf('(') > 0) {
      decoder = decoder.substring(0, decoder.indexOf('('));
    }

    if (decoder.indexOf("UTF8Type") > 0) { //$NON-NLS-1$
      deserializer = UTF8Type.instance;
    } else if (decoder.indexOf("AsciiType") > 0) { //$NON-NLS-1$
      deserializer = AsciiType.instance;
    } else if (decoder.indexOf("LongType") > 0) { //$NON-NLS-1$
      deserializer = LongType.instance;
    } else if (decoder.indexOf("DoubleType") > 0) { //$NON-NLS-1$
      deserializer = DoubleType.instance;
    } else if (decoder.indexOf("DateType") > 0) { //$NON-NLS-1$
      deserializer = DateType.instance;
    } else if (decoder.indexOf("IntegerType") > 0) { //$NON-NLS-1$
      deserializer = IntegerType.instance;

      result = new Long(((IntegerType) deserializer).compose(valueBuff)
          .longValue());
      return result;
    } else if (decoder.indexOf("FloatType") > 0) { //$NON-NLS-1$
      deserializer = FloatType.instance;

      result = new Double(((FloatType) deserializer).compose(valueBuff))
          .doubleValue();
      return result;
    } else if (decoder.indexOf("LexicalUUIDType") > 0) { //$NON-NLS-1$
      deserializer = LexicalUUIDType.instance;

      result = new String(((LexicalUUIDType) deserializer).compose(valueBuff)
          .toString());
      return result;
    } else if (decoder.indexOf("UUIDType") > 0) { //$NON-NLS-1$
      deserializer = UUIDType.instance;

      result = new String(((UUIDType) deserializer).compose(valueBuff)
          .toString());
      return result;
    } else if (decoder.indexOf("BooleanType") > 0) { //$NON-NLS-1$
      deserializer = BooleanType.instance;
    } else if (decoder.indexOf("Int32Type") > 0) { //$NON-NLS-1$
      deserializer = Int32Type.instance;

      result = new Long(((Int32Type) deserializer).compose(valueBuff))
          .longValue();
      return result;
    } else if (decoder.indexOf("DecimalType") > 0) { //$NON-NLS-1$
      deserializer = DecimalType.instance;
    } else if (decoder.indexOf("DynamicCompositeType") > 0) { //$NON-NLS-1$
      try {
        deserializer = TypeParser.parse(fullDecoder);

        // now return the string representation of the composite value
        result = ((DynamicCompositeType) deserializer).getString(valueBuff);
        return result;
      } catch (Exception e) {
        throw new KettleException(e.getMessage(), e);
      }
    } else if (decoder.indexOf("CompositeType") > 0) { //$NON-NLS-1$
      try {
        deserializer = TypeParser.parse(fullDecoder);

        // now return the string representation of the composite value
        result = ((CompositeType) deserializer).getString(valueBuff);

        return result;
      } catch (Exception e) {
        throw new KettleException(e.getMessage(), e);
      }
    }

    if (deserializer == null) {
      throw new KettleException(BaseMessages.getString(PKG,
          "CassandraColumnMetaData.Error.CantFindADeserializerForType", //$NON-NLS-1$
          fullDecoder));
    }

    result = deserializer.compose(valueBuff);

    return result;
  }

  /**
   * Decode the supplied column value. Uses the default validation class to
   * decode the value if the column is not explicitly defined in the schema.
   * 
   * @param aCol
   * @return
   * @throws KettleException
   */
  public Object getColumnValue(Column aCol) throws KettleException {
    String colName = getColumnName(aCol);
    String decoder = null;

    decoder = m_columnMeta.get(colName);

    if (decoder == null) {
      // column is not in schema so use default validator
      decoder = m_defaultValidationClass;
    }

    String fullDecoder = decoder;
    if (decoder.indexOf('(') > 0) {
      decoder = decoder.substring(0, decoder.indexOf('('));
    }

    if (decoder.indexOf("BytesType") > 0) { //$NON-NLS-1$
      return aCol.getValue(); // raw bytes
    }

    ByteBuffer valueBuff = aCol.bufferForValue();
    Object result = getColumnValue(valueBuff, fullDecoder);

    // check for indexed values
    if (m_indexedVals.containsKey(colName)) {
      HashSet<Object> vals = m_indexedVals.get(colName);

      // look for the correct index
      int foundIndex = -1;
      Object[] indexedV = vals.toArray();
      for (int i = 0; i < indexedV.length; i++) {
        if (indexedV[i].equals(result)) {
          foundIndex = i;
          break;
        }
      }

      if (foundIndex >= 0) {
        result = new Integer(foundIndex);
      } else {
        result = null; // any values that are not indexed are unknown...
      }
    }

    return result;
  }
}
