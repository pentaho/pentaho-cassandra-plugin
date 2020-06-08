/*******************************************************************************
 *
 * Copyright (C) 2018-2020 by Hitachi Vantara : http://www.pentaho.com
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

package org.pentaho.cassandra.spi;

import com.datastax.oss.driver.api.core.type.DataType;
import org.pentaho.di.core.row.ValueMetaInterface;

import java.util.List;

public interface IQueryMetaData {

  void setKeyspace( Keyspace ks );

  void parseQuery( String query ) throws Exception;

  boolean isExpandCollection();

  void setExpandCollection( boolean expandCollection );

  boolean isNotExpandingMaps();

  void setNotExpandingMaps( boolean notExpandingMaps );

  String getTableName();

  /**
   * Return the appropriate Kettle type for the named column. If the column is not explicitly named in the table schema
   * then the Kettle type equivalent to the default validation class should be returned. Note that columns that make up
   * a composite key should be covered by this method too - i.e. the appropriate Kettle type for each should be returned
   * 
   * @param colName
   *          the Cassandra column name to get the Kettle type for
   * @return the Kettle type for the named column.
   */
  ValueMetaInterface getValueMetaForColumn( String colName );

  /**
   * Return a list of Kettle types for all the columns explicitly defined in this table (not including the default
   * validator).
   * 
   * @return a list of Kettle types for explicitly defined columns in this table
   */
  List<ValueMetaInterface> getValueMetasForQuery();

  /**
   * Returns the CQL type for a given column in this table
   * 
   * @param colName
   *          the name of the column
   * @return the CQL type
   */
  DataType getColumnCQLType( String colName );

  /**
   * Returns the names of the columns in a CQL table
   * 
   * @return a list of the CQL table column names
   */
  List<String> getColumnNames();

}
