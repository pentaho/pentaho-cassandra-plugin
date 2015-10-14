/*! ******************************************************************************
 *
 * Pentaho Data Integration
 *
 * Copyright (C) 2002-2015 by Pentaho : http://www.pentaho.com
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
package org.pentaho.cassandra.cql;

/**
 * @author Tatsiana_Kasiankova
 * 
 */
/**
 * A representation of a selector in a selection list of a select clause. 
 *
 */
public class Selector {

  private String columnName;

  private String alias;

  private CQLFunctions function;

  private boolean isFunction;

  /**
   * @param columnName
   */
  public Selector( String columnName ) {
    this( columnName, null );
  }

  /**
   * @param columnName
   * @param alias
   */
  public Selector( String columnName, String alias ) {
    this( columnName, alias, null );
  }

  /**
   * @param columnName
   * @param alias
   * @param function
   */
  public Selector( String columnName, String alias, String function ) {
    super();
    this.columnName = columnName;
    this.alias = alias;
    this.function = CQLFunctions.getFromString( function );
    this.isFunction = this.function != null;
  }

  /**
   * @return the alias
   */
  public String getAlias() {
    return alias;
  }

  /**
   * @return the columnName
   */
  public String getColumnName() {
    return columnName;
  }

  /**
   * @return the function
   */
  public CQLFunctions getFunction() {
    return function;
  }

  /**
   * @return the isFunction
   */
  public boolean isFunction() {
    return isFunction;
  }

  /*
   * (non-Javadoc)
   * 
   * @see java.lang.Object#toString()
   */
  @Override
  public String toString() {
    return "Selector [columnName=" + columnName + ", alias=" + alias + ", function=" + function + ", isFunction="
        + isFunction + "]";
  }

}
