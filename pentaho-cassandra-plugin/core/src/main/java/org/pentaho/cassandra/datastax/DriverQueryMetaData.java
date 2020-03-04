/*******************************************************************************
 *
 * Copyright (C) 2019-2020 by Hitachi Vantara : http://www.pentaho.com
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

package org.pentaho.cassandra.datastax;

import com.datastax.oss.driver.api.core.cql.ColumnDefinition;
import com.datastax.oss.driver.api.core.cql.ColumnDefinitions;
import com.datastax.oss.driver.api.core.cql.PreparedStatement;
import com.datastax.oss.driver.api.core.type.DataType;
import org.pentaho.cassandra.spi.IQueryMetaData;
import org.pentaho.cassandra.spi.Keyspace;
import org.pentaho.di.core.row.ValueMetaInterface;

import java.util.List;
import java.util.stream.Collectors;
import java.util.stream.StreamSupport;

public class DriverQueryMetaData implements IQueryMetaData {

  protected DriverKeyspace keySpace = null;
  protected String tableName = null;
  protected ColumnDefinitions columnDefinitions;
  protected boolean expandCollection = true;

  @Override
  public void setKeyspace( Keyspace ks ) {
    keySpace = (DriverKeyspace) ks;
    expandCollection = keySpace.getConnection().isExpandCollection();
  }

  @Override
  public void parseQuery( String query ) throws Exception {
    try {
      PreparedStatement ps = keySpace.getSession().prepare( query );
      columnDefinitions = ps.getResultSetDefinitions();
      tableName = columnDefinitions.get( 0 ).getTable().toString();
    } catch ( Exception e ) {
      columnDefinitions = null;
      tableName = null;
      throw e;
    }
  }

  @Override
  public String getTableName() {
    return tableName;
  }

  @Override
  public ValueMetaInterface getValueMetaForColumn( String colName ) {
    if ( columnDefinitions != null ) {
      ColumnDefinition cd = columnDefinitions.get( colName );
      if ( cd != null ) {
        return TableMetaData.toValueMeta( cd.getName(), cd.getType(), expandCollection );
      }
    }
    return null;
  }

  @Override
  public List<ValueMetaInterface> getValueMetasForQuery() {
    return getColumnNames().stream().map( cn -> getValueMetaForColumn( cn ) ).collect( Collectors.toList() );
  }

  @Override
  public DataType getColumnCQLType( String colName ) {
    return columnDefinitions.get( colName ).getType();
  }

  @Override
  public List<String> getColumnNames() {
    return StreamSupport.stream( columnDefinitions.spliterator(), false ).map( cd -> cd.getName().toString() )
        .collect( Collectors.toList() );
  }

}
