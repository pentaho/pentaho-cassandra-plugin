/*******************************************************************************
 *
 * Copyright (C) 2018 by Hitachi Vantara : http://www.pentaho.com
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

package org.pentaho.cassandra.driver.datastax;

import java.util.ArrayList;
import java.util.List;
import java.util.stream.Collectors;

import org.pentaho.cassandra.util.Selector;
import org.pentaho.cassandra.spi.ITableMetaData;
import org.pentaho.cassandra.spi.Keyspace;
import org.pentaho.di.core.row.ValueMetaInterface;
import org.pentaho.di.core.row.value.ValueMetaBigNumber;
import org.pentaho.di.core.row.value.ValueMetaBinary;
import org.pentaho.di.core.row.value.ValueMetaBoolean;
import org.pentaho.di.core.row.value.ValueMetaDate;
import org.pentaho.di.core.row.value.ValueMetaInteger;
import org.pentaho.di.core.row.value.ValueMetaNumber;
import org.pentaho.di.core.row.value.ValueMetaString;

import com.datastax.driver.core.ColumnMetadata;
import com.datastax.driver.core.DataType;
import com.datastax.driver.core.TableMetadata;

public class TableMetaData implements ITableMetaData {

  private DriverKeyspace keyspace;
  private TableMetadata meta;

  private String name;

  // expand collection values into multiple rows (behaviour of other implementation)
  private boolean expandCollection = true;

  public TableMetaData( DriverKeyspace keyspace, TableMetadata metadata ) {
    meta = metadata;
    name = meta.getName();
    setKeyspace( keyspace );
  }

  @Override
  public void setKeyspace( Keyspace keyspace ) {
    this.keyspace = (DriverKeyspace) keyspace;
    expandCollection = this.keyspace.getConnection().isExpandCollection();
  }

  @Override
  public void setTableName( String tableName ) {
    this.name = tableName;
  }
  @Override
  public String getTableName() {
    return name;
  }
  @Override
  public String describe() throws Exception {
    return meta.exportAsString();
  }

  @Override
  public boolean columnExistsInSchema( String colName ) {
    return meta.getColumn( colName ) != null;
  }

  @Override
  public ValueMetaInterface getValueMetaForKey() {
    List<ColumnMetadata> partKeys = meta.getPartitionKey();
    if ( partKeys.size() > 1 ) {
      return new ValueMetaString( "KEY" );
    }
    return toValueMeta( partKeys.get( 0 ).getName(), partKeys.get( 0 ).getType() );
  }

  @Override
  public List<String> getKeyColumnNames() {
    return meta.getPrimaryKey().stream().map( col -> col.getName() ).collect( Collectors.toList() );
  }

  @Override
  public ValueMetaInterface getValueMetaForColumn( String colName ) {
    ColumnMetadata column = meta.getColumn( colName );
    return getValueMetaForColumn( column );
  }

  protected ValueMetaInterface getValueMetaForColumn( ColumnMetadata column ) {
    if ( column != null ) {
      return toValueMeta( column.getName(), column.getType() );
    }
    return new ValueMetaString( name );
  }

  @Override
  public List<ValueMetaInterface> getValueMetasForSchema() {
    return meta.getColumns().stream().map( col -> getValueMetaForColumn( col ) ).collect( Collectors.toList() );
  }

  @Override
  public ValueMetaInterface getValueMeta( Selector selector ) {
    String name = selector.getColumnName();
    return getValueMetaForColumn( name );
  }

  @Override
  public List<String> getColumnNames() {
    List<ColumnMetadata> colMeta = meta.getColumns();
    List<String> colNames = new ArrayList<>();
    for ( ColumnMetadata c : colMeta ) {
      colNames.add( c.getName() );
    }

    return colNames;
  }

  @Override
  public DataType getColumnCQLType( String colName ) {
    return meta.getColumn( colName ).getType();
  }

  protected ValueMetaInterface toValueMeta( String name, DataType dataType ) {
    if ( expandCollection && dataType.isCollection() && dataType.getName().equals( DataType.Name.MAP ) ) {
      dataType = dataType.getTypeArguments().get( 0 );
    }
    // http://docs.datastax.com/en/cql/3.1/cql/cql_reference/cql_data_types_c.html
    switch ( dataType.getName() ) {
      case BIGINT:
      case COUNTER:
      case INT:
      case SMALLINT:
      case TINYINT:
        return new ValueMetaInteger( name );
      case DOUBLE:
      case FLOAT:
        return new ValueMetaNumber( name );
      case DATE:
      case TIMESTAMP:
        return new ValueMetaDate( name );
      case DECIMAL:
      case VARINT:
        return new ValueMetaBigNumber( name );
      case BLOB:
        return new ValueMetaBinary( name );
      case BOOLEAN:
        return new ValueMetaBoolean( name );
      default:
        return new ValueMetaString( name );
    }
  }
}
