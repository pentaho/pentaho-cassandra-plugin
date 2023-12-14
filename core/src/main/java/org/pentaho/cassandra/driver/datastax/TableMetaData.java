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
import java.util.Collection;
import java.util.function.Function;
import java.util.HashMap;
import java.util.List;
import java.util.Map;
import java.util.Optional;
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

import com.datastax.oss.driver.api.core.metadata.schema.ColumnMetadata;
import com.datastax.oss.driver.api.core.metadata.schema.TableMetadata;
import com.datastax.oss.driver.api.core.type.DataType;
import com.datastax.oss.driver.api.core.type.DataTypes;
import com.datastax.oss.driver.api.core.type.MapType;


public class TableMetaData implements ITableMetaData {

  private static final Map<DataType, Function<String, ValueMetaInterface>> DATA_TYPE_VALUE_SUPPLIERS = new HashMap<>();
  static {
    DATA_TYPE_VALUE_SUPPLIERS.put( DataTypes.BIGINT, name -> new ValueMetaInteger( name ) );
    DATA_TYPE_VALUE_SUPPLIERS.put( DataTypes.COUNTER, name -> new ValueMetaInteger( name ) );
    DATA_TYPE_VALUE_SUPPLIERS.put( DataTypes.INT, name -> new ValueMetaInteger( name ) );
    DATA_TYPE_VALUE_SUPPLIERS.put( DataTypes.SMALLINT, name -> new ValueMetaInteger( name ) );
    DATA_TYPE_VALUE_SUPPLIERS.put( DataTypes.TINYINT, name -> new ValueMetaInteger( name ) );
    DATA_TYPE_VALUE_SUPPLIERS.put( DataTypes.DOUBLE, name -> new ValueMetaNumber( name ) );
    DATA_TYPE_VALUE_SUPPLIERS.put( DataTypes.FLOAT, name -> new ValueMetaNumber( name ) );
    DATA_TYPE_VALUE_SUPPLIERS.put( DataTypes.DATE, name -> new ValueMetaDate( name ) );
    DATA_TYPE_VALUE_SUPPLIERS.put( DataTypes.TIMESTAMP, name -> new ValueMetaDate( name ) );
    DATA_TYPE_VALUE_SUPPLIERS.put( DataTypes.DECIMAL, name -> new ValueMetaBigNumber( name ) );
    DATA_TYPE_VALUE_SUPPLIERS.put( DataTypes.VARINT, name -> new ValueMetaBigNumber( name ) );
    DATA_TYPE_VALUE_SUPPLIERS.put( DataTypes.BLOB, name -> new ValueMetaBinary( name ) );
    DATA_TYPE_VALUE_SUPPLIERS.put( DataTypes.BOOLEAN, name -> new ValueMetaBoolean( name ) );

  }

  private static class ProtocolTypes {
  }

  private DriverKeyspace keyspace;
  private TableMetadata meta;

  private String name;

  // expand collection values into multiple rows (behaviour of other implementation)
  private boolean expandCollection = true;

  public TableMetaData( DriverKeyspace keyspace, TableMetadata metadata ) {
    meta = metadata;
    name = meta.getName().toString();
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
    return meta.describe( true );
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
    return toValueMeta( partKeys.get( 0 ).getName().toString(), partKeys.get( 0 ).getType() );
  }

  @Override
  public List<String> getKeyColumnNames() {
    return meta.getPrimaryKey().stream().map( col -> col.getName().toString() ).collect( Collectors.toList() );
  }

  @Override
  public ValueMetaInterface getValueMetaForColumn( String colName ) {
    Optional<ColumnMetadata> column = meta.getColumn( colName );
    return column.map( c -> getValueMetaForColumn( c ) ).orElse( null );
  }

  protected ValueMetaInterface getValueMetaForColumn( ColumnMetadata column ) {
    if ( column != null ) {
      return toValueMeta( column.getName().toString(), column.getType() );
    }
    return new ValueMetaString( name );
  }

  @Override
  public List<ValueMetaInterface> getValueMetasForSchema() {
    return meta.getColumns().keySet().stream().map( col -> getValueMetaForColumn( col.toString() ) )
                                              .collect( Collectors.toList() );
  }

  @Override
  public ValueMetaInterface getValueMeta( Selector selector ) {
    String name = selector.getColumnName();
    return getValueMetaForColumn( name );
  }

  @Override
  public List<String> getColumnNames() {
    Collection<ColumnMetadata> colMeta = meta.getColumns().values();
    List<String> colNames = new ArrayList<>();
    for ( ColumnMetadata c : colMeta ) {
      colNames.add( c.getName().toString() );
    }

    return colNames;
  }

  @Override
  public DataType getColumnCQLType( String colName ) {
    return meta.getColumn( colName ).map( ColumnMetadata::getType ).orElse( null );
  }

  protected ValueMetaInterface toValueMeta( String name, DataType dataType ) {
    if ( expandCollection &&  dataType instanceof MapType ) {
      MapType mType = (MapType) dataType;
      dataType = mType.getKeyType();
    }
    // http://docs.datastax.com/en/cql/3.1/cql/cql_reference/cql_data_types_c.html
    Function<String, ValueMetaInterface> valueMetaFunc = DATA_TYPE_VALUE_SUPPLIERS.get( dataType );
    if ( valueMetaFunc != null ) {
      return valueMetaFunc.apply( name );
    } else {
        return new ValueMetaString( name );
    }
  }
}
