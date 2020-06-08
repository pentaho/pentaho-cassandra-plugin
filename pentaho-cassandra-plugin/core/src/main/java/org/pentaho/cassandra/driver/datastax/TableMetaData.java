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

package org.pentaho.cassandra.driver.datastax;

import com.datastax.oss.driver.api.core.CqlIdentifier;
import com.datastax.oss.driver.api.core.metadata.schema.ColumnMetadata;
import com.datastax.oss.driver.api.core.metadata.schema.TableMetadata;
import com.datastax.oss.driver.api.core.type.DataType;
import com.datastax.oss.driver.api.core.type.DataTypes;
import com.datastax.oss.driver.api.core.type.ListType;
import com.datastax.oss.driver.api.core.type.MapType;
import com.datastax.oss.driver.api.core.type.SetType;
import org.pentaho.cassandra.spi.ITableMetaData;
import org.pentaho.cassandra.spi.Keyspace;
import org.pentaho.cassandra.util.Selector;
import org.pentaho.di.core.row.ValueMetaInterface;
import org.pentaho.di.core.row.value.ValueMetaBigNumber;
import org.pentaho.di.core.row.value.ValueMetaBinary;
import org.pentaho.di.core.row.value.ValueMetaBoolean;
import org.pentaho.di.core.row.value.ValueMetaDate;
import org.pentaho.di.core.row.value.ValueMetaInteger;
import org.pentaho.di.core.row.value.ValueMetaNumber;
import org.pentaho.di.core.row.value.ValueMetaString;

import java.util.List;
import java.util.stream.Collectors;

public class TableMetaData implements ITableMetaData {

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
    return meta.describeWithChildren( true );
  }

  @Override
  public boolean columnExistsInSchema( String colName ) {
    return meta.getColumn( colName ).isPresent();
  }

  @Override
  public ValueMetaInterface getValueMetaForKey( boolean notExpandingMaps ) {
    List<ColumnMetadata> partKeys = meta.getPartitionKey();
    if ( partKeys.size() > 1 ) {
      return new ValueMetaString( "KEY" );
    }

    return toValueMeta( partKeys.get( 0 ).getName(), partKeys.get( 0 ).getType(), expandCollection, notExpandingMaps );
  }

  @Override
  public List<String> getKeyColumnNames() {
    return meta.getPrimaryKey().stream().map( col -> col.getName().toString() ).collect( Collectors.toList() );
  }

  @Override
  public ValueMetaInterface getValueMetaForColumn( String colName, boolean notExpandingMaps ) {
    ColumnMetadata column = meta.getColumn( colName ).get();
    return getValueMetaForColumn( column, notExpandingMaps );
  }

  protected ValueMetaInterface getValueMetaForColumn( ColumnMetadata column, boolean notExpandingMaps ) {
    if ( column != null ) {
      return toValueMeta( column.getName(), column.getType(), expandCollection, notExpandingMaps );
    }
    return new ValueMetaString( name );
  }

  @Override
  public List<ValueMetaInterface> getValueMetasForSchema( boolean notExpandingMaps ) {
    return meta.getColumns().values().stream().map( col -> getValueMetaForColumn( col, notExpandingMaps ) )
      .collect( Collectors.toList() );
  }

  @Override
  public ValueMetaInterface getValueMeta( Selector selector, boolean notExpandingMaps ) {
    String name = selector.getColumnName();
    return getValueMetaForColumn( name, notExpandingMaps );
  }

  @Override
  public List<String> getColumnNames() {
    return meta.getColumns().keySet().stream().map( c -> c.toString() ).collect( Collectors.toList() );
  }

  @Override
  public DataType getColumnCQLType( String colName ) {
    return meta.getColumn( colName ).get().getType();
  }

  protected static ValueMetaInterface toValueMeta( CqlIdentifier name, DataType dataType, boolean expandCollection, boolean notExpandingMaps ) {

    String nameStr = name.toString();

    if ( expandCollection ) {
      if ( !notExpandingMaps && dataType instanceof MapType ) {
        dataType = ( (MapType) dataType ).getKeyType();
      } else if ( dataType instanceof ListType ) {
        dataType = ( (ListType) dataType ).getElementType();
      } else if ( dataType instanceof SetType ) {
        dataType = ( (SetType) dataType ).getElementType();
      }
    }

    // http://docs.datastax.com/en/cql/3.1/cql/cql_reference/cql_data_types_c.html
    if ( dataType == DataTypes.BIGINT
      || dataType == DataTypes.COUNTER
      || dataType == DataTypes.INT
      || dataType == DataTypes.SMALLINT
      || dataType == DataTypes.TINYINT ) {
      return new ValueMetaInteger( nameStr );
    } else if ( dataType == DataTypes.DOUBLE
      || dataType == DataTypes.FLOAT ) {
      return new ValueMetaNumber( nameStr );
    } else if ( dataType == DataTypes.DATE
      || dataType == DataTypes.TIMESTAMP ) {
      return new ValueMetaDate( nameStr );
    } else if ( dataType == DataTypes.BOOLEAN ) {
      return new ValueMetaBoolean( nameStr );
    } else if ( dataType == DataTypes.BLOB ) {
      return new ValueMetaBinary( nameStr );
    } else if ( dataType == DataTypes.DECIMAL
      || dataType == DataTypes.VARINT ) {
      return new ValueMetaBigNumber( nameStr );
    }
    return new ValueMetaString( nameStr );
  }
}
