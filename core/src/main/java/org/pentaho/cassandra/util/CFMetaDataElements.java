/*! ******************************************************************************
 *
 * Pentaho
 *
 * Copyright (C) 2002 - 2026 by Pentaho Canada Inc. : http://www.pentaho.com
 *
 * Use of this software is governed by the Business Source License included
 * in the LICENSE.TXT file.
 *
 * Change Date: 2030-06-15
 ******************************************************************************/

package org.pentaho.cassandra.util;

/**
 * Created by mracine on 2/6/2018.
 */
public enum CFMetaDataElements {
  KEYSPACE_NAME( "keyspace_name" ), TABLE_NAME( "table_name" ),
  BLOOM_FILTER_FP_CHANCE( "bloom_filter_fp_chance" ),
  CACHING( "caching" ), CDC( "cdc" ),
  CLUSTERING_ORDER( "clustering_order" ),
  COMMENT( "comment" ),
  COMPACTION( "compaction" ), COMPRESSION( "compression" ),
  COLUMN_NAME( "column_name" ),
  COLUMN_NAME_BYTES( "column_name_bytes" ),
  CRC_CHECK_CHANCE( "crc_check_chance" ),
  DC_LOCAL_READ_REPAIR_CHANCE( "dc_local_read_repair_chance" ),
  DEFAULT_TIME_TO_LIVE( "default_time_to_live" ),
  EXTENSIONS( "extensions" ), FLAGS( "flags" ),
  GC_GRACE_SECONDS( "gc_grace_seconds" ), ID( "id" ),
  KIND( "kind" ),
  MAX_INDEX_INTERVAL( "max_index_interval" ),
  MEMTABLE_FLUSH_PERIOD_IN_MS( "memtable_flush_period_in_ms" ),
  MIN_INDEX_INTERVAL( "min_index_interval" ),
  POSITION( "position" ),
  READ_REPAIR_CHANCE( "read_repair_chance" ),
  SPECULATIVE_RETRY( "speculative_retry" ),
  TYPE( "type" );

  private final String m_name;

  CFMetaDataElements( String name ) {
    m_name = name;
  }

  @Override
  public String toString() {
    return m_name;
  }
}
