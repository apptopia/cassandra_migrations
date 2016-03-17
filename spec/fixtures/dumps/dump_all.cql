DROP KEYSPACE IF EXISTS cassandra_migrations_test;CREATE KEYSPACE cassandra_migrations_test WITH REPLICATION = {'class' : 'org.apache.cassandra.locator.SimpleStrategy', 'replication_factor' : '1'} AND DURABLE_WRITES = true;CREATE TABLE cassandra_migrations_test.collection_lists (
  id uuid,
  map_1 map<varchar, float>,
  PRIMARY KEY (id)
)
WITH bloom_filter_fp_chance = 0.01
 AND caching = '{"keys":"ALL", "rows_per_partition":"NONE"}'
 AND comment = ''
 AND compaction = {'class' : 'org.apache.cassandra.db.compaction.SizeTieredCompactionStrategy'}
 AND compression = {'sstable_compression' : 'org.apache.cassandra.io.compress.LZ4Compressor'}
 AND dclocal_read_repair_chance = 0.1
 AND default_time_to_live = 0
 AND gc_grace_seconds = 864000
 AND index_interval = 128
 AND populate_io_cache_on_flush = 'false'
 AND read_repair_chance = 0.0
 AND speculative_retry = '99.0PERCENTILE';CREATE TABLE cassandra_migrations_test.kitchen_sink (
  id uuid,
  a_float float,
  a_list_of_strings list<varchar>,
  a_string varchar,
  a_timestamp timestamp,
  PRIMARY KEY (id)
)
WITH bloom_filter_fp_chance = 0.01
 AND caching = '{"keys":"ALL", "rows_per_partition":"NONE"}'
 AND comment = ''
 AND compaction = {'class' : 'org.apache.cassandra.db.compaction.SizeTieredCompactionStrategy'}
 AND compression = {'sstable_compression' : 'org.apache.cassandra.io.compress.LZ4Compressor'}
 AND dclocal_read_repair_chance = 0.1
 AND default_time_to_live = 0
 AND gc_grace_seconds = 864000
 AND index_interval = 128
 AND populate_io_cache_on_flush = 'false'
 AND read_repair_chance = 0.0
 AND speculative_retry = '99.0PERCENTILE';DROP KEYSPACE IF EXISTS secondary_first_test;CREATE KEYSPACE secondary_first_test WITH REPLICATION = {'class' : 'org.apache.cassandra.locator.SimpleStrategy', 'replication_factor' : '1'} AND DURABLE_WRITES = true;CREATE TABLE secondary_first_test.collection_lists (
  id uuid,
  map_1 map<varchar, float>,
  PRIMARY KEY (id)
)
WITH bloom_filter_fp_chance = 0.01
 AND caching = '{"keys":"ALL", "rows_per_partition":"NONE"}'
 AND comment = ''
 AND compaction = {'class' : 'org.apache.cassandra.db.compaction.SizeTieredCompactionStrategy'}
 AND compression = {'sstable_compression' : 'org.apache.cassandra.io.compress.LZ4Compressor'}
 AND dclocal_read_repair_chance = 0.1
 AND default_time_to_live = 0
 AND gc_grace_seconds = 864000
 AND index_interval = 128
 AND populate_io_cache_on_flush = 'false'
 AND read_repair_chance = 0.0
 AND speculative_retry = '99.0PERCENTILE';CREATE TABLE secondary_first_test.kitchen_sink (
  id uuid,
  a_float float,
  a_list_of_strings list<varchar>,
  a_string varchar,
  a_timestamp timestamp,
  PRIMARY KEY (id)
)
WITH bloom_filter_fp_chance = 0.01
 AND caching = '{"keys":"ALL", "rows_per_partition":"NONE"}'
 AND comment = ''
 AND compaction = {'class' : 'org.apache.cassandra.db.compaction.SizeTieredCompactionStrategy'}
 AND compression = {'sstable_compression' : 'org.apache.cassandra.io.compress.LZ4Compressor'}
 AND dclocal_read_repair_chance = 0.1
 AND default_time_to_live = 0
 AND gc_grace_seconds = 864000
 AND index_interval = 128
 AND populate_io_cache_on_flush = 'false'
 AND read_repair_chance = 0.0
 AND speculative_retry = '99.0PERCENTILE';DROP KEYSPACE IF EXISTS secondary_second_test;CREATE KEYSPACE secondary_second_test WITH REPLICATION = {'class' : 'org.apache.cassandra.locator.SimpleStrategy', 'replication_factor' : '1'} AND DURABLE_WRITES = true;CREATE TABLE secondary_second_test.collection_lists (
  id uuid,
  map_1 map<varchar, float>,
  PRIMARY KEY (id)
)
WITH bloom_filter_fp_chance = 0.01
 AND caching = '{"keys":"ALL", "rows_per_partition":"NONE"}'
 AND comment = ''
 AND compaction = {'class' : 'org.apache.cassandra.db.compaction.SizeTieredCompactionStrategy'}
 AND compression = {'sstable_compression' : 'org.apache.cassandra.io.compress.LZ4Compressor'}
 AND dclocal_read_repair_chance = 0.1
 AND default_time_to_live = 0
 AND gc_grace_seconds = 864000
 AND index_interval = 128
 AND populate_io_cache_on_flush = 'false'
 AND read_repair_chance = 0.0
 AND speculative_retry = '99.0PERCENTILE';CREATE TABLE secondary_second_test.kitchen_sink (
  id uuid,
  a_float float,
  a_list_of_strings list<varchar>,
  a_string varchar,
  a_timestamp timestamp,
  PRIMARY KEY (id)
)
WITH bloom_filter_fp_chance = 0.01
 AND caching = '{"keys":"ALL", "rows_per_partition":"NONE"}'
 AND comment = ''
 AND compaction = {'class' : 'org.apache.cassandra.db.compaction.SizeTieredCompactionStrategy'}
 AND compression = {'sstable_compression' : 'org.apache.cassandra.io.compress.LZ4Compressor'}
 AND dclocal_read_repair_chance = 0.1
 AND default_time_to_live = 0
 AND gc_grace_seconds = 864000
 AND index_interval = 128
 AND populate_io_cache_on_flush = 'false'
 AND read_repair_chance = 0.0
 AND speculative_retry = '99.0PERCENTILE';