# encoding: utf-8

module CassandraMigrations
  module Migrator

    METADATA_TABLE = 'cassandra_migrations_metadata'
    METADATA_TABLE_V2 = 'cassandra_migrations_metadata_v2'

    def self.up_to_latest!(keyspace = nil)
      new_migrations =  get_missing_migrations(keyspace)
      new_migrations.each { |migration| up(get_all_migration_hash[migration]) }

      new_migrations.size
    end

    def self.rollback!(count=1, keyspace = nil)
      executed_migrations = get_migrated_migrations.reverse

      down_count = 0

      executed_migrations[0..count-1].each do |migration|
        down(get_all_migration_hash[migration])
        down_count += 1
      end

      down_count
    end

    def self.get_missing_migrations(keyspace = nil)
      all_migrations_versions = get_all_migration_names(keyspace).map{|s| get_version_from_migration_name(s).to_s}
      migrated_migrations = get_migrated_migrations

      missed_migrations = all_migrations_versions - migrated_migrations
      missed_migrations.sort!
    end 

    def self.read_current_version_v1
      begin
        Cassandra.select(METADATA_TABLE, :selection => "data_name='version'", :projection => 'data_value').first['data_value'].to_i
      rescue ::Cassandra::Errors::InvalidError => e # table cassandra_migrations_metadata does not exist
        Cassandra.execute("CREATE TABLE #{METADATA_TABLE} (data_name varchar PRIMARY KEY, data_value varchar)")
        Cassandra.write!(METADATA_TABLE, {:data_name => 'version', :data_value => '0'})
        return 0
      end
    end

    def self.read_current_version_v2
      get_migrated_migrations.last
    end

    #this method used only for moving to new SET way of storing migrations
    def self.migrate_to_v2(keyspace = nil)
      old_version = Cassandra.select(METADATA_TABLE, :selection => "data_name='version'", :projection => 'data_value').first['data_value'].to_i
      p "Old version: #{old_version}"

      passed_migrations = get_all_migration_names(keyspace).sort.select do |migration_name|
        get_version_from_migration_name(migration_name) <= old_version
      end

      passed_migrations.each do |passed|
        version = get_version_from_migration_name(passed)
        Cassandra.update!(METADATA_TABLE_V2, "data_name = 'version'", {data_value: [version.to_s]}, {operations: {data_value: :+}})
      end
    end

    def self.get_migrated_migrations
      begin
        Cassandra.select(METADATA_TABLE_V2, :selection => "data_name='version'", :projection => 'data_value').first["data_value"].to_a
      rescue ::Cassandra::Errors::InvalidError => e # table cassandra_migrations_metadata does not exist
        Cassandra.execute("CREATE TABLE #{METADATA_TABLE_V2} (data_name varchar PRIMARY KEY, data_value set<varchar>)")
        Cassandra.write!(METADATA_TABLE_V2, {:data_name => 'version', :data_value => []})
        return []
      end
    end

private

    def self.up(migration_name)
      # load migration
      require migration_name
      # run migration
      get_class_from_migration_name(migration_name).new.migrate(:up)

      # update version
      version = get_version_from_migration_name(migration_name).to_s
      Cassandra.update!(METADATA_TABLE_V2, "data_name = 'version'", {data_value: [version.to_s]}, {operations: {data_value: :+}})
    end

    def self.down(migration_name)
      # load migration
      require migration_name
      # run migration
      get_class_from_migration_name(migration_name).new.migrate(:down)

      # downgrade version
      version = get_version_from_migration_name(migration_name).to_s
      Cassandra.update!(METADATA_TABLE_V2, "data_name = 'version'", {data_value: [version.to_s]}, {operations: {data_value: :-}})
    end

    def self.get_all_migration_names(keyspace = nil)
      if keyspace
        Dir[Rails.root.join("db", "cassandra_migrate/#{keyspace}/[0-9]*_*.rb")]
      else
        Dir[Rails.root.join("db", "cassandra_migrate/[0-9]*_*.rb")]
      end
    end

    def self.get_all_migration_hash(keyspace = nil)
      h = {}
      names = get_all_migration_names(keyspace)
      names.each{|n| h[get_version_from_migration_name(n).to_s] = n}
      h
    end

    def self.get_class_from_migration_name(filename)
      filename.match(/[0-9]{14}_(.+)\.rb$/).captures.first.camelize.constantize
    end

    def self.get_version_from_migration_name(migration_name)
      migration_name.match(/([0-9]{14})_.+\.rb$/).captures.first.to_i
    end
  end
end
