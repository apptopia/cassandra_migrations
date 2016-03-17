# encoding: utf-8

module CassandraMigrations
  module Migrator
    extend self

    METADATA_TABLE = 'cassandra_migrations_metadata'
    METADATA_TABLE_V2 = 'cassandra_migrations_metadata_v2'

    def up_to_latest!(force_keyspace = nil)
      keyspaces = force_keyspace ? [force_keyspace] : get_keyspaces_list

      keyspaces.each do |keyspace|
        new_migrations =  get_missing_migrations(keyspace)
        unless new_migrations.empty?
          puts "Running missing migrations for #{keyspace}".green
          new_migrations.each { |migration| up(get_all_migration_hash(keyspace)[migration], keyspace) }
          #dump new version of CQL dump
          dump_keyspace(keyspace)
        end
      end
    end

    def dump_keyspace(keyspace)
      CassandraMigrations::SchemaDump.new(keyspace).dump
    end

    def rollback!(count, keyspace)
      executed_migrations = get_migrated_migrations(keyspace).reverse

      executed_migrations[0..count-1].each do |migration|
        down(get_all_migration_hash(keyspace)[migration], keyspace)
      end
    end

    def get_missing_migrations(keyspace)
      all_migrations_versions = get_all_migration_names(keyspace).map{|s| get_version_from_migration_name(s).to_s}
      migrated_migrations = get_migrated_migrations(keyspace)

      missed_migrations = all_migrations_versions - migrated_migrations
      missed_migrations.sort!
    end 

    def read_current_version_v1
      begin
        Cassandra.select(METADATA_TABLE, :selection => "data_name='version'", :projection => 'data_value').first['data_value'].to_i
      rescue ::Cassandra::Errors::InvalidError => e # table cassandra_migrations_metadata does not exist
        Cassandra.execute("CREATE TABLE #{METADATA_TABLE} (data_name varchar PRIMARY KEY, data_value varchar)")
        Cassandra.write!(METADATA_TABLE, {:data_name => 'version', :data_value => '0'})
        return 0
      end
    end

    def read_current_version_v2(keyspace)
      get_migrated_migrations(keyspace).last
    end

    #this method used only for moving to new SET way of storing migrations
    def migrate_to_v2
      get_keyspaces_list.each do |keyspace|
        with_env_keyspace(keyspace) do |enved_keyspace|
          begin
            old_version = Cassandra.select(METADATA_TABLE, :selection => "data_name='version'", :projection => 'data_value').first['data_value'].to_i
          rescue
            unless get_migrated_migrations(keyspace).empty?
              p "Nothing to do for #{enved_keyspace}"
            end
            next
          end

          passed_migrations = get_all_migration_names(keyspace).sort.select do |migration_name|
            get_version_from_migration_name(migration_name) <= old_version
          end

          Cassandra.execute("DROP TABLE IF EXISTS #{METADATA_TABLE_V2}")
          get_migrated_migrations(keyspace) #force to create METADATA_TABLE_V2

          passed_migrations.each do |passed|
            version = get_version_from_migration_name(passed)
            Cassandra.update!(METADATA_TABLE_V2, "data_name = 'version'", {data_value: [version.to_s]}, {operations: {data_value: :+}})
          end
        end
      end
    end

    def get_migrated_migrations(keyspace)
      with_env_keyspace(keyspace) do |enved_keyspace|
        begin
          Cassandra.select(METADATA_TABLE_V2, :selection => "data_name='version'", :projection => 'data_value').first["data_value"].to_a
        rescue ::Cassandra::Errors::InvalidError => e # table cassandra_migrations_metadata does not exist
          Cassandra.execute("CREATE TABLE #{METADATA_TABLE_V2} (data_name varchar PRIMARY KEY, data_value set<varchar>)")
          puts "Created #{enved_keyspace}.#{METADATA_TABLE_V2}".yellow
          Cassandra.write!(METADATA_TABLE_V2, {:data_name => 'version', :data_value => []})
          return []
        end
      end
    end

    def get_keyspaces_list
      path = Rails.root.join("db", "cassandra_migrate")
      Dir.entries(path.to_s).select{|s| !%w(. ..).include?(s)}
    end

private

    def up(migration_name, keyspace)
      # load migration
      require migration_name
      # run migration
      with_env_keyspace(keyspace) do |enved_keyspace|
        get_class_from_migration_name(migration_name).new.migrate(:up)

        # update version
        version = get_version_from_migration_name(migration_name).to_s
        p "migrated #{version} in keyspace '#{enved_keyspace}'"

        Cassandra.update!(METADATA_TABLE_V2, "data_name = 'version'", {data_value: [version.to_s]}, {operations: {data_value: :+}})
      end
    end

    def down(migration_name, keyspace)
      # load migration
      require migration_name
      # run migration
      with_env_keyspace(keyspace) do |enved_keyspace|
        get_class_from_migration_name(migration_name).new.migrate(:down)

        # downgrade version
        version = get_version_from_migration_name(migration_name).to_s
        p "rolled back #{version} in keyspace '#{enved_keyspace}'"

        Cassandra.update!(METADATA_TABLE_V2, "data_name = 'version'", {data_value: [version.to_s]}, {operations: {data_value: :-}})
      end
    end

    def get_all_migration_names(keyspace = nil)
      #p Rails.root
      if keyspace
        Dir[Rails.root.join("db", "cassandra_migrate/#{keyspace}/[0-9]*_*.rb")]
      else
        Dir[Rails.root.join("db", "cassandra_migrate/[0-9]*_*.rb")]
      end
    end

    def get_all_migration_hash(keyspace)
      h = {}
      names = get_all_migration_names(keyspace)
      names.each{|n| h[get_version_from_migration_name(n).to_s] = n}
      h
    end

    def get_class_from_migration_name(filename)
      filename.match(/[0-9]{14}_(.+)\.rb$/).captures.first.camelize.constantize
    end

    def get_version_from_migration_name(migration_name)
      migration_name.match(/([0-9]{14})_.+\.rb$/).captures.first.to_i
    end

    def with_env_keyspace(keyspace, &block)
      keyspace = detect_keyspace_from_env(keyspace)
      res = nil
      CassandraMigrations::Cassandra.using_keyspace(keyspace) do
        res = block.call(keyspace)
      end
      res
    end

    def detect_keyspace_from_env(keyspace)
      detected_for_env = cassandra_keyspaces.detect{|k| k == "#{keyspace}_#{Rails.env}"}

      #for production we don't have suffix
      detected_for_env = cassandra_keyspaces.detect{|k| k == keyspace} unless detected_for_env

      raise "Can't find keyspace in C* for '#{keyspace}'. Please create it first (#{keyspace} or #{keyspace}_#{Rails.env})" unless detected_for_env

      detected_for_env
    end

    #list of cassandra avaliable keyspaces
    def cassandra_keyspaces
      CassandraMigrations::SchemaDump.keyspaces - ["system", "system_traces"]
    end
  end
end
