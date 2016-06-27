# encoding : utf-8
require 'colorize'

namespace :cassandra do

  task :start do
    CassandraMigrations::Cassandra.start!
  end

  desc 'Create main keyspace and secondary keyspaces in config/cassandra.yml for the current environment'
  task :create do

    begin
      CassandraMigrations::Cassandra.start!
      puts "Keyspace #{CassandraMigrations::Config.keyspace} already exists!"

      #create secondary keyspaces
      CassandraMigrations::Cassandra.create_secondary_keyspaces!(Rails.env)

    rescue CassandraMigrations::Errors::UnexistingKeyspaceError
      CassandraMigrations::Cassandra.create_keyspaces!(Rails.env)
    end
  end

  desc 'Drop main keyspace and secondary keyspaces in config/cassandra.yml for the current environment'
  task :drop do
    begin
      CassandraMigrations::Cassandra.drop_keyspaces!(Rails.env)
    rescue CassandraMigrations::Errors::UnexistingKeyspaceError
      puts "Keyspace #{CassandraMigrations::Config.keyspace} does not exist... cannot be dropped"
    end
  end

  desc 'Migrate the keyspace to the latest version'
  task :migrate => :start do
    keyspace = ENV['KEYSPACE']
    CassandraMigrations::Migrator.up_to_latest!(keyspace)
    Rake::Task['cassandra:clone_structure'].execute
  end

  desc 'Rolls the schema back to the previous version (specify steps w/ STEP=n and KEYSPACE=xxx)'
  task :rollback => :start do
    steps = (ENV['STEP'] ? ENV['STEP'].to_i : 1)
    keyspace = ENV['KEYSPACE']
    
    unless keyspace
      all_k = CassandraMigrations::Migrator.get_keyspaces_list
      keyspace = all_k.first if all_k.size == 1
    end

    raise "You have more than 1 keyspace. Please specify KEYSPACE=xxx" unless keyspace

    CassandraMigrations::Migrator.rollback!(steps, keyspace)
  end

  # namespace :migrate do
  #   desc 'Resets and prepares cassandra database (all data will be lost)'
  #   task :reset do
  #     Rake::Task['cassandra:drop'].execute
  #     Rake::Task['cassandra:create'].execute
  #     Rake::Task['cassandra:migrate'].execute
  #   end
  # end

  task :setup do
    puts "DEPRECATION WARNING: `cassandra:setup` rake task has been deprecated, use `cassandra:migrate:reset` instead"
    Rake::Task['cassandra:create'].execute
    Rake::Task['cassandra:migrate'].execute
  end

  namespace :test do
    desc 'Load the development schema in to the test schema via cql structure'
    task :prepare do
      Rails.env = 'test'
      CassandraMigrations::SchemaDump.restore_keyspaces
    end
  end

  desc 'clones structure into cql file for each keyspace'
  task :clone_structure do
    CassandraMigrations::SchemaDump.dump_for_all_keyspaces
  end


  desc 'Retrieves the current schema version number'
  task :version => :start do
    CassandraMigrations::Migrator.get_keyspaces_list.each do |keyspace|
      puts "Current version for #{keyspace}: #{CassandraMigrations::Migrator.read_current_version_v2(keyspace)}"
    end
  end

  desc 'Show missing migrations'
  task :missing => :start do
    CassandraMigrations::Migrator.get_keyspaces_list.each do |keyspace|
      p "Missing for #{keyspace}: #{CassandraMigrations::Migrator.get_missing_migrations(keyspace)}"
    end
  end

  desc 'Migrate to new schema of migrations'
  task :migrate_to_v2 do
    #Rake::Task['cassandra:create'].execute
    Rake::Task['cassandra:start'].execute
    CassandraMigrations::Migrator.migrate_to_v2
  end

end
