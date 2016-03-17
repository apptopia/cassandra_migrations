# encoding: utf-8
require 'cassandra'

module CassandraMigrations
  module Cassandra
    module KeyspaceOperations

      def create_keyspaces!(env)
        config = Config.configurations[env]
        create_keyspace_from_hash(config)
        create_secondary_keyspaces!(env)
      end
 
      def create_secondary_keyspaces!(env)
        config = Config.configurations[env]
        if config.secondary_keyspaces
          config.secondary_keyspaces.each do |config|
            create_keyspace_from_hash(config)
          end 
        end
      end

      def drop_keyspaces!(env)
        config = Config.configurations[env]
        drop_keyspace_from_hash(config)

        if config.secondary_keyspaces
          config.secondary_keyspaces.each{|s| drop_keyspace_from_hash(s)}
        end
      end

      private

      def drop_keyspace_from_hash(config)
        begin
          execute("DROP KEYSPACE #{config['keyspace']}")
          puts "Dropped keyspace #{config['keyspace']}"
        rescue ::Cassandra::Errors::ConfigurationError
          raise Errors::UnexistingKeyspaceError, config["keyspace"]
        end
      end

      def create_keyspace_from_hash(config)
        validate_config(config)

        begin
          execute(
            "CREATE KEYSPACE #{config['keyspace']} \
             WITH replication = { \
               'class':'#{config['replication']['class']}', \
               'replication_factor': #{config['replication']['replication_factor']} \
             }"
          )

          puts "Created keyspace #{config['keyspace']}"
        rescue ::Cassandra::Errors::AlreadyExistsError
          puts "Keyspace #{config['keyspace']} already exists!"
        end

        begin
          use(config['keyspace'])
        rescue StandardErorr => exception
          drop_keyspace_from_hash!(config)
          raise exception
        end
      end

      def validate_config(config)
        if config['keyspace'].nil?
          raise Errors::MissingConfigurationError.new("Configuration of 'keyspace' is required in config.yml, but none is defined.")
        end
        unless config_includes_replication?(config)
          raise Errors::MissingConfigurationError.new("Configuration for 'replication' is required in config.yml, but none is defined.")
        end
        true
      end

      def config_includes_replication?(config)
        config['replication'] &&
        config['replication']['class'] &&
        config['replication']['replication_factor']
      end
    end
  end
end
