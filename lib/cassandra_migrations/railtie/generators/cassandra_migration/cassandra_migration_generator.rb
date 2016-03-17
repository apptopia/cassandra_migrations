class CassandraMigrationGenerator < Rails::Generators::Base
  source_root File.expand_path('templates', File.dirname(__FILE__))
  
  argument :migration_name, :type => :string
  class_option :keyspace, :type => :string

  def generate_migration
    file_name = "#{Time.current.utc.strftime('%Y%m%d%H%M%S')}_#{migration_name.underscore}"
    @migration_class_name = migration_name.camelize
    if options.keyspace
      template "empty_migration.rb.erb", "db/cassandra_migrate/#{options.keyspace}/#{file_name}.rb"  
    else
      template "empty_migration.rb.erb", "db/cassandra_migrate/#{file_name}.rb"  
    end
  end
end
