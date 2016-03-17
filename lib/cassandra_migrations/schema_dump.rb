module CassandraMigrations
  class SchemaDump
    attr_accessor :donor_keyspace, :dest_keyspace
    
    # i.e. keyspace = 'egg' will donor_keyspace => 'egg_development'
    # and dest_keyspace = 'egg_test'
    # set donor_keyspace, dest_keyspace if you need other logic

    def initialize(keyspace, destination = nil)
      @force_one_destination = !!destination
      @destination = destination || Rails.root.join("db", "cassandra_schema_#{keyspace}.cql")
      @keyspace = keyspace
      @donor_keyspace = "#{keyspace}_development"
      @dest_keyspace = "#{keyspace}_test"
    end

    #if destination is nil just return text
    def dump
      donor = ::Cassandra.cluster.keyspaces.detect{|k| k.name == @donor_keyspace}
      raise "Can't find #{@donor_keyspace}" unless donor

      create_keyspace_cql = donor.to_cql.gsub(@donor_keyspace, @dest_keyspace)
      drop_keyspace_cql = "DROP KEYSPACE IF EXISTS #{@dest_keyspace};"
      create_table_cqls = donor.tables.map(&:to_cql).map{|r| r.gsub(@donor_keyspace, @dest_keyspace)}

      body = [drop_keyspace_cql, create_keyspace_cql, create_table_cqls.join("")].join("")

      mode = (@force_one_destination ? 'a' : 'w')
      File.open(@destination, mode){|f| f.write(body)}

      body
    end

    def self.restore_file(file)
      f = File.read(file)
      f.split(";").each{|l| CassandraMigrations::Cassandra.execute("#{l};")}
    end

    def self.restore_keyspace(keyspace)
      file = Rails.root.join("db", "cassandra_schema_#{keyspace}.cql")
      restore_file(file)
    end

    def self.keyspaces
      CassandraMigrations::Cassandra.execute("select keyspace_name from system.schema_keyspaces").to_a.map{|t| t["keyspace_name"]}
    end


    def self.dump_for_all_keyspaces(force_one_destination = nil) 
      File.open(force_one_destination, 'w') {|file| file.truncate(0)} if force_one_destination
      keyspaces = CassandraMigrations::Migrator.get_keyspaces_list
      keyspaces.each do |keyspace|
        self.new(keyspace, force_one_destination).dump
      end
    end

    def self.restore_keyspaces
      keyspaces = CassandraMigrations::Migrator.get_keyspaces_list
      keyspaces.each do |keyspace|
        restore_keyspace(keyspace)
      end
    end
  end
end