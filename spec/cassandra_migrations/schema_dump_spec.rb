# encoding : utf-8
require 'spec_helper'

describe CassandraMigrations::SchemaDump do
  let(:keyspace) { 'development' }
  let(:dump_file_fixture) { "spec/fixtures/dumps/dump1.cql" }
  let(:dump_all_file_fixture) { "spec/fixtures/dumps/dump_all.cql" }

  let(:restored_keyspace) { 'my_keyspace_test' }
  let(:dev_keyspaces) { ["cassandra_migrations_development", "secondary_first_development", "secondary_second_development"] }

  before do
    require_relative '../fixtures/migrations/migrations'

    allow(Rails).to receive(:root).and_return Pathname.new("spec/fixtures")
    allow(Rails).to receive(:env).and_return ActiveSupport::StringInquirer.new("development")

    CassandraMigrations::Cassandra.create_keyspaces!("development")
    CassandraMigrations::Cassandra.create_keyspaces!("test")

    dev_keyspaces.each do |ks|
      CassandraMigrations::Cassandra.use(ks)

      CreateKitchenSink.new.up
      CollectionsMapMigration.new.up
    end
  end

  after do
    CassandraMigrations::Cassandra.client = nil
    CassandraMigrations::Cassandra.drop_keyspaces!("development")
    CassandraMigrations::Cassandra.drop_keyspaces!("test")

    CassandraMigrations::Config.configurations = nil
  end

  context "all_keyspaces" do
    describe "#dump_for_all_keyspaces" do
      let(:dump_file_destination) { "/tmp/dump.cql" }

      it "should create dumps for all keyspaces" do
        allow(CassandraMigrations::Migrator).to receive(:get_keyspaces_list).and_return(dev_keyspaces.map{|s| s.chomp("_development")})

        generated_dump = nil 
        MemFs.activate do
          s = CassandraMigrations::SchemaDump.dump_for_all_keyspaces(dump_file_destination)
          generated_dump = File.read(dump_file_destination)
        end

        File.open(dump_all_file_fixture, 'w'){|f| f.write(generated_dump)}
        expect(generated_dump).to eq(File.read(dump_all_file_fixture))
      end
    end
  end

  describe ".keyspaces" do
    it "should return keyspaces" do
      expect(CassandraMigrations::SchemaDump.keyspaces).to include("cassandra_migrations_development")
    end
  end

  describe "#dump" do
    let(:dump_file_destination) { "/tmp/dump.cql" }
    it "should dump to file" do

      generated_dump = nil 
      MemFs.activate do
        s = CassandraMigrations::SchemaDump.new('cassandra_migrations', dump_file_destination)
        s.dest_keyspace = restored_keyspace
        s.dump
        generated_dump = File.read(dump_file_destination)
      end

      expect(generated_dump).to eq(File.read(dump_file_fixture))
    end
  end

  describe "#restore_file" do
    it "should resotre from cql" do
      CassandraMigrations::SchemaDump.restore_file(dump_file_fixture)

      keyspace = ::Cassandra.cluster.keyspaces.detect{|k| k.name == restored_keyspace}
      expect(keyspace.tables.map(&:name).sort).to eq(['collection_lists', 'kitchen_sink'])
    end
  end

end