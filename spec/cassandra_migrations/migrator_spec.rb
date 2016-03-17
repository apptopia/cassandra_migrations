# encoding : utf-8
require 'spec_helper'

describe CassandraMigrations::Migrator do
  let(:keyspace) { 'development' }
  let(:dump_file_fixture) { "spec/fixtures/dumps/dump1.cql" }
  let(:dump_all_file_fixture) { "spec/fixtures/dumps/dump_all.cql" }

  let(:restored_keyspace) { 'my_keyspace_test' }
  let(:dev_keyspaces) { ["cassandra_migrations_development", "secondary_first_development", "secondary_second_development"] }

  describe ".get_keyspaces_list" do
    it "should return list of keyspaces based on dirs" do
      allow(Rails).to receive(:root).and_return Pathname.new("spec/fixtures")
      allow(Rails).to receive(:env).and_return ActiveSupport::StringInquirer.new("development")

      expect(subject.get_keyspaces_list).to eq(["cassandra_migrations", "secondary_first"])
    end
  end

  context "with real keyspaces" do

    let(:rails_root) { Pathname.new(File.expand_path('../../fixtures/', __FILE__)) }
    before do
      allow(Rails).to receive(:root).and_return rails_root
      allow(Rails).to receive(:env).and_return ActiveSupport::StringInquirer.new("development")
      allow(CassandraMigrations::Migrator).to receive(:dump_keyspace).and_return nil

      CassandraMigrations::Cassandra.create_keyspaces!("development")
      #CassandraMigrations::Cassandra.create_keyspaces!("test")

      CassandraMigrations::Migrator.up_to_latest!("cassandra_migrations")
    end

    after do
      CassandraMigrations::Cassandra.client = nil
      CassandraMigrations::Cassandra.drop_keyspaces!("development")
      #CassandraMigrations::Cassandra.drop_keyspaces!("test")

      CassandraMigrations::Config.configurations = nil
    end

    describe ".get_missing_migrations" do
      it "should return missing migration for keyspace" do
        expect(subject.get_missing_migrations("secondary_first")).to eq(['20160310203644'])        
      end
    end

    describe ".get_migrated_migrations" do
      it "should return migrated list" do
        expect(subject.get_migrated_migrations("cassandra_migrations")).to eq(["20160307203644", "20160308203644"])
      end
    end

    describe ".rollback!" do
      it "should rollback exactly x migrations" do
        expect(subject.get_missing_migrations("cassandra_migrations")).to eq([])
        CassandraMigrations::Migrator.rollback!(2, "cassandra_migrations")
        expect(subject.get_missing_migrations("cassandra_migrations")).to eq(["20160307203644", "20160308203644"])
      end
    end

    describe ".up_to_latest" do 
      it "should migrate to latest for all keyspaces" do
        CassandraMigrations::Migrator.rollback!(1, "cassandra_migrations")
        CassandraMigrations::Migrator.rollback!(1, "secondary_first")

        expect(CassandraMigrations::Migrator.read_current_version_v2("cassandra_migrations")).to eq("20160307203644")
        expect(CassandraMigrations::Migrator.read_current_version_v2("secondary_first")).to eq(nil)

        CassandraMigrations::Migrator.up_to_latest!
        expect(CassandraMigrations::Migrator.read_current_version_v2("cassandra_migrations")).to eq("20160308203644")
        expect(CassandraMigrations::Migrator.read_current_version_v2("secondary_first")).to eq("20160310203644")
      end

      it "should migrate one keyspace" do
        CassandraMigrations::Migrator.rollback!(1, "secondary_first")
        CassandraMigrations::Migrator.rollback!(1, "cassandra_migrations")

        expect(CassandraMigrations::Migrator.read_current_version_v2("cassandra_migrations")).to eq("20160307203644")
        expect(CassandraMigrations::Migrator.read_current_version_v2("secondary_first")).to eq(nil)

        CassandraMigrations::Migrator.up_to_latest!("cassandra_migrations")
        expect(CassandraMigrations::Migrator.read_current_version_v2("cassandra_migrations")).to eq("20160308203644")
        expect(CassandraMigrations::Migrator.read_current_version_v2("secondary_first")).to eq(nil)
      end
    end
  end

  describe ".migrate_to_v2" do

    let(:rails_root) { Pathname.new(File.expand_path('../../fixtures/', __FILE__)) }
    let(:v1_metadata_tale) { CassandraMigrations::Migrator::METADATA_TABLE }

    before do
      allow(Rails).to receive(:root).and_return rails_root
      allow(Rails).to receive(:env).and_return ActiveSupport::StringInquirer.new("development")
      allow(CassandraMigrations::Migrator).to receive(:dump_keyspace).and_return nil

      CassandraMigrations::Cassandra.create_keyspaces!("development")
    end

    after do
      CassandraMigrations::Cassandra.client = nil
      CassandraMigrations::Cassandra.drop_keyspaces!("development")

      CassandraMigrations::Config.configurations = nil
    end

    it "should migrate to newest version of storing migrations" do

      CassandraMigrations::Cassandra.execute("CREATE TABLE cassandra_migrations_development.#{v1_metadata_tale} (data_name varchar PRIMARY KEY, data_value varchar)")
      CassandraMigrations::Cassandra.execute("CREATE TABLE secondary_first_development.#{v1_metadata_tale} (data_name varchar PRIMARY KEY, data_value varchar)")

      CassandraMigrations::Cassandra.write!("cassandra_migrations_development.#{v1_metadata_tale}", {:data_name => 'version', :data_value => '20160307203644'})
      CassandraMigrations::Cassandra.write!("secondary_first_development.#{v1_metadata_tale}", {:data_name => 'version', :data_value => '20160310203644'})

      subject.migrate_to_v2

      expect(subject.read_current_version_v2("cassandra_migrations_development")).to eq("20160307203644")
      expect(subject.read_current_version_v2("secondary_first_development")).to eq("20160310203644")
      
      #this is skipped as is not present in folders
      expect(subject.read_current_version_v2("secondary_second_development")).to be_nil

      expect(subject.get_missing_migrations("cassandra_migrations")).to eq(["20160308203644"])
      expect(subject.get_missing_migrations("secondary_first_development")).to eq([])

    end
  end

end