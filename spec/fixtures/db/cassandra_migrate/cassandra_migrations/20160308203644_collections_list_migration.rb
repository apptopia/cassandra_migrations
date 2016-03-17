class CollectionsListMigration < CassandraMigrations::Migration
  def up
    create_table :collection_lists do |t|
      t.uuid :id, :primary_key => true
      t.list :list_1, :type => :string
    end
  end

  def down
    drop_table :collection_lists
  end
end