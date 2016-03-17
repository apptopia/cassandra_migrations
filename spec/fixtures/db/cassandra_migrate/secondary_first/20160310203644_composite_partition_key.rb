class CompositePartitionKey < CassandraMigrations::Migration
  def up
    create_table :composite_partition_key, :partition_keys => [:id, :a_string], :primary_keys => [:id, :a_string, :a_timestamp] do |t|
      t.uuid :id
      t.string :a_string
      t.timestamp :a_timestamp
    end
  end

  
  def down
    drop_table :composite_partition_key
  end
end
