class CreateKitchenSink < CassandraMigrations::Migration
  def up
    create_table :kitchen_sink do |t|
      t.uuid :id, :primary_key => true
      t.string :a_string
      t.timestamp :a_timestamp
      t.float :a_float
      t.list :a_list_of_strings, :type => :string
    end
  end

  def down
    drop_table :kitchen_sink
  end
end