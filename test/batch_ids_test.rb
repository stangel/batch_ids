require File.dirname(__FILE__) + '/test_helper'

class BatchIdsTest < Test::Unit::TestCase
  class CreateTables < ActiveRecord::Migration
    def self.up
      create_table :app_models do |t|
        t.string :name
      end
    end

    def self.create_sample_models
      ['Blick', 'Flick', 'Glick', 'Plick', 'Quee', 'Snick', 'Whick'].each do |new_model_name|
        AppModel.create(:name => new_model_name)
      end
    end

    def self.down
      drop_table :app_models
    end
  end

  class AppModel < ActiveRecord::Base
  end

  context 'with a db connection' do
    setup do
      CreateTables.verbose = false
      CreateTables.up

      CreateTables.create_sample_models
      BatchIds.destroy_tmp_table(:model => AppModel)
    end

    teardown do
      BatchIds.destroy_tmp_table(:model => AppModel)
      CreateTables.down
    end

    should 'yield all ids' do
      yielded_ids = []
      yield_count = 0

      batch = AppModel.each_batch(:batch_size => 4) do |id_set, bb|
        yield_count += 1
        yielded_ids += id_set
        bb.mark_completed( id_set.first )
      end

      assert_equal 2, yield_count
      assert_equal AppModel.all.collect {|mod| mod.id.to_s }.sort, yielded_ids
      assert_equal BatchIds, batch.class

      assert_not_nil AppModel.connection.select_value("SELECT end_time FROM #{batch.tmp_table_name} WHERE id = #{AppModel.first.id}")
    end

    should 'allow no args' do
      yielded_ids = []

      batch = AppModel.each_batch do |id_set, bb|
        yielded_ids += id_set
      end

      assert_equal AppModel.all.collect {|mod| mod.id.to_s }.sort, yielded_ids
    end

    should 'allow counting with optional conditions' do
      batch = BatchIds.new(:model => AppModel, :batch_size => 4)

      assert_equal AppModel.all.size, batch.count

      yielded_ids = []
      batch.each_batch do |id_set, bb|
        yielded_ids += id_set
        assert_equal yielded_ids.size, batch.count('start_time IS NOT NULL')
      end
    end

    context 'resume' do
      should 'continue previous batch' do
        yielded_ids = []

        batch = AppModel.each_batch do |id_set, bb|
          yielded_ids += id_set
          break
        end

        batch = AppModel.each_batch(:resume => true) do |id_set, bb|
          yielded_ids += id_set
          break
        end

        assert_equal AppModel.all.collect {|mod| mod.id.to_s }.sort, yielded_ids
      end
    end
  end
end
