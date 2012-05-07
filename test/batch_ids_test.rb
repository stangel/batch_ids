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
    end

    teardown do
      CreateTables.down
    end

    should "yield all ids" do
      yielded_ids = []
      yield_count = 0

      AppModel.each_batch(:batch_size => 4, :reuse_tmp_table => true) do |id_set|
        yield_count += 1
        yielded_ids += id_set
      end

      assert_equal 2, yield_count
      assert_equal AppModel.all.collect {|mod| mod.id.to_s }.sort, yielded_ids
    end

  end
end
