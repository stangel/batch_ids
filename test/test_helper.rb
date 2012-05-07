require 'rubygems'
require 'test/unit'
require 'shoulda'
require 'mocha'
require 'pp'

$LOAD_PATH.unshift File.dirname(__FILE__) + "/../lib"
require 'batch_ids'

class Test::Unit::TestCase
end

ActiveRecord::Base.establish_connection(
  :adapter  => "postgresql",
  :host     => "localhost",
  :username => "postgres",
  :password => "",
  :database => "batch_ids_test"
)
ActiveRecord::Migration.verbose = false
ActiveRecord::Base.connection.client_min_messages = 'panic'
